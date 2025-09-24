import json
import os
import re
import time
from typing import Any, List, Tuple

from .database import connect
from .configs import (
    DEMO_RW,
    RW_TABLE_WHITELIST,
    DEFAULT_LIMIT,
    MAX_LIMIT,
    DEFAULT_OFFSET,
    QUERY_TIMEOUT_MS,
)

DEBUG_MODE = os.getenv("DEBUG_MCP", "false").lower() == "true"

# --- Policy comandi ammessi ---------------------------------------------------
READONLY_ALLOWED_PREFIX = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
BANNED_ALWAYS = (
    "CREATE", "DROP", "ALTER", "TRUNCATE", "ATTACH", "DETACH",
    "COPY", "PRAGMA", "EXPORT", "IMPORT", "CALL", "SET "
)

def _is_allowed(sql: str) -> bool:
    s = sql.strip().upper()
    if any(b in s for b in BANNED_ALWAYS):
        return False
    if s.startswith(("INSERT", "UPDATE", "DELETE")):
        if not DEMO_RW:
            return False
        sql_l = f" {sql.lower()} "
        return any(f" {t.lower()} " in sql_l for t in RW_TABLE_WHITELIST)
    return s.startswith(READONLY_ALLOWED_PREFIX)

# --- Conta '?' ignorando quelli dentro apici ---------------------------------
def _placeholder_count(sql: str) -> int:
    n = 0
    in_s = False
    esc = False
    for ch in sql:
        if in_s:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "'":
                in_s = False
        else:
            if ch == "'":
                in_s = True
            elif ch == "?":
                n += 1
    return n

# --- Normalizzazione 'params' (accetta array, dict, stringa JSON, scalari) ---
def _extract_scalar(v: Any) -> Any:
    if isinstance(v, dict):
        for key in ("value", "data", "text"):
            if key in v:
                return v[key]
        if len(v) == 1:
            return next(iter(v.values()))
        return str(v)
    return v

def _normalize_params(params: Any) -> List[Any]:
    if params is None:
        return []
    if isinstance(params, str):
        s = params.strip()
        if s in ("", "null", "none", "undefined"):
            return []
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                decoded = json.loads(s)
                return _normalize_params(decoded)
            except Exception:
                return [s]
        return [s]
    if isinstance(params, (int, float, bool)):
        return [params]
    if isinstance(params, list):
        return [_extract_scalar(x) for x in params]
    if isinstance(params, dict):
        if "items" in params and isinstance(params["items"], list):
            return [_extract_scalar(x) for x in params["items"]]
        return [_extract_scalar(v) for v in params.values()]
    return [params]

# --- Paginazione: solo letterali, MAI placeholder aggiunti dal server --------
_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+", re.I)
_OFFSET_RE = re.compile(r"\bOFFSET\s+\d+", re.I)

def _ensure_limit_offset_literals(sql: str) -> str:
    """
    Aggiunge LIMIT/OFFSET **come letterali** se mancano e se ha senso farlo.
    - SHOW/DESCRIBE: non toccare.
    - SELECT/WITH/EXPLAIN: se mancano, aggiungi " LIMIT <int> OFFSET <int>" letterali.
    - Mai aggiungere placeholder "?" qui.
    """
    s_up = sql.strip().upper()
    if s_up.startswith(("SHOW", "DESCRIBE")):
        return sql  # non toccare

    has_limit = bool(_LIMIT_RE.search(sql))
    has_offset = bool(_OFFSET_RE.search(sql))
    out = sql.rstrip().rstrip(";")

    # Aggiungi letterali solo se mancano
    if not has_limit:
        out += f" LIMIT {min(DEFAULT_LIMIT, MAX_LIMIT)}"
    if not has_offset:
        out += f" OFFSET {DEFAULT_OFFSET}"
    return out

# --- Entry point --------------------------------------------------------------
def run_query(sql: str, params: Any = None, timeout_ms: int = QUERY_TIMEOUT_MS) -> dict:
    """
    Esegue su MotherDuck con guard-rail:
      - whitelist comandi
      - paginazione SOLO letterale (mai parametri aggiunti dal server)
      - normalizzazione parametri lato server
      - ⚠️ se la SQL FINALE non ha placeholder, esegui SENZA 'params' (nessun secondo argomento).
    """
    if not _is_allowed(sql):
        return {"error": "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. Mutazioni solo in DEMO su tabelle whitelisted."}

    # Normalizza i param ricevuti (ma NON aggiungeremo parametri noi)
    params_norm = _normalize_params(params)

    # Aggiungi LIMIT/OFFSET come letterali (mai '?')
    sql_final = _ensure_limit_offset_literals(sql)

    # Conta i placeholder della SQL FINALE
    ph_final = _placeholder_count(sql_final)

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()

        if ph_final == 0:
            # 🔒 Guardia assoluta: niente parametri a DuckDB
            cur.execute(sql_final)
            passed_params = []
        else:
            # Se ci sono placeholder, usiamo SOLO i param arrivati dal client (normalizzati)
            # e li tagliamo a ph_final per evitare mismatch "got N"
            params_to_pass = (params_norm or [])[:ph_final]
            cur.execute(sql_final, params_to_pass)
            passed_params = params_to_pass

        if not cur.description:
            result = {"ok": True, "query_info": {"elapsed_ms": int((time.time() - start) * 1000)}}
            if DEBUG_MODE:
                result["debug"] = {
                    "sql_original": sql,
                    "sql_final": sql_final,
                    "placeholders_final": ph_final,
                    "params_received": params,
                    "params_passed": passed_params,
                }
            return result

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]

        # Paginazione riportata (letterali = default)
        result = {
            "columns": cols,
            "rows": out_rows,
            "pagination": {
                "limit": DEFAULT_LIMIT,
                "offset": DEFAULT_OFFSET,
                "next_offset": DEFAULT_OFFSET + len(out_rows),
            },
            "query_info": {"elapsed_ms": int((time.time() - start) * 1000)},
        }
        if DEBUG_MODE:
            result["debug"] = {
                "sql_original": sql,
                "sql_final": sql_final,
                "placeholders_final": ph_final,
                "params_received": params,
                "params_passed": passed_params,
            }
        return result

    except Exception as e:
        err = {"error": str(e)}
        if DEBUG_MODE:
            err["debug"] = {
                "sql_original": sql,
                "sql_final": sql_final,
                "placeholders_final": ph_final,
                "params_received": params,
                "params_passed": [] if ph_final == 0 else (params_norm or [])[:ph_final],
            }
        return err
    finally:
        con.close()

# --- Shim compat --------------------------------------------------------------
def build_application():
    return None
