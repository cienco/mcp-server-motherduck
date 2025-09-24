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

# --- Policy comandi -----------------------------------------------------------
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

# --- Normalizzazione params ---------------------------------------------------
def _extract_scalar(v: Any) -> Any:
    if isinstance(v, dict):
        for key in ("value", "data", "text"):
            if key in v:
                return v[key]
        if len(v) == 1:
            return next(iter(v.values()))
        return str(v)
    return v

def _normalize_params(params: Any, sql: str) -> List[Any]:
    """
    Accetta None/scalare/lista/dict/stringa JSON o semplice.
    Evita di passare parametri quando la SQL non ha placeholder.
    """
    if params is None:
        return []

    if isinstance(params, str):
        s = params.strip()
        if s in ("", "null", "none", "undefined"):
            return []
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                decoded = json.loads(s)
                return _normalize_params(decoded, sql)
            except Exception:
                pass
        # stringa semplice: decidi in base ai placeholder dell'ORIGINALE
        nph = _placeholder_count(sql)
        if nph == 0:
            return []
        if nph == 1:
            return [s]
        return []

    if isinstance(params, (int, float, bool)):
        return [params]

    if isinstance(params, list):
        return [_extract_scalar(x) for x in params]

    if isinstance(params, dict):
        if "items" in params and isinstance(params["items"], list):
            return [_extract_scalar(x) for x in params["items"]]
        return [_extract_scalar(v) for v in params.values()]

    return [params]

# --- Helpers SQL --------------------------------------------------------------
_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+", flags=re.I)
_OFFSET_RE = re.compile(r"\bOFFSET\s+\d+", flags=re.I)

def _ensure_limit_offset(sql: str, params: List[Any]) -> Tuple[str, List[Any]]:
    """
    Regole:
    - SHOW/DESCRIBE: non toccare (niente LIMIT/OFFSET).
    - Se l'ORIGINALE non ha placeholder:
        -> aggiungi LIMIT/OFFSET **letterali** (no parametri).
    - Se l'ORIGINALE ha placeholder:
        -> aggiungi LIMIT ? OFFSET ? e appendi i valori ai params.
    """
    s_up = sql.strip().upper()
    if s_up.startswith("SHOW") or s_up.startswith("DESCRIBE"):
        return sql, params  # nessuna modifica

    has_limit = bool(_LIMIT_RE.search(sql))
    has_offset = bool(_OFFSET_RE.search(sql))
    new_sql = sql.rstrip().rstrip(";")
    new_params = list(params)

    if has_limit and has_offset:
        return new_sql, new_params

    nph_before = _placeholder_count(new_sql)

    # Caso A: nessun placeholder ORIGINALE -> letterali
    if nph_before == 0:
        if not has_limit:
            new_sql += f" LIMIT {min(DEFAULT_LIMIT, MAX_LIMIT)}"
        if not has_offset:
            new_sql += f" OFFSET {DEFAULT_OFFSET}"
        return new_sql, new_params  # nessun parametro aggiunto

    # Caso B: ci sono placeholder nell'ORIGINALE -> parametrici
    if not has_limit:
        new_sql += " LIMIT ?"
        new_params.append(min(DEFAULT_LIMIT, MAX_LIMIT))
    if not has_offset:
        new_sql += " OFFSET ?"
        new_params.append(DEFAULT_OFFSET)

    return new_sql, new_params

# --- Entry point --------------------------------------------------------------
def run_query(sql: str, params: Any = None, timeout_ms: int = QUERY_TIMEOUT_MS) -> dict:
    """
    Esegue su MotherDuck con guard-rail:
      - whitelist
      - paginazione safe
      - normalizzazione parametri
      - ⚠️ se la SQL FINALE ha 0 placeholder, esegue SENZA params (niente seconda arg a DuckDB)
    """
    if not _is_allowed(sql):
        return {"error": "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. Mutazioni solo in DEMO su tabelle whitelisted."}

    params_norm = _normalize_params(params, sql)
    sql_final, params_final = _ensure_limit_offset(sql, params_norm)

    ph_after = _placeholder_count(sql_final)

    # Guardia hard: se 0 placeholder nella SQL FINALE, esegui senza params del tutto
    force_no_params = (ph_after == 0)

    # Se #params > #placeholder, tronca
    if not force_no_params and len(params_final) > ph_after:
        params_final = params_final[:ph_after]

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()

        if force_no_params:
            cur.execute(sql_final)  # <-- nessun secondo argomento
        else:
            cur.execute(sql_final, params_final)

        if not cur.description:
            result = {"ok": True, "query_info": {"elapsed_ms": int((time.time() - start) * 1000)}}
            if DEBUG_MODE:
                result["debug"] = {
                    "sql_original": sql,
                    "sql_final": sql_final,
                    "placeholders_final": ph_after,
                    "params_received": params,
                    "params_final": [] if force_no_params else params_final,
                }
            return result

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]

        # deduci limit/offset (se letterali, usa default)
        def _to_int(x, default):
            try:
                return int(x)
            except Exception:
                return default
        limit = DEFAULT_LIMIT
        offset = DEFAULT_OFFSET
        if not force_no_params and ph_after >= 2 and len(params_final) >= 2:
            limit = _to_int(params_final[-2], DEFAULT_LIMIT)
            offset = _to_int(params_final[-1], DEFAULT_OFFSET)

        result = {
            "columns": cols,
            "rows": out_rows,
            "pagination": {"limit": limit, "offset": offset, "next_offset": offset + len(out_rows)},
            "query_info": {"elapsed_ms": int((time.time() - start) * 1000)},
        }
        if DEBUG_MODE:
            result["debug"] = {
                "sql_original": sql,
                "sql_final": sql_final,
                "placeholders_final": ph_after,
                "params_received": params,
                "params_final": [] if force_no_params else params_final,
            }
        return result
    except Exception as e:
        err = {"error": str(e)}
        if DEBUG_MODE:
            err["debug"] = {
                "sql_original": sql,
                "sql_final": sql_final,
                "placeholders_final": ph_after,
                "params_received": params,
                "params_final": [] if force_no_params else params_final,
            }
        return err
    finally:
        con.close()

# --- Shim compat --------------------------------------------------------------
def build_application():
    return None
