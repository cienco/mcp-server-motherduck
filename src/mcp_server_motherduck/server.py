import json
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

# --- Placeholder utils (ignora '?' dentro apici) ------------------------------
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
        # stringa semplice: decidi in base ai placeholder
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
    - SHOW/DESCRIBE: non toccare (niente LIMIT/OFFSET aggiunti).
    - Se NON ci sono placeholder nella SQL originale:
        -> aggiungi LIMIT/OFFSET **letterali** (no parametri).
    - Se ci sono già placeholder:
        -> aggiungi LIMIT ? OFFSET ? e appendi i valori ai params.
    In ogni caso, non duplicare LIMIT/OFFSET se già presenti.
    """
    s_up = sql.strip().upper()
    if s_up.startswith("SHOW") or s_up.startswith("DESCRIBE"):
        return sql, params  # non aggiungere paginazione a SHOW/DESCRIBE

    has_limit = bool(_LIMIT_RE.search(sql))
    has_offset = bool(_OFFSET_RE.search(sql))
    new_sql = sql.rstrip().rstrip(";")
    new_params = list(params)

    # niente da fare se già presenti entrambi
    if has_limit and has_offset:
        return new_sql, new_params

    nph_before = _placeholder_count(new_sql)

    # Caso A: nessun placeholder nella SQL -> aggiungi letterali
    if nph_before == 0:
        if not has_limit:
            new_sql += f" LIMIT {min(DEFAULT_LIMIT, MAX_LIMIT)}"
        if not has_offset:
            new_sql += f" OFFSET {DEFAULT_OFFSET}"
        return new_sql, new_params  # NON aggiungere parametri

    # Caso B: ci sono placeholder -> usa parametri per lim/offset mancanti
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
    Esegue query su MotherDuck con guard-rail:
      - whitelist comandi
      - paginazione sicura (vedi _ensure_limit_offset)
      - esecuzione parametrica
      - normalizzazione 'params' (ed evita mismatch 0 vs 1)
    """
    if not _is_allowed(sql):
        return {"error": "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. Mutazioni solo in DEMO su tabelle whitelisted."}

    # Normalizza in base alla SQL originale (serve per gestire il caso 0 placeholder)
    params_norm = _normalize_params(params, sql)

    # Aggiungi paginazione secondo le regole
    sql_final, params_final = _ensure_limit_offset(sql, params_norm)

    # Allineamento finale: se #params > #placeholder, tronca gli extra
    ph_after = _placeholder_count(sql_final)
    if len(params_final) > ph_after:
        params_final = params_final[:ph_after]
    # Se #params < #placeholder (dovrebbe accadere solo per i due extra che aggiungiamo noi),
    # non forziamo a riempire: _ensure_limit_offset si occupa di appendere i suoi se servono.

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql_final, params_final)

        if not cur.description:
            return {"ok": True, "query_info": {"elapsed_ms": int((time.time() - start) * 1000)}}

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]

        # deduci limit/offset usati (se letterali, usa default)
        def _to_int(x, default):
            try:
                return int(x)
            except Exception:
                return default
        # Proviamo a leggere dagli ultimi due parametri solo se corrispondono a placeholder aggiunti
        limit = DEFAULT_LIMIT
        offset = DEFAULT_OFFSET
        if ph_after >= 2 and len(params_final) >= 2:
            limit = _to_int(params_final[-2], DEFAULT_LIMIT)
            offset = _to_int(params_final[-1], DEFAULT_OFFSET)

        return {
            "columns": cols,
            "rows": out_rows,
            "pagination": {"limit": limit, "offset": offset, "next_offset": offset + len(out_rows)},
            "query_info": {"elapsed_ms": int((time.time() - start) * 1000)},
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        con.close()

# --- Shim di compatibilità ----------------------------------------------------
def build_application():
    return None
