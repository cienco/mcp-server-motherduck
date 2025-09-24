import json
import time
from typing import Any, List

from .database import connect
from .configs import DEMO_RW, RW_TABLE_WHITELIST, QUERY_TIMEOUT_MS

# Solo lettura consentita
READONLY_ALLOWED_PREFIX = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
BANNED_ALWAYS = ("CREATE","DROP","ALTER","TRUNCATE","ATTACH","DETACH","COPY","PRAGMA","EXPORT","IMPORT","CALL","SET ")

def _is_allowed(sql: str) -> bool:
    s = sql.strip().upper()
    if any(b in s for b in BANNED_ALWAYS):
        return False
    if s.startswith(("INSERT","UPDATE","DELETE")):
        if not DEMO_RW:
            return False
        sql_l = f" {sql.lower()} "
        return any(f" {t.lower()} " in sql_l for t in RW_TABLE_WHITELIST)
    return s.startswith(READONLY_ALLOWED_PREFIX)

def _placeholder_count(sql: str) -> int:
    # conta ? ignorando quelli tra apici singoli
    n, in_s, esc = 0, False, False
    for ch in sql:
        if in_s:
            if esc: esc = False
            elif ch == "\\": esc = True
            elif ch == "'": in_s = False
        else:
            if ch == "'": in_s = True
            elif ch == "?": n += 1
    return n

def _extract_scalar(v: Any) -> Any:
    if isinstance(v, dict):
        for k in ("value","data","text"):
            if k in v: return v[k]
        if len(v) == 1: return next(iter(v.values()))
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
                dec = json.loads(s)
                return _normalize_params(dec)
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

def run_query(sql: str, params: Any = None, timeout_ms: int = QUERY_TIMEOUT_MS) -> dict:
    """
    Esegue la SQL così com'è:
      - NESSUNA paginazione/riscrittura automatica.
      - Se la SQL finale non ha '?', NON passa alcun parametro a DuckDB (neppure []).
      - Se ha '?', passa SOLO i param inviati dal client (normalizzati) tagliati al numero di '?'.
    """
    if not _is_allowed(sql):
        return {"error": "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. Mutazioni solo in DEMO su tabelle whitelisted."}

    sql_final = sql.strip().rstrip(";")
    ph = _placeholder_count(sql_final)
    params_norm = _normalize_params(params)
    params_passed: List[Any] = []

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()
        if ph == 0:
            # ⚠️ niente secondo argomento → elimina “Expected 0, got 1”
            cur.execute(sql_final)
        else:
            params_passed = (params_norm or [])[:ph]
            cur.execute(sql_final, params_passed)

        if not cur.description:
            return {
                "ok": True,
                "query_info": {"elapsed_ms": int((time.time()-start)*1000)},
                "debug": {
                    "sql_final": sql_final,
                    "placeholders": ph,
                    "params_received": params,
                    "params_passed": params_passed
                }
            }

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]
        return {
            "columns": cols,
            "rows": out_rows,
            "query_info": {"elapsed_ms": int((time.time()-start)*1000)},
            "debug": {
                "sql_final": sql_final,
                "placeholders": ph,
                "params_received": params,
                "params_passed": params_passed
            }
        }
    except Exception as e:
        return {
            "error": str(e),
            "debug": {
                "sql_final": sql_final,
                "placeholders": ph,
                "params_received": params,
                "params_passed": params_passed
            }
        }
    finally:
        con.close()

def build_application():
    return None
