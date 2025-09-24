import re, time
from typing import Any, List, Tuple
from .database import connect
from .configs import DEMO_RW, RW_TABLE_WHITELIST, DEFAULT_LIMIT, MAX_LIMIT, DEFAULT_OFFSET, QUERY_TIMEOUT_MS

READONLY_ALLOWED_PREFIX = ("SELECT","WITH","SHOW","DESCRIBE","EXPLAIN")
BANNED_ALWAYS = ("CREATE","DROP","ALTER","TRUNCATE","ATTACH","DETACH","COPY","PRAGMA","EXPORT","IMPORT","CALL","SET ")

def _is_allowed(sql: str) -> bool:
    s = sql.strip().upper()
    if any(b in s for b in BANNED_ALWAYS): return False
    if s.startswith(("INSERT","UPDATE","DELETE")):
        if not DEMO_RW: return False
        sql_l = f" {sql.lower()} "
        return any(f" {t.lower()} " in sql_l for t in RW_TABLE_WHITELIST)
    return s.startswith(READONLY_ALLOWED_PREFIX)

def _ensure_limit_offset(sql: str, params: List[Any]) -> Tuple[str, List[Any]]:
    has_limit = bool(re.search(r"\bLIMIT\s+\d+", sql, flags=re.I))
    has_offset = bool(re.search(r"\bOFFSET\s+\d+", sql, flags=re.I))
    new_sql = sql.rstrip().rstrip(";")
    new_params = list(params)
    if not has_limit:
        new_sql += " LIMIT ?"; new_params.append(min(DEFAULT_LIMIT, MAX_LIMIT))
    if not has_offset:
        new_sql += " OFFSET ?"; new_params.append(DEFAULT_OFFSET)
    return new_sql, new_params

def run_query(sql: str, params: List[Any] | None = None, timeout_ms: int = QUERY_TIMEOUT_MS):
    if not _is_allowed(sql):
        return {"error":"Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. Mutazioni solo in DEMO su tabelle whitelisted."}
    params = params or []
    if sql.strip().upper().startswith(READONLY_ALLOWED_PREFIX):
        sql, params = _ensure_limit_offset(sql, params)

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        if not cur.description:
            return {"ok": True, "query_info": {"elapsed_ms": int((time.time()-start)*1000)}}
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]

        def _to_int(x, d):
            try: return int(x)
            except Exception: return d
        limit  = _to_int(params[-2], DEFAULT_LIMIT) if len(params) >= 2 else DEFAULT_LIMIT
        offset = _to_int(params[-1], DEFAULT_OFFSET) if len(params) >= 1 else DEFAULT_OFFSET

        return {
            "columns": cols,
            "rows": out_rows,
            "pagination": {"limit": limit, "offset": offset, "next_offset": offset + len(out_rows)},
            "query_info": {"elapsed_ms": int((time.time()-start)*1000)},
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        con.close()

# Compat per template che importava build_application
def build_application(): return None
