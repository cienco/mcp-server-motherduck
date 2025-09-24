from typing import Any, Dict
from mcp.server.fastmcp import FastMCP
from .server import run_query

mcp = FastMCP("mcp-server-motherduck")

@mcp.tool(name="query", description="Esegue una query parametrica su MotherDuck (DB my_db).")
def query(sql: str, params: Any = None, timeout_ms: Any = None) -> dict:
    to_ms = 8000
    if timeout_ms is not None:
        try:
            to_ms = int(timeout_ms)
        except Exception:
            pass
    return run_query(sql, params, to_ms)

# 🔎 Tool di sola diagnostica: NON esegue il DB
@mcp.tool(name="diag", description="Diagnostica parametri: conta i placeholder e normalizza params senza eseguire la query.")
def diag(sql: str, params: Any = None) -> Dict[str, Any]:
    def count_placeholders(s: str) -> int:
        n, in_s, esc = 0, False, False
        for ch in s:
            if in_s:
                if esc: esc = False
                elif ch == "\\": esc = True
                elif ch == "'": in_s = False
            else:
                if ch == "'": in_s = True
                elif ch == "?": n += 1
        return n

    # stessa normalizzazione del server, ma senza DB
    import json
    def extract_scalar(v: Any) -> Any:
        if isinstance(v, dict):
            for k in ("value","data","text"):
                if k in v: return v[k]
            if len(v) == 1: return next(iter(v.values()))
            return str(v)
        return v

    def normalize(p: Any) -> list:
        if p is None: return []
        if isinstance(p, str):
            s = p.strip()
            if s in ("", "null", "none", "undefined"): return []
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                try: return normalize(json.loads(s))
                except Exception: return [s]
            return [s]
        if isinstance(p, (int,float,bool)): return [p]
        if isinstance(p, list): return [extract_scalar(x) for x in p]
        if isinstance(p, dict):
            if "items" in p and isinstance(p["items"], list):
                return [extract_scalar(x) for x in p["items"]]
            return [extract_scalar(v) for v in p.values()]
        return [p]

    sql_final = sql.strip().rstrip(";")
    ph = count_placeholders(sql_final)
    p_norm = normalize(params)
    # se la SQL finale non ha '?', NON passeremo parametri al DB
    params_to_pass = [] if ph == 0 else p_norm[:ph]

    return {
        "sql_final": sql_final,
        "placeholders": ph,
        "params_received": params,
        "params_normalized": p_norm,
        "params_to_pass": params_to_pass
    }

# MCP via Streamable HTTP (endpoint: /mcp)
application = mcp.streamable_http_app()
