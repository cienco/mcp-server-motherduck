from typing import Any, Dict, List
import importlib
import sys

from mcp.server.fastmcp import FastMCP
from .database import connect

# 🔁 Importa e forza il reload del server locale, così NON usa versioni installate “vecchie”
import mcp_server_motherduck.server as srv
srv = importlib.reload(srv)

# Banner per capire cosa sta girando (lo vedi nei log Render)
print(">>> LOADED mcp_server_motherduck.app_http (REMOTE MCP) <<<")
print(f">>> Using server module: {srv.__file__}")

mcp = FastMCP("mcp-server-motherduck")

# --- helpers locali (solo per fallback zero-param) ----------------------------
READONLY_ALLOWED_PREFIX = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
BANNED_ALWAYS = ("CREATE","DROP","ALTER","TRUNCATE","ATTACH","DETACH","COPY","PRAGMA","EXPORT","IMPORT","CALL","SET ")

def _is_allowed(sql: str) -> bool:
    s = sql.strip().upper()
    if any(b in s for b in BANNED_ALWAYS):
        return False
    if s.startswith(("INSERT","UPDATE","DELETE")):
        # in fallback zero-param non permettiamo mai mutazioni
        return False
    return s.startswith(READONLY_ALLOWED_PREFIX)

def _placeholder_count(sql: str) -> int:
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

def _rows_to_dicts(cur) -> Dict[str, Any]:
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []
    out = [dict(zip(cols, r)) for r in rows]
    return {"columns": cols, "rows": out}

# -----------------------------------------------------------------------------


@mcp.tool(name="query", description="Esegue una query parametrica su MotherDuck (DB my_db).")
def query(sql: str, params: Any = None, timeout_ms: Any = None) -> dict:
    """
    Fallback robusto:
      - se la SQL NON contiene '?', esegue direttamente senza parametri (evita definitivamente 'Expected 0, got 1')
      - se contiene '?', delega a srv.run_query (che gestisce normalizzazione, ecc.)
    """
    sql_final = (sql or "").strip().rstrip(";")
    ph = _placeholder_count(sql_final)

    # ✅ Caso A: zero placeholder -> eseguo DIRETTO senza params
    if ph == 0:
        if not _is_allowed(sql_final):
            return {"error": "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN."}
        con = connect()
        try:
            cur = con.cursor()
            cur.execute(sql_final)  # ⚠️ NESSUN secondo argomento
            if not cur.description:
                return {
                    "ok": True,
                    "debug": {
                        "path_server_module": srv.__file__,
                        "sql_final": sql_final,
                        "placeholders": ph,
                        "mode": "direct_no_params"
                    }
                }
            data = _rows_to_dicts(cur)
            data["debug"] = {
                "path_server_module": srv.__file__,
                "sql_final": sql_final,
                "placeholders": ph,
                "mode": "direct_no_params"
            }
            return data
        except Exception as e:
            return {
                "error": str(e),
                "debug": {
                    "path_server_module": srv.__file__,
                    "sql_final": sql_final,
                    "placeholders": ph,
                    "mode": "direct_no_params"
                }
            }
        finally:
            con.close()

    # 🔁 Caso B: con placeholder -> delego al server “vero”
    to_ms = 8000
    if timeout_ms is not None:
        try:
            to_ms = int(timeout_ms)
        except Exception:
            pass

    # Usiamo SEMPRE il run_query del modulo appena ricaricato
    return srv.run_query(sql, params, to_ms)


# MCP via Streamable HTTP (endpoint: /mcp)
application = mcp.streamable_http_app()
