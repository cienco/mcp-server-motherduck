from typing import Any
from mcp.server.fastmcp import FastMCP
from .server import run_query

mcp = FastMCP("mcp-server-motherduck")

@mcp.tool(name="query", description="Esegue una query parametrica su MotherDuck (DB my_db).")
def query(sql: str, params: Any = None, timeout_ms: Any = None) -> dict:
    """
    'params' può essere lista/scalare/dict/stringa JSON/None.
    La normalizzazione e la protezione 0-placeholder sono gestite in run_query().
    """
    to_ms = 8000
    if timeout_ms is not None:
        try:
            to_ms = int(timeout_ms)
        except Exception:
            pass
    return run_query(sql, params, to_ms)

# MCP su Streamable HTTP (endpoint: /mcp)
application = mcp.streamable_http_app()
