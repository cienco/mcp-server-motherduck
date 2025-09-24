from typing import Any
from mcp.server.fastmcp import FastMCP
from .server import run_query

# Server MCP con UN SOLO tool: 'query'
mcp = FastMCP("mcp-server-motherduck")

@mcp.tool()
def query(sql: str, params: Any = None, timeout_ms: Any = None) -> dict:
    """
    Tool MCP: esegue query su MotherDuck.
    'params' può essere lista/scalare/dict/None; la normalizzazione avviene in run_query().
    """
    to_ms = 8000
    if timeout_ms is not None:
        try:
            to_ms = int(timeout_ms)
        except Exception:
            pass
    return run_query(sql, params, to_ms)

# Espone direttamente l'app MCP (Streamable HTTP) come ASGI
# Endpoint MCP: /mcp
application = mcp.streamable_http_app()
