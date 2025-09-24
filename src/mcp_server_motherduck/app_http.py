# src/mcp_server_motherduck/app_http.py
from mcp.server.fastmcp import FastMCP
from .server import run_query

# Server MCP con UN solo tool: `query`
mcp = FastMCP("mcp-server-motherduck")

@mcp.tool()
def query(sql: str, params: list | None = None, timeout_ms: int | None = None) -> dict:
    return run_query(sql, params, timeout_ms or 8000)

# ❗️Espone direttamente l'app MCP (Streamable HTTP) come ASGI.
#    Questo garantisce il corretto lifespan e inizializza il task group.
application = mcp.streamable_http_app()
# L'endpoint MCP sarà disponibile su /mcp
