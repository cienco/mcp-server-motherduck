# src/mcp_server_motherduck/app_http.py

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from .server import run_query  # tua logica DB

# -- MCP server con UN solo tool: `query` --
mcp = FastMCP("mcp-server-motherduck")

@mcp.tool()
def query(sql: str, params: list | None = None, timeout_ms: int | None = None) -> dict:
    """Esegue query su MotherDuck (read-only; mutazioni solo in DEMO/whitelist)."""
    return run_query(sql, params, timeout_ms or 8000)

# --- App ASGI MCP (Streamable HTTP) ---
# NOTE: l'app risultante espone MCP su /mcp per default.
mcp_asgi = mcp.streamable_http_app()

# --- App FastAPI da lanciare con Uvicorn ---
application = FastAPI()

# Montiamo l'app MCP sotto "/" (l'endpoint effettivo sarà /mcp)
application.mount("/", mcp_asgi)

# (Facoltativo) se vuoi un healthcheck semplice, decomenta:
# @application.get("/health")
# def health():
#     return {"ok": True}
