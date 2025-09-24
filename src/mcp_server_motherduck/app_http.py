# src/mcp_server_motherduck/app_http.py
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from mcp.server.http import create_base_app  # MCP over Streamable HTTP
from .server import run_query  # tua logica che parla con MotherDuck

# 1) Definisci il server MCP con UN SOLO tool: 'query'
mcp = FastMCP("mcp-server-motherduck")

@mcp.tool()
def query(sql: str, params: list | None = None, timeout_ms: int | None = None) -> dict:
    """Esegue query su MotherDuck (read-only; mutazioni solo in DEMO/whitelist)."""
    return run_query(sql, params, timeout_ms or 8000)

# 2) Crea l'app ASGI MCP su /mcp
mcp_asgi_app = create_base_app(server=mcp, streamable_http_path="/mcp")

# 3) App finale esposta da Uvicorn (solo MCP). NIENTE /query http.
application = FastAPI()
application.mount("/mcp", mcp_asgi_app)

# (FACOLTATIVO) Se vuoi un healthcheck:
# @application.get("/health")
# def health(): return {"ok": True}
