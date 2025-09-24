from typing import Any, Dict
from mcp.server.fastmcp import FastMCP
from .server import run_query

mcp = FastMCP("mcp-server-motherduck")

# Definizione esplicita dello schema: 'params' accetta array | string | object | null
QUERY_INPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "sql": {"type": "string"},
        "params": {
            "oneOf": [
                {"type": "array"},
                {"type": "string"},
                {"type": "object"},
                {"type": "null"}
            ]
        },
        "timeout_ms": {
            "oneOf": [
                {"type": "integer"},
                {"type": "string"},
                {"type": "null"}
            ]
        }
    },
    "required": ["sql"]
}

@mcp.tool(name="query", description="Esegue una query parametrica su MotherDuck (my_db).", input_schema=QUERY_INPUT_SCHEMA)
def query(sql: str, params: Any = None, timeout_ms: Any = None) -> dict:
    """
    Tool MCP: accetta params come array, stringa JSON, oggetto o null.
    La normalizzazione a lista di scalari è gestita in run_query().
    """
    to_ms = 8000
    if timeout_ms is not None:
        try:
            to_ms = int(timeout_ms)
        except Exception:
            pass
    return run_query(sql, params, to_ms)

# App ASGI MCP (Streamable HTTP) esposta su /mcp
application = mcp.streamable_http_app()
