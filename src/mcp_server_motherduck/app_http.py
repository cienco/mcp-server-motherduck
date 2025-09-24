# src/mcp_server_motherduck/app_http.py
from fastapi import FastAPI
from pydantic import BaseModel
from .server import run_query

app = FastAPI()

class QueryPayload(BaseModel):
    sql: str
    params: list | None = None
    timeout_ms: int | None = None

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/query")
def query(payload: QueryPayload):
    return run_query(payload.sql, payload.params, payload.timeout_ms)
