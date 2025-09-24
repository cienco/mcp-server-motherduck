import json
import re
import time
from typing import Any, List, Tuple

from .database import connect
from .configs import (
    DEMO_RW,
    RW_TABLE_WHITELIST,
    DEFAULT_LIMIT,
    MAX_LIMIT,
    DEFAULT_OFFSET,
    QUERY_TIMEOUT_MS,
)

# --- Policy comandi -----------------------------------------------------------
READONLY_ALLOWED_PREFIX = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
BANNED_ALWAYS = (
    "CREATE", "DROP", "ALTER", "TRUNCATE", "ATTACH", "DETACH",
    "COPY", "PRAGMA", "EXPORT", "IMPORT", "CALL", "SET "
)


def _is_allowed(sql: str) -> bool:
    s = sql.strip().upper()
    if any(b in s for b in BANNED_ALWAYS):
        return False
    if s.startswith(("INSERT", "UPDATE", "DELETE")):
        if not DEMO_RW:
            return False
        sql_l = f" {sql.lower()} "
        return any(f" {t.lower()} " in sql_l for t in RW_TABLE_WHITELIST)
    return s.startswith(READONLY_ALLOWED_PREFIX)


# --- Normalizzazione params (compat con vari client MCP) ----------------------
def _extract_scalar(v: Any) -> Any:
    """
    Estrae un valore scalare da formati comuni:
    - {"type":"text","text":"M1"}  -> "M1"
    - {"value": 100}               -> 100
    - {"data": "..."} / {"text": "..."} -> ...
    - dict con un solo campo       -> primo valore
    - altrimenti stringify come fallback
    """
    if isinstance(v, dict):
        for key in ("value", "data", "text"):
            if key in v:
                return v[key]
        if len(v) == 1:
            return next(iter(v.values()))
        return str(v)
    return v


def _normalize_params(params: Any) -> List[Any]:
    """
    Accetta:
      - None
      - scalare (str/int/float/bool)
      - lista mista (scalari/dict)
      - dict (anche {"items":[...]})
      - stringa JSON ("[...]" o "{...}")
    Ritorna sempre una LISTA di scalari.
    """
    if params is None:
        return []

    # 1) Se è stringa che sembra JSON, prova a decodificare
    if isinstance(params, str):
        s = params.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                decoded = json.loads(s)
                return _normalize_params(decoded)
            except Exception:
                # non è JSON valido ⇒ trattalo come scalare singolo
                return [params]
        # stringa "semplice" ⇒ scalare
        return [params]

    # 2) Scalare nativo
    if isinstance(params, (int, float, bool)):
        return [params]

    # 3) Lista
    if isinstance(params, list):
        return [_extract_scalar(x) for x in params]

    # 4) Dict
    if isinstance(params, dict):
        if "items" in params and isinstance(params["items"], list):
            return [_extract_scalar(x) for x in params["items"]]
        return [_extract_scalar(v) for v in params.values()]

    # 5) Fallback
    return [params]


# --- Helpers SQL --------------------------------------------------------------
def _ensure_limit_offset(sql: str, params: List[Any]) -> Tuple[str, List[Any]]:
    has_limit = bool(re.search(r"\bLIMIT\s+\d+", sql, flags=re.I))
    has_offset = bool(re.search(r"\bOFFSET\s+\d+", sql, flags=re.I))
    new_sql = sql.rstrip().rstrip(";")
    new_params = list(params)
    if not has_limit:
        new_sql += " LIMIT ?"
        new_params.append(min(DEFAULT_LIMIT, MAX_LIMIT))
    if not has_offset:
        new_sql += " OFFSET ?"
        new_params.append(DEFAULT_OFFSET)
    return new_sql, new_params


# --- Entry principale ---------------------------------------------------------
def run_query(sql: str, params: Any = None, timeout_ms: int = QUERY_TIMEOUT_MS) -> dict:
    """
    Esegue una query su MotherDuck con guard-rail:
      - whitelist comandi
      - LIMIT/OFFSET forzati sulle SELECT
      - esecuzione
