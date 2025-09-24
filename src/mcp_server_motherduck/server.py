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


# --- Supporto placeholder count (ignora '?' dentro apici) ---------------------
def _placeholder_count(sql: str) -> int:
    n = 0
    in_s = False
    esc = False
    for ch in sql:
        if in_s:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "'":
                in_s = False
        else:
            if ch == "'":
                in_s = True
            elif ch == "?":
                n += 1
    return n


# --- Normalizzazione params (compat con vari client MCP) ----------------------
def _extract_scalar(v: Any) -> Any:
    if isinstance(v, dict):
        for key in ("value", "data", "text"):
            if key in v:
                return v[key]
        if len(v) == 1:
            return next(iter(v.values()))
        return str(v)
    return v


def _normalize_params(params: Any, sql: str) -> List[Any]:
    """
    Accetta:
      - None
      - scalare (str/int/float/bool)
      - lista mista (scalari/dict)
      - dict (anche {"items":[...]})
      - stringa JSON ("[...]" o "{...}")
      - stringa semplice (es. "", "null", "M1")
    Usa il conteggio dei placeholder nella SQL per evitare il bug "expected 0, got 1".
    """
    if params is None:
        return []

    # stringhe: gestisci ""/null/JSON e il caso scalare guidato dai placeholder
    if isinstance(params, str):
        s = params.strip()
        if s in ("", "null", "none", "undefined"):
            return []
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                decoded = json.loads(s)
                return _normalize_params(decoded, sql)
            except Exception:
                # se non è JSON valido: decidi in base ai placeholder
                pass
        nph = _placeholder_count(sql)
        if nph == 0:
            return []          # nessun parametro atteso
        if nph == 1:
            return [s]         # un solo parametro: tratta la stringa come scalare
        return []              # 2+ placeholder: non indovinare; aspetta array JSON

    # scalari nativi
    if isinstance(params, (int, float, bool)):
        return [params]

    # lista
    if isinstance(params, list):
        return [_extract_scalar(x) for x in params]

    # dict
    if isinstance(params, dict):
        if "items" in params and isinstance(params["items"], list):
            return [_extract_scalar(x) for x in params["items"]]
        return [_extract_scalar(v) for v in params.values()]

    # fallback
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
      - esecuzione parametrica (mai concatenare stringhe)
      - normalizzazione 'params' da formati ricchi/stringhe → lista scalari,
        evitando di passare 1 parametro quando i placeholder sono 0.
    """
    if not _is_allowed(sql):
        return {
            "error": (
                "Query non permessa. Solo SELECT/WITH/SHOW/DESCRIBE/EXPLAIN. "
                "Mutazioni solo in DEMO su tabelle whitelisted."
            )
        }

    params = _normalize_params(params, sql)

    if sql.strip().upper().startswith(READONLY_ALLOWED_PREFIX):
        sql, params = _ensure_limit_offset(sql, params)

    start = time.time()
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)

        if not cur.description:
            return {"ok": True, "query_info": {"elapsed_ms": int((time.time() - start) * 1000)}}

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        out_rows = [dict(zip(cols, r)) for r in rows]

        def _to_int(x, default):
            try:
                return int(x)
            except Exception:
                return default

        limit = _to_int(params[-2], DEFAULT_LIMIT) if len(params) >= 2 else DEFAULT_LIMIT
        offset = _to_int(params[-1], DEFAULT_OFFSET) if len(params) >= 1 else DEFAULT_OFFSET

        return {
            "columns": cols,
            "rows": out_rows,
            "pagination": {"limit": limit, "offset": offset, "next_offset": offset + len(out_rows)},
            "query_info": {"elapsed_ms": int((time.time() - start) * 1000)},
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        con.close()


# --- Shim di compatibilità (alcuni template importano build_application) ------
def build_application():
    return None
