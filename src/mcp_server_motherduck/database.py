import duckdb
from .configs import DB_PATH, MOTHERDUCK_TOKEN, DEMO_RW

def connect():
    """
    Connessione a MotherDuck/DuckDB.
    - read_only = True (prod) / False (demo RW)
    - imposta il token MotherDuck se presente
    """
    con = duckdb.connect(database=DB_PATH, read_only=not DEMO_RW)
    if MOTHERDUCK_TOKEN:
        con.execute("SET motherduck_token = ?;", [MOTHERDUCK_TOKEN])
    try:
        con.execute("PRAGMA threads=4;")
    except Exception:
        pass
    return con
