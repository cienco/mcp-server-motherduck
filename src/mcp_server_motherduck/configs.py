import os

# === MotherDuck / DB ===
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
# Il tuo DB su MotherDuck
XEEL_DB = os.getenv("XEEL_DB", "my_db")
# Percorso connessione per DuckDB/MotherDuck
DB_PATH = os.getenv("DB_PATH", f"md:{XEEL_DB}")

# === Sicurezza / limiti ===
# In demo puoi permettere INSERT/UPDATE/DELETE su tabelle in whitelist
DEMO_RW = os.getenv("DEMO_RW", "false").lower() == "true"
RW_TABLE_WHITELIST = [
    t.strip() for t in os.getenv("RW_TABLE_WHITELIST", "jobs").split(",") if t.strip()
]

# Timeout e limiti query
QUERY_TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "8000"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "500"))
DEFAULT_OFFSET = int(os.getenv("DEFAULT_OFFSET", "0"))
