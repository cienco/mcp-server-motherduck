import os

# === MotherDuck / DB ===
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# DB di default su MotherDuck: il tuo è "my_db"
XEEL_DB = os.getenv("XEEL_DB", "my_db")
DB_PATH = os.getenv("DB_PATH", f"md:{XEEL_DB}")
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# === Sicurezza / limiti ===
DEMO_RW = os.getenv("DEMO_RW", "false").lower() == "true"  # true solo per demo con scritture
RW_TABLE_WHITELIST = [t.strip() for t in os.getenv("RW_TABLE_WHITELIST", "jobs").split(",") if t.strip()]

QUERY_TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "8000"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "500"))
DEFAULT_OFFSET = int(os.getenv("DEFAULT_OFFSET", "0"))
