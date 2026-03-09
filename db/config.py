import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

DB_DIR = Path(os.getenv("DB_DIR", str(BASE_DIR / "db")))
DB_PATH = DB_DIR / "cars.db"

DB_CONFIG = {
    "path": str(DB_PATH),
    "timeout": float(os.getenv("DB_TIMEOUT", "30.0")),
    "check_same_thread": False,
}

API_CONFIG = {
    "default_limit": int(os.getenv("API_DEFAULT_LIMIT", "100")),
    "max_limit": int(os.getenv("API_MAX_LIMIT", "1000")),
}

CSV_PATH = BASE_DIR / "cars_on_sale.csv"
