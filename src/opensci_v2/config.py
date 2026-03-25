from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
INPUT_DIR = ROOT / "inputs"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


def ensure_data_dirs() -> None:
    for path in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, INPUT_DIR):
        path.mkdir(parents=True, exist_ok=True)
