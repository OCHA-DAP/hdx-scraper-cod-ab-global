from decimal import Decimal
from logging import INFO, basicConfig
from os import environ, getenv
from pathlib import Path

from dotenv import load_dotenv


def _is_bool(string: str) -> bool:
    """Check if string is boolean-like."""
    return string.upper() in ("YES", "ON", "TRUE", "1")


load_dotenv(override=True)
basicConfig(level=INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
cwd = Path(__file__).parent

dbname = getenv("DBNAME", "app")
distance = Decimal(getenv("DISTANCE", "0.0002"))
num_threads = int(getenv("NUM_THREADS", "1"))
quiet = _is_bool(getenv("QUIET", "YES"))
