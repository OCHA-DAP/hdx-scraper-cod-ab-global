"""Global configuration and environment variable parsing for the COD-AB scraper."""

import logging
from os import environ, getenv

from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
environ["OGR_ORGANIZE_POLYGONS"] = "ONLY_CCW"
environ["PYOGRIO_USE_ARROW"] = "1"


UPDATED_BY_SCRIPT = "HDX Scraper: COD-AB Global"

iso3_include = [
    x.strip() for x in getenv("ISO3_INCLUDE", "").upper().split(",") if x.strip()
]
iso3_exclude = [
    x.strip() for x in getenv("ISO3_EXCLUDE", "").upper().split(",") if x.strip()
]

admin_level_full_overrides = {
    "BGD": 4,
    "IDN": 4,
    "PHL": 4,
}

date_valid_on_overrides = {
    "UKR": "2025-09-01",
}

where_filter = {
    "LBN": "adm1_pcode <> 'Conflict'",
    "PAK": "adm1_pcode not in ('PK1', 'PK3')",
    "SDN": "adm1_pcode <> 'SD19'",
    "SSD": "adm1_pcode <> 'SS00' and adm2_pcode <> 'SS0807'",
}

gdal_parquet_options = [
    "--overwrite",
    "--quiet",
    "--lco=USE_PARQUET_GEO_TYPES=YES",
    "--lco=COMPRESSION_LEVEL=15",
    "--lco=COMPRESSION=ZSTD",
]
