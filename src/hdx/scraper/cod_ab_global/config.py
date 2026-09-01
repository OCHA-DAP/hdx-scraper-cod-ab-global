"""Global configuration and environment variable parsing for the COD-AB scraper."""

import logging
from os import cpu_count, environ, getenv
from pathlib import Path

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

ARCGIS_SERVER = getenv("ARCGIS_SERVER", "https://gis.unocha.org")
ARCGIS_SERVICES_URL = f"{ARCGIS_SERVER}/server/rest/services/Hosted"
ARCGIS_TOKEN_URL = f"{ARCGIS_SERVER}/portal/sharing/rest/generateToken"
ARCGIS_USERNAME = getenv("ARCGIS_USERNAME", "")
ARCGIS_PASSWORD = getenv("ARCGIS_PASSWORD", "")
ARCGIS_EXPIRATION = int(getenv("ARCGIS_EXPIRATION", "1440"))
ARCGIS_TIMEOUT = int(getenv("ARCGIS_TIMEOUT", "60"))

SOURCECOOP_REMOTE = getenv(
    "SOURCECOOP_REMOTE",
    "s3://us-west-2.opendata.source.coop/hdx/cod-ab/",
)
PORTOLAN_WORK_DIR = getenv("PORTOLAN_WORK_DIR", "")
PORTOLAN_WORKERS = int(getenv("PORTOLAN_WORKERS", str(min(cpu_count() or 4, 8))))

HDX_EXPORT_OUTPUT_DIR = getenv("HDX_EXPORT_OUTPUT_DIR", "")
# Explicit opt-in, defaulting to off — even once this pipeline is wired up as
# the main entrypoint, actually writing to HDX requires deliberately setting
# this (in addition to whatever hdx_site is configured in
# ~/.hdx_configuration.yaml), so a plain `python -m ...cod_ab_global` run
# never pushes to HDX by accident.
HDX_EXPORT_PUSH = getenv("HDX_EXPORT_PUSH", "false").strip().lower() == "true"

# ArcGIS's COD_Global_Metadata table under-reports admin_level_full for these
# countries relative to their actual adm{N}/original.parquet depth.
admin_level_full_overrides = {
    "QAT": 3,
}

ADMIN_SCHEMA_PATH = Path(__file__).parent / "config" / "admin_schema.yaml"
