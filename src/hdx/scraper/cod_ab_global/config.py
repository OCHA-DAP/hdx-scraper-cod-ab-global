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


def is_bool(string: str) -> bool:
    """Check if a string is boolean-like."""
    return string.upper() in ("YES", "TRUE", "ON", "1")


OBJECTID = "esriFieldTypeOID"

ARCGIS_SERVER = getenv("ARCGIS_SERVER", "https://gis.unocha.org")
ARCGIS_USERNAME = getenv("ARCGIS_USERNAME", "")
ARCGIS_PASSWORD = getenv("ARCGIS_PASSWORD", "")
ARCGIS_FOLDER = getenv("ARCGIS_FOLDER", "Hosted")
ARCGIS_SERVICE_URL = f"{ARCGIS_SERVER}/server/rest/services/{ARCGIS_FOLDER}"
ARCGIS_SERVICE_REGEX = getenv("ARCGIS_SERVICE_REGEX", r"cod_ab_[a-z]{3}$")
ARCGIS_SERVICE_VERSIONED_REGEX = getenv(
    "ARCGIS_SERVICE_VERSIONED_REGEX",
    r"cod_ab_[a-z]{3}_v_?\d{2}$",
)
ARCGIS_LAYER_REGEX = getenv("ARCGIS_LAYER_REGEX", r"^[a-z]{3}_admin\d$")
ARCGIS_METADATA = getenv("ARCGIS_METADATA", "COD_Global_Metadata")
ARCGIS_METADATA_URL = f"{ARCGIS_SERVICE_URL}/{ARCGIS_METADATA}/FeatureServer/0"

ATTEMPT = int(getenv("ATTEMPT", "5"))
WAIT = int(getenv("WAIT", "10"))
TIMEOUT = int(getenv("TIMEOUT", "60"))
TIMEOUT_DOWNLOAD = int(getenv("TIMEOUT_DOWNLOAD", "600"))
EXPIRATION = int(getenv("EXPIRATION", "1440"))  # minutes (1 day)

RUN_DOWNLOAD = is_bool(getenv("RUN_DOWNLOAD", "YES"))
RUN_CHECK = is_bool(getenv("RUN_CHECK", "YES"))
RUN_ORIGINAL = is_bool(getenv("RUN_ORIGINAL", "YES"))
RUN_EXTENDED_PRE = is_bool(getenv("RUN_EXTENDED_PRE", "YES"))
RUN_EXTENDED_POST = is_bool(getenv("RUN_EXTENDED_POST", "YES"))
RUN_ALIGNED = is_bool(getenv("RUN_ALIGNED", "YES"))
RUN_ORIGINAL = is_bool(getenv("RUN_ORIGINAL", "YES"))
RUN_DATASETS = is_bool(getenv("RUN_DATASETS", "YES"))

ISO_3_LEN = 3
iso3_include = [
    x.upper() for x in getenv("ISO3_INCLUDE", "").split(",") if len(x) == ISO_3_LEN
]
iso3_exclude = [
    x.upper() for x in getenv("ISO3_EXCLUDE", "").split(",") if len(x) == ISO_3_LEN
]

where_filter = {
    "LBN": "adm1_pcode <> 'Conflict'",
    "PAK": "adm1_pcode not in ('PK1', 'PK3')",
    "SDN": "adm1_pcode <> 'SD19'",
    "SSD": "adm1_pcode <> 'SS00'",
}

bnda_disp = ["xAB"]
