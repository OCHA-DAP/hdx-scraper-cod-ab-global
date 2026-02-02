from pathlib import Path
from subprocess import run
from urllib.parse import urlencode

from ..config import ARCGIS_ADM0_URL, gdal_parquet_options
from ..utils import client_get
from .utils import parse_fields


def download_admin0(data_dir: Path, token: str) -> None:
    """Download Admin 0 from Feature Services."""
    params = {"f": "json", "token": token}
    fields = client_get(ARCGIS_ADM0_URL, params).json()["fields"]
    objectid, field_names = parse_fields(fields)
    query = {
        **params,
        "orderByFields": objectid,
        "outFields": field_names,
        "where": "1=1",
    }
    query_url = f"{ARCGIS_ADM0_URL}/query?{urlencode(query)}"
    output_file = data_dir / "bnda_cty.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            *["gdal", "vector", "pipeline", "!"],
            *["read", "ESRIJSON:" + query_url, "!"],
            *["reproject", "--dst-crs=EPSG:4326", "!"],
            *["clean-coverage", "!"],
            *["make-valid", "!"],
            *["write", output_file],
            *gdal_parquet_options,
        ],
        check=False,
    )
