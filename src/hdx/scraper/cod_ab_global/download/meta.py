from pathlib import Path
from subprocess import run
from urllib.parse import urlencode

from pandas import read_parquet, to_datetime

from ..utils import client_get
from .utils import parse_fields


def main(data_dir: Path, url: str, token: str) -> None:
    """Download the metadata table from a Feature Layer."""
    params = {"f": "json", "token": token}
    fields = client_get(url, params).json()["fields"]
    objectid, field_names = parse_fields(fields)
    query = {
        **params,
        "orderByFields": objectid,
        "outFields": field_names,
        "where": "1=1",
    }
    query_url = f"{url}/query?{urlencode(query)}"
    output_file = data_dir / "metadata.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            *["gdal", "vector", "convert"],
            *["ESRIJSON:" + query_url, output_file],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )
    # TODO (Max): change back to parquet once issue addressed:  # noqa: FIX002
    # https://github.com/OSGeo/gdal/issues/13093
    df = read_parquet(output_file)
    for col in df.columns:
        try:
            if col.startswith("date_"):
                df[col] = to_datetime(df[col], format="ISO8601")
                df[col] = df[col].dt.date
        except ValueError:
            pass
    df.to_parquet(data_dir / "metadata.parquet", compression="zstd")


def cleanup_metadata(data_dir: Path) -> None:
    """Cleanup downloaded files."""
    output_file = data_dir / "metadata.parquet"
    output_file.unlink(missing_ok=True)
