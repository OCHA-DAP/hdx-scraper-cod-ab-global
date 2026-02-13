from pathlib import Path
from subprocess import run

from httpx import Client, Response
from pandas import DataFrame, read_csv
from tenacity import retry, stop_after_attempt, wait_fixed

from .config import (
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_SERVICE_URL,
    ARCGIS_USERNAME,
    ATTEMPT,
    EXPIRATION,
    TIMEOUT,
    WAIT,
)

cwd = Path(__file__).parent


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
def client_get(url: str, params: dict | None = None) -> Response:
    """HTTP GET with retries, waiting, and longer timeouts."""
    with Client(http2=True, timeout=TIMEOUT) as client:
        return client.get(url, params=params)


def _get_admin_level_full(iso3: str) -> int:
    """Get admin level with full country coverage.

    TODO: change custon CSV to Parquet once metadata is ready.
    """
    df = read_csv(cwd / "config" / "metadata.csv")
    return df[(df["country_iso3"] == iso3) & df["version"].isna()].to_dict(
        "records",
    )[0]["admin_level_full"]


def get_columns(admin_level: int) -> list[str]:
    """Get a list of column names for the given admin level."""
    columns = []
    for level in range(admin_level, -1, -1):
        columns += [f"adm{level}_name"]
        columns += [f"adm{level}_name1", f"adm{level}_name2", f"adm{level}_name3"]
        columns += [f"adm{level}_pcode"]
    columns += ["lang", "lang1", "lang2", "lang3"]
    columns += ["iso2", "iso3", "version", "valid_on", "valid_to", "adm_origin"]
    return columns


def _get_feature_server_url(iso3: str) -> str:
    """Get a url for a feature server."""
    return f"{ARCGIS_SERVICE_URL}/cod_ab_{iso3.lower()}/FeatureServer"


def generate_token() -> str:
    """Generate a token for ArcGIS Server."""
    url = f"{ARCGIS_SERVER}/portal/sharing/rest/generateToken"
    data = {
        "username": ARCGIS_USERNAME,
        "password": ARCGIS_PASSWORD,
        "referer": f"{ARCGIS_SERVER}/portal",
        "expiration": EXPIRATION,
        "f": "json",
    }
    with Client(http2=True) as client:
        r = client.post(url, data=data).json()
        return r["token"]


def _to_parquet(output_path: Path) -> None:
    """Convert to GeoParquet."""
    run(
        [
            *["gdal", "vector", "select"],
            *[output_path, output_path.with_suffix(".parquet")],
            *["--exclude", "fid"],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )
    output_path.unlink()


def _save_metadata_files(output_file: Path, df: DataFrame) -> None:
    """Save metadata in parquet and csv."""
    df.to_parquet(
        output_file,
        compression="zstd",
        compression_level=15,
        index=False,
    )
    df.to_csv(
        output_file.with_suffix(".csv"),
        index=False,
        encoding="utf-8-sig",
    )


def save_metadata(output_file: Path, df_all: DataFrame) -> None:
    """Save metadata in with all and latest versions."""
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_all"),
        df_all,
    )
    df_latest = df_all.drop_duplicates(subset=["country_iso3"], keep="last")
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_latest"),
        df_latest,
    )
    key_columns = ["country_iso3", "version"]
    df_historic = df_all.merge(
        df_latest[key_columns],
        on=key_columns,
        how="left",
        indicator=True,
    )
    df_historic = df_historic[df_historic["_merge"] == "left_only"].drop(
        columns=["_merge"],
    )
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_historic"),
        df_historic,
    )
