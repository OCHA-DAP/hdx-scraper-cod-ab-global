import re
from pathlib import Path
from subprocess import run

from httpx import Client, Response
from pandas import DataFrame, read_csv
from tenacity import retry, stop_after_attempt, wait_fixed

from .config import (
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_SERVICE_REGEX,
    ARCGIS_SERVICE_URL,
    ARCGIS_USERNAME,
    ATTEMPT,
    EXPIRATION,
    TIMEOUT,
    WAIT,
    iso3_exclude,
    iso3_include,
)

cwd = Path(__file__).parent


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
def client_get(url: str, params: dict | None = None) -> Response:
    """HTTP GET with retries, waiting, and longer timeouts."""
    with Client(http2=True, timeout=TIMEOUT) as client:
        return client.get(url, params=params)


def get_admin_level_full(iso3: str) -> int:
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


def get_feature_server_url(iso3: str) -> str:
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


def to_parquet(output_path: Path) -> None:
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


def save_metadata(output_file: Path, df: DataFrame) -> None:
    """Save metadata in with all and latest versions."""
    df.to_parquet(
        output_file.with_stem(output_file.stem + "_all"),
        compression="zstd",
        compression_level=15,
        index=False,
    )
    df.to_csv(
        output_file.with_stem(output_file.stem + "_all").with_suffix(".csv"),
        index=False,
        encoding="utf-8-sig",
    )
    df = df.drop_duplicates(subset=["country_iso3"], keep="last")
    df.to_parquet(
        output_file.with_stem(output_file.stem + "_latest"),
        compression="zstd",
        compression_level=15,
        index=False,
    )
    df.to_csv(
        output_file.with_stem(output_file.stem + "_latest").with_suffix(".csv"),
        index=False,
        encoding="utf-8-sig",
    )


def get_iso3_list(token: str) -> list[str]:
    """Get a list of ISO3 codes available on the ArcGIS server."""
    params = {"f": "json", "token": token}
    services = client_get(ARCGIS_SERVICE_URL, params=params).json()["services"]
    p = re.compile(ARCGIS_SERVICE_REGEX)
    iso3_list = [
        x["name"][14:17].upper()
        for x in services
        if x["type"] == "FeatureServer" and p.search(x["name"])
    ]
    return [
        iso3
        for iso3 in iso3_list
        if (not iso3_include or iso3 in iso3_include)
        and (not iso3_exclude or iso3 not in iso3_exclude)
    ]
