from pathlib import Path
from re import search
from shutil import rmtree

from pandas import read_parquet
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

from ...config import ARCGIS_LAYER_REGEX, ARCGIS_SERVICE_URL, ATTEMPT, WAIT
from ...utils import client_get
from .feature import download_feature


def download_layers(output_dir: Path, url: str, params: dict, layers: dict) -> None:
    """Download all ESRIJSON from a Feature Service."""
    for layer in layers:
        if layer["type"] == "Feature Layer":
            feature_url = f"{url}/{layer['id']}"
            response = client_get(feature_url, params).json()
            if search(ARCGIS_LAYER_REGEX, response["name"]):
                download_feature(output_dir, feature_url, params, response)


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
def download_services(data_dir: Path, params: dict, iso3: str, version: str) -> None:
    """Download all ESRIJSON from Feature Services for a country and version."""
    service_name_out = f"cod_ab_{iso3.lower()}_{version}"
    output_dir = data_dir / "country" / "original" / service_name_out
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        service_name_in = service_name_out
        service_url = f"{ARCGIS_SERVICE_URL}/{service_name_in}/FeatureServer"
        layers = client_get(service_url, params).json()["layers"]
    except KeyError:
        service_name_in = service_name_out.replace("_v", "_v_")
        service_url = f"{ARCGIS_SERVICE_URL}/{service_name_in}/FeatureServer"
        layers = client_get(service_url, params).json()["layers"]
    download_layers(output_dir, service_url, params, layers)
    if sorted(output_dir.glob("*.parquet"))[-1].stem[-1] == "0":
        rmtree(output_dir)


def download_boundaries(data_dir: Path, token: str, run_version: str) -> None:
    """Download all ESRIJSON from Feature Services."""
    params = {"f": "json", "token": token}
    services = read_parquet(
        data_dir / f"metadata/global_admin_boundaries_metadata_{run_version}.parquet",
        columns=["country_iso3", "version"],
    ).itertuples(index=False, name=None)
    pbar = tqdm(services)
    for iso3, version in pbar:
        pbar.set_description(f"{iso3} {version}")
        download_services(data_dir, params, iso3, version)
