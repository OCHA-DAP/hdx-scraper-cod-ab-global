from pathlib import Path
from re import search
from shutil import rmtree

from tqdm import tqdm

from ...config import (
    ARCGIS_LAYER_REGEX,
    ARCGIS_SERVICE_URL,
    ARCGIS_SERVICE_VERSIONED_REGEX,
    iso3_exclude,
    iso3_include,
)
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


def is_service_allowed(iso3: str, version: str) -> bool:
    """Check if a layer is allowed to be downloaded."""
    exclude_version = ""
    if iso3_exclude:
        if any(x.startswith(iso3) and x != iso3 for x in iso3_exclude):
            version_exclude = next(x for x in iso3_exclude if x.startswith(iso3))
            version_exclude = version_exclude.split("_")[1].lower()
            if version == version_exclude:
                exclude_version = version
        elif iso3 in iso3_exclude:
            return False
    if iso3_include:
        if any(x.startswith(iso3) and x != iso3 for x in iso3_include):
            version_include = next(x for x in iso3_include if x.startswith(iso3))
            version_include = version_include.split("_")[1].lower()
            return version == version_include
        if iso3 in iso3_include:
            return version != exclude_version
        return False
    return True


def download_boundaries(data_dir: Path, token: str) -> None:
    """Download all ESRIJSON from Feature Services."""
    params = {"f": "json", "token": token}
    response = client_get(ARCGIS_SERVICE_URL, params).json()
    services = [
        x
        for x in response["services"]
        if x["type"] == "FeatureServer"
        and search(ARCGIS_SERVICE_VERSIONED_REGEX, x["name"])
    ]
    pbar = tqdm(services)
    for service in pbar:
        service_name_in = service["name"].split("/")[-1]
        service_name_out = service_name_in.replace("_v_", "_v")
        iso3 = service_name_out.split("_")[2].upper()
        version = service_name_out.split("_")[-1].lower()
        pbar.set_postfix_str(f"{iso3} {version}")
        if not is_service_allowed(iso3, version):
            continue
        output_dir = data_dir / "country" / "original" / service_name_out
        output_dir.mkdir(parents=True, exist_ok=True)
        service_url = f"{ARCGIS_SERVICE_URL}/{service_name_in}/FeatureServer"
        layers = client_get(service_url, params).json()["layers"]
        download_layers(output_dir, service_url, params, layers)
        if sorted(output_dir.glob("*.parquet"))[-1].stem[-1] == "0":
            rmtree(output_dir)
