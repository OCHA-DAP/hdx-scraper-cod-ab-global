from pathlib import Path
from re import search

from tqdm import tqdm

from ..config import (
    ARCGIS_LAYER_REGEX,
    ARCGIS_SERVICE_URL,
    ARCGIS_SERVICE_VERSIONED_REGEX,
)
from ..utils import client_get
from .utils import download_feature


def download_layers(output_dir: Path, url: str, params: dict, layers: dict) -> None:
    """Download all ESRIJSON from a Feature Service."""
    for layer in layers:
        if layer["type"] == "Feature Layer":
            feature_url = f"{url}/{layer['id']}"
            response = client_get(feature_url, params).json()
            if search(ARCGIS_LAYER_REGEX, response["name"]):
                download_feature(output_dir, feature_url, params, response)


def main(data_dir: Path, token: str) -> None:
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
        pbar.set_postfix_str(service["name"].split("/")[-1])
        service_name = service["name"].split("/")[-1]
        output_dir = data_dir / "boundaries" / service_name.replace("_v_", "_v")
        output_dir.mkdir(parents=True, exist_ok=True)
        service_url = f"{ARCGIS_SERVICE_URL}/{service_name}/FeatureServer"
        layers = client_get(service_url, params).json()["layers"]
        download_layers(output_dir, service_url, params, layers)
