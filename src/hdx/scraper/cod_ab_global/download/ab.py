from pathlib import Path
from re import search

from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

from ..config import (
    ARCGIS_LAYER_REGEX,
    ARCGIS_SERVICE_URL,
    ARCGIS_SERVICE_VERSIONED_REGEX,
    ATTEMPT,
    WAIT,
)
from ..create.ab import main as create_ab
from ..utils import client_get
from .utils import download_feature


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
def download_layers(output_dir: Path, url: str, params: dict, layers: dict) -> None:
    """Download all ESRIJSON from a Feature Service."""
    for layer in layers:
        name_match = search(ARCGIS_LAYER_REGEX, layer["name"])
        if layer["type"] == "Feature Layer" and name_match:
            feature_url = f"{url}/{layer['id']}"
            response = client_get(feature_url, params).json()
            download_feature(output_dir, feature_url, params, response)
            create_ab(output_dir, response["name"])


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
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
        pbar.set_postfix_str(service["name"])
        service_name = service["name"].split("/")[-1]
        output_dir = data_dir / "versioned" / service_name
        service_url = f"{ARCGIS_SERVICE_URL}/{service_name}/FeatureServer"
        layers = client_get(service_url, params).json()["layers"]
        download_layers(output_dir, service_url, params, layers)
