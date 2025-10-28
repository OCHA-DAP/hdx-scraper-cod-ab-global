from pathlib import Path

from ..config import where_filter
from ..utils import client_get, get_admin_level_full, get_feature_server_url
from .utils import download_feature


def main(data_dir: Path, token: str, iso3: str) -> None:
    """Download a ESRIJSON from a Feature Layer."""
    params = {"f": "json", "token": token}
    url = get_feature_server_url(iso3)
    response_layers = client_get(url, params).json()
    admin_level_full = get_admin_level_full(iso3)
    layer = next(
        x
        for x in response_layers["layers"]
        if x["type"] == "Feature Layer"
        and x["name"] == f"{iso3.lower()}_admin{admin_level_full}"
    )
    feature_url = f"{url}/{layer['id']}"
    response_feature = client_get(feature_url, params).json()
    where_query = where_filter.get(iso3, "1=1")
    output_dir = data_dir / "edge_matched" / "cod_ab"
    download_feature(output_dir, feature_url, params, response_feature, where_query)
