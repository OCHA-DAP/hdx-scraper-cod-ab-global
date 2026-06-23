"""ArcGIS HTTP helpers for token generation, JSON fetching, and service discovery."""

import re

import httpx

from .config import (
    ARCGIS_EXPIRATION,
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_SERVICES_URL,
    ARCGIS_TIMEOUT,
    ARCGIS_TOKEN_URL,
    ARCGIS_USERNAME,
)

# Matches cod_ab_<ISO3> and cod_ab_<ISO3>_v<N> — excludes non-country entries
# like COD_AB_Style_Template.
_SERVICE_RE = re.compile(r"^cod_ab_[a-z]{3}(_v\d+)?$", re.IGNORECASE)


def generate_token() -> str:
    """Generate an ArcGIS Enterprise token via username/password authentication."""
    with httpx.Client(http2=True) as client:
        r = client.post(
            ARCGIS_TOKEN_URL,
            data={
                "username": ARCGIS_USERNAME,
                "password": ARCGIS_PASSWORD,
                "referer": f"{ARCGIS_SERVER}/portal",
                "expiration": str(ARCGIS_EXPIRATION),
                "f": "json",
            },
        )
        r.raise_for_status()
        return r.json()["token"]


def fetch_json(url: str, token: str) -> dict:
    """Fetch a JSON response from an ArcGIS REST endpoint with token auth."""
    with httpx.Client(http2=True, timeout=ARCGIS_TIMEOUT) as client:
        r = client.get(url, params={"f": "json", "token": token})
        r.raise_for_status()
        return r.json()


def list_services(token: str) -> list[str]:
    """Return COD-AB service names (cod_ab_<ISO3> and versioned) from Hosted."""
    data = fetch_json(ARCGIS_SERVICES_URL, token)
    results = []
    for s in data.get("services", []):
        if s.get("type") != "FeatureServer":
            continue
        # Names are returned as "Hosted/service_name" — strip the folder prefix
        name = s["name"].split("/")[-1]
        if _SERVICE_RE.match(name):
            results.append(name)
    return results
