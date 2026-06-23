"""ArcGIS HTTP helpers for token generation, JSON fetching, and service discovery."""

from fnmatch import fnmatch

import httpx

from .config import (
    ARCGIS_EXPIRATION,
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_SERVICES_FILTER,
    ARCGIS_SERVICES_URL,
    ARCGIS_TIMEOUT,
    ARCGIS_TOKEN_URL,
    ARCGIS_USERNAME,
)


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
    """Return service names matching ARCGIS_SERVICES_FILTER from the Hosted folder."""
    data = fetch_json(ARCGIS_SERVICES_URL, token)
    results = []
    for s in data.get("services", []):
        if s.get("type") != "FeatureServer":
            continue
        # Names are returned as "Hosted/service_name" — strip the folder prefix
        name = s["name"].split("/")[-1]
        if fnmatch(name.lower(), ARCGIS_SERVICES_FILTER.lower()):
            results.append(name)
    return results
