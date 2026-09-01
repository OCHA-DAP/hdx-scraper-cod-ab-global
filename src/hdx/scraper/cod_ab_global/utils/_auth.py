"""ArcGIS Enterprise token generation and authenticated JSON fetches."""

import httpx

from hdx.scraper.cod_ab_global.config import (
    ARCGIS_EXPIRATION,
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
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
