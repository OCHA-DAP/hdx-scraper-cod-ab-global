"""ArcGIS HTTP helpers for token generation, JSON fetching, and service discovery."""

import re

import httpx

from hdx.scraper.cod_ab_global.config import admin_level_full_overrides

from .config import (
    ARCGIS_EXPIRATION,
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_SERVICES_URL,
    ARCGIS_TIMEOUT,
    ARCGIS_TOKEN_URL,
    ARCGIS_USERNAME,
)

_METADATA_TABLE_URL = (
    f"{ARCGIS_SERVER}/server/rest/services/Hosted/COD_Global_Metadata/FeatureServer/0"
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


def _is_newer(row: dict, current: dict | None) -> bool:
    """Return True if row should replace current as the latest for its ISO3."""
    if current is None:
        return True
    cur_expired = current.get("date_valid_to") is not None
    new_expired = row.get("date_valid_to") is not None
    if cur_expired != new_expired:
        return not new_expired
    return (row.get("date_valid_on") or "") > (current.get("date_valid_on") or "")


def fetch_metadata_table(token: str) -> dict[str, dict]:
    """Return {service_name_lower: attrs} for all COD-AB metadata rows.

    Includes versioned entries (cod_ab_afg_v01) keyed via feature_server_url,
    versioned fallback entries via (iso3, version) when the URL is malformed,
    and unversioned entries (cod_ab_afg) mapped to the latest row per ISO3.
    """
    with httpx.Client(http2=True, timeout=ARCGIS_TIMEOUT) as client:
        r = client.get(
            f"{_METADATA_TABLE_URL}/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "resultRecordCount": "2000",
                "f": "json",
                "token": token,
            },
        )
        r.raise_for_status()
        rows = [f["attributes"] for f in r.json().get("features", [])]

    result: dict[str, dict] = {}
    by_iso3_version: dict[tuple[str, str], dict] = {}
    by_iso3_latest: dict[str, dict] = {}

    for row in rows:
        url = row.get("feature_server_url") or ""
        if "/Hosted/" in url and "/FeatureServer" in url:
            svc = url.split("/Hosted/")[-1].split("/")[0].lower()
            result[svc] = row

        iso3 = (row.get("country_iso3") or "").lower()
        version = (row.get("version") or "").lower()
        if iso3 and version:
            by_iso3_version[(iso3, version)] = row
        if iso3 and _is_newer(row, by_iso3_latest.get(iso3)):
            by_iso3_latest[iso3] = row

    for (iso3, version), row in by_iso3_version.items():
        svc = f"cod_ab_{iso3}_{version}"
        if svc not in result:
            result[svc] = row

    for iso3, row in by_iso3_latest.items():
        result[f"cod_ab_{iso3}"] = row

    for row in result.values():
        iso3_upper = (row.get("country_iso3") or "").upper()
        if iso3_upper in admin_level_full_overrides:
            row["admin_level_full"] = admin_level_full_overrides[iso3_upper]

    return result


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
