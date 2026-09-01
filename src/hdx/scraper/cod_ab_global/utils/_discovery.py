"""Discover COD-AB ArcGIS service names and their COD_Global_Metadata rows."""

import re

import httpx

from hdx.scraper.cod_ab_global.config import (
    ARCGIS_SERVER,
    ARCGIS_SERVICES_URL,
    ARCGIS_TIMEOUT,
)

from ._auth import fetch_json

_METADATA_TABLE_URL = (
    f"{ARCGIS_SERVER}/server/rest/services/Hosted/COD_Global_Metadata/FeatureServer/0"
)

# Matches cod_ab_<ISO3> and cod_ab_<ISO3>_v<N>, excludes non-country entries
# like COD_AB_Style_Template.
_SERVICE_RE = re.compile(r"^cod_ab_[a-z]{3}(_v\d+)?$", re.IGNORECASE)


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

    Keys unversioned entries to the latest row per ISO3 and versioned
    entries to their own service name.
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

    return result


def list_services(token: str) -> list[str]:
    """Return COD-AB service names (cod_ab_<ISO3> and versioned) from Hosted."""
    data = fetch_json(ARCGIS_SERVICES_URL, token)
    results = []
    for s in data.get("services", []):
        if s.get("type") != "FeatureServer":
            continue
        # Names are returned as "Hosted/service_name", strip the folder prefix
        name = s["name"].split("/")[-1]
        if _SERVICE_RE.match(name):
            results.append(name)
    return results
