"""Write COD_Global_Metadata fields into a service's catalog.json/metadata.yaml."""

import json
from pathlib import Path

import yaml
from hdx.location.country import Country

from hdx.scraper.cod_ab_global.config import ARCGIS_SERVICES_URL

# Mirrors refactor.py's column list; written as cod_ab:* STAC properties so
# the table is reconstructable from the catalog.
_COD_AB_METADATA_FIELDS = [
    "country_name",
    "country_iso2",
    "country_iso3",
    "version",
    "admin_level_full",
    "admin_level_max",
    "admin_1_name",
    "admin_2_name",
    "admin_3_name",
    "admin_4_name",
    "admin_5_name",
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
    "admin_notes",
    "date_source",
    "date_updated",
    "date_reviewed",
    "date_metadata",
    "date_valid_on",
    "date_valid_to",
    "update_frequency",
    "update_type",
    "source",
    "contributor",
    "methodology_dataset",
    "methodology_pcodes",
    "caveats",
]


def enrich_service_catalog(service_dir: Path, meta: dict) -> None:
    """Write COD_Global_Metadata fields as cod_ab:* properties in catalog.json."""
    catalog_path = service_dir / "catalog.json"
    if not catalog_path.exists():
        return
    data = json.loads(catalog_path.read_text())
    for field in _COD_AB_METADATA_FIELDS:
        value = meta.get(field)
        if value is not None and str(value).strip():
            data[f"cod_ab:{field}"] = value
    iso3 = (meta.get("country_iso3") or "").upper()
    if "cod_ab:country_iso2" not in data:
        iso2 = Country.get_iso2_from_iso3(iso3) if iso3 else None
        if iso2:
            data["cod_ab:country_iso2"] = iso2
    catalog_path.write_text(json.dumps(data, indent=2))


def write_service_metadata(
    service_dir: Path, service_name: str, meta: dict | None
) -> None:
    """Write .portolan/metadata.yaml for a service (subcatalog)."""
    if not meta:
        return

    content: dict = {}

    contributor = (meta.get("contributor") or "").strip()
    source = (meta.get("source") or "").strip()
    if contributor and source:
        content["attribution"] = f"{contributor} / {source}"
    elif contributor or source:
        content["attribution"] = contributor or source

    content["source_url"] = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"

    version = (meta.get("version") or "").strip()
    if version:
        content["upstream_version"] = version

    caveats = (meta.get("caveats") or "").strip()
    if caveats:
        content["known_issues"] = caveats

    notes = (meta.get("admin_notes") or "").strip()
    if notes:
        content["processing_notes"] = notes

    date_valid_on = (meta.get("date_valid_on") or "").strip()
    date_valid_to = (meta.get("date_valid_to") or "").strip() or None
    if date_valid_on:
        temporal: dict = {"start": date_valid_on}
        if date_valid_to:
            temporal["end"] = date_valid_to
        content["defaults"] = {"temporal": temporal}

    portolan_dir = service_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            content, default_flow_style=False, allow_unicode=True, sort_keys=False
        )
    )
