"""Mirror OCHA COD-AB ArcGIS FeatureServer services into original/.

Uses portolan's native extract_arcgis_catalog(), skipping a service when its
ArcGIS lastEditDate fingerprint is unchanged from the last successful run.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError

import geoparquet_io.core.arcgis as _gpio_arcgis
import portolan_cli.extract.arcgis.discovery as _arcgis_discovery
import pyarrow as pa
import yaml
from hdx.location.country import Country
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    extract_arcgis_catalog,
)

from .catalog import (
    _portolan,
    portolan_add,
    read_json_state,
    remove_stale_versions,
    write_json_state,
)
from .config import (
    ARCGIS_SERVICES_URL,
    PORTOLAN_WORKERS,
    admin_level_full_overrides,
)
from .utils import fetch_json, fetch_metadata_table, generate_token, list_services

logger = logging.getLogger(__name__)

# discover_layers() has no token param, unlike discover_services() (portolan-cli#855).
_orig_fetch_json = _arcgis_discovery._fetch_json  # noqa: SLF001
_ARCGIS_TOKEN: str | None = None


def _patched_fetch_json(
    url: str, timeout: float = 60.0, token: str | None = None
) -> dict:
    return _orig_fetch_json(url, timeout=timeout, token=token or _ARCGIS_TOKEN)


_arcgis_discovery._fetch_json = _patched_fetch_json  # noqa: SLF001

# GDAL misdetects ESRIJSON as GeoJSON when features[] precedes the ID keys
# (geoparquet-io#501).
_orig_esrijson_page_to_table = _gpio_arcgis._esrijson_page_to_table  # noqa: SLF001
_ESRIJSON_HEADER_KEYS = (
    "geometryType",
    "spatialReference",
    "fields",
    "objectIdFieldName",
)


def _patched_esrijson_page_to_table(page: dict, con: object = None) -> object:
    if not page.get("features"):
        return None
    reordered = {k: page[k] for k in _ESRIJSON_HEADER_KEYS if k in page}
    reordered.update(page)
    return _gpio_arcgis._json_doc_to_table(  # noqa: SLF001
        reordered, exclude="geom", suffix=".json", con=con
    )


_gpio_arcgis._esrijson_page_to_table = _patched_esrijson_page_to_table  # noqa: SLF001

# esriFieldTypeBigInteger is missing from gpio's TYPE_MAPPING, so fields of
# that type silently fall back to string instead of int64.
_orig_build_schema_from_layer_info = _gpio_arcgis._build_schema_from_layer_info  # noqa: SLF001


def _patched_build_schema_from_layer_info(layer_info: object) -> pa.Schema:
    schema = _orig_build_schema_from_layer_info(layer_info)
    big_int_fields = {
        f["name"] for f in layer_info.fields if f["type"] == "esriFieldTypeBigInteger"
    }
    if not big_int_fields:
        return schema
    return pa.schema(
        [
            field.with_type(pa.int64()) if field.name in big_int_fields else field
            for field in schema
        ]
    )


_gpio_arcgis._build_schema_from_layer_info = (  # noqa: SLF001
    _patched_build_schema_from_layer_info
)


def _service_to_path(service_name: str) -> tuple[str, str]:
    """Return (iso3, version) for a COD-AB service name.

    cod_ab_eth_v04 -> ("eth", "v04")
    cod_ab_eth     -> ("eth", "latest")
    """
    # cod_ab_<iso3>[_<version>]: 3 parts unversioned, 4 parts versioned
    parts = service_name.lower().split("_")
    iso3 = parts[2]
    version = parts[3] if len(parts) > 3 else "latest"  # noqa: PLR2004
    return iso3, version


# All meaningful fields from COD_Global_Metadata (mirrors refactor.py's column list).
# Written as cod_ab:* custom STAC properties so the table is reconstructable from
# the catalog.
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


def _enrich_service_catalog(service_dir: Path, meta: dict) -> None:
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


def _write_service_metadata(
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


def _last_edit_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="milliseconds")


def _extract_service(
    service_name: str,
    token: str,
    original_dir: Path,
    metadata: dict[str, dict],
    stored_fingerprints: dict[str, dict[str, str]],
) -> tuple[dict[str, str], bool]:
    """Extract one service into original/ if its ArcGIS layers changed.

    Returns (layer_updated, changed); changed is False on a fingerprint-match
    skip, so downstream stages see this service's output as untouched.
    """
    service_url = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"
    data = fetch_json(service_url, token)
    iso3, version = _service_to_path(service_name)
    version_dir = original_dir / iso3 / version

    layer_updated: dict[str, str] = {}
    for layer in data.get("layers", []):
        layer_name = layer["name"].lower().replace(" ", "_")
        if layer_name.endswith("_em"):
            continue
        layer_meta = fetch_json(f"{service_url}/{layer['id']}", token)
        last_edit = (layer_meta.get("editingInfo") or {}).get("lastEditDate")
        if last_edit is not None:
            layer_updated[layer_name] = _last_edit_to_iso(last_edit)

    if (
        version_dir.exists()
        and stored_fingerprints.get(service_name.lower()) == layer_updated
    ):
        logger.debug("Skipping unchanged service %s", service_name)
        return layer_updated, False

    logger.info("Extracting %s (source changed)", service_name)
    rmtree(version_dir, ignore_errors=True)
    options = ExtractionOptions(
        token=token,
        output_crs="EPSG:4326",
        retries=3,
        license="CC-BY-3.0-IGO",
        timeout=300,
    )
    try:
        extract_arcgis_catalog(
            service_url, version_dir, layer_exclude=["*_em"], options=options
        )
    except Exception:
        logger.exception("Extraction failed for %s", service_name)
        return layer_updated, True

    meta = metadata.get(service_name.lower())
    _write_service_metadata(version_dir, service_name, meta)
    return layer_updated, True


def _remove_stale_services(services: list[str], work_dir: Path) -> None:
    """Remove version directories no longer present in ArcGIS."""
    remove_stale_versions({_service_to_path(s) for s in services}, work_dir)


def run(original_dir: Path) -> None:
    """Mirror OCHA COD-AB ArcGIS services into original/.

    Requires: ARCGIS_USERNAME, ARCGIS_PASSWORD.
    Push is handled by __main__.py after all stages complete.
    """
    global _ARCGIS_TOKEN  # noqa: PLW0603
    token = generate_token()
    _ARCGIS_TOKEN = token
    services = list_services(token)
    logger.info("Found %d COD-AB services", len(services))
    metadata = fetch_metadata_table(token)
    logger.info("Fetched metadata for %d services", len(metadata))

    fingerprints_path = original_dir / ".state" / "fingerprints.json"
    stored_fingerprints = read_json_state(fingerprints_path)

    changed_services: dict[str, bool] = {}
    new_fingerprints: dict[str, dict[str, str]] = {}

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {
            pool.submit(
                _extract_service, sn, token, original_dir, metadata, stored_fingerprints
            ): sn
            for sn in sorted(services)
        }
        for future in as_completed(futures):
            sn = futures[future]
            try:
                layer_updated, changed = future.result()
                new_fingerprints[sn.lower()] = layer_updated
                changed_services[sn] = changed
            except Exception:
                logger.exception("Extraction failed for %s, skipping", sn)
                changed_services[sn] = False

    write_json_state(fingerprints_path, new_fingerprints)
    _remove_stale_services(services, original_dir)

    workers = str(PORTOLAN_WORKERS)
    for service_name in sorted(services):
        iso3, version = _service_to_path(service_name)
        version_dir = original_dir / iso3 / version
        if not version_dir.exists() or not changed_services.get(service_name, False):
            continue

        meta = metadata.get(service_name.lower())
        date_valid_on = (meta.get("date_valid_on") or "").strip() if meta else ""
        portolan_add(
            original_dir, f"{iso3}/{version}/", workers, datetime_=date_valid_on or None
        )
        if meta:
            override = admin_level_full_overrides.get(iso3.upper())
            if override is not None:
                meta = {**meta, "admin_level_full": override}
            _enrich_service_catalog(version_dir, meta)

    try:
        _portolan(["stac-geoparquet"], cwd=original_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet reported no items, skipping")
