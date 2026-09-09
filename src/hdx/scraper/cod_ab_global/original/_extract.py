"""Per-service ArcGIS extraction into original/, plus service<->path mapping."""

import logging
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree

from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    extract_arcgis_catalog,
)

from hdx.scraper.cod_ab_global.catalog import remove_stale_versions
from hdx.scraper.cod_ab_global.config import ARCGIS_SERVICES_URL
from hdx.scraper.cod_ab_global.extended import _write_gpq2
from hdx.scraper.cod_ab_global.utils import fetch_json

from ._metadata import write_service_metadata

logger = logging.getLogger(__name__)


def service_to_path(service_name: str) -> tuple[str, str]:
    """Return (iso3, version) for a COD-AB service name.

    cod_ab_eth_v04 -> ("eth", "v04")
    cod_ab_eth     -> ("eth", "latest")
    """
    # cod_ab_<iso3>[_<version>]: 3 parts unversioned, 4 parts versioned
    parts = service_name.lower().split("_")
    iso3 = parts[2]
    version = parts[3] if len(parts) > 3 else "latest"  # noqa: PLR2004
    return iso3, version


def _last_edit_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="milliseconds")


def extract_service(
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
    iso3, version = service_to_path(service_name)
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

    for parquet_path in version_dir.glob("*/*.parquet"):
        tmp_path = parquet_path.with_suffix(".gpq2.tmp")
        _write_gpq2(parquet_path, tmp_path)
        tmp_path.replace(parquet_path)

    meta = metadata.get(service_name.lower())
    write_service_metadata(version_dir, service_name, meta)
    return layer_updated, True


def remove_stale_services(services: list[str], work_dir: Path) -> None:
    """Remove version directories no longer present in ArcGIS."""
    remove_stale_versions({service_to_path(s) for s in services}, work_dir)
