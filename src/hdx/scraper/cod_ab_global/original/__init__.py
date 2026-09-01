"""Mirror OCHA COD-AB ArcGIS FeatureServer services into original/.

Uses portolan's native extract_arcgis_catalog(), skipping a service when its
ArcGIS lastEditDate fingerprint is unchanged from the last successful run.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from subprocess import CalledProcessError

from hdx.scraper.cod_ab_global.catalog import (
    _portolan,
    portolan_add,
    read_json_state,
    write_json_state,
)
from hdx.scraper.cod_ab_global.config import (
    PORTOLAN_WORKERS,
    admin_level_full_overrides,
)
from hdx.scraper.cod_ab_global.utils import (
    fetch_metadata_table,
    generate_token,
    list_services,
)

from . import _patches
from ._extract import extract_service, remove_stale_services, service_to_path
from ._metadata import enrich_service_catalog

logger = logging.getLogger(__name__)


def run(original_dir: Path) -> None:
    """Mirror OCHA COD-AB ArcGIS services into original/.

    Requires: ARCGIS_USERNAME, ARCGIS_PASSWORD.
    Push is handled by __main__.py after all stages complete.
    """
    token = generate_token()
    _patches.set_token(token)
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
                extract_service, sn, token, original_dir, metadata, stored_fingerprints
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
    remove_stale_services(services, original_dir)

    workers = str(PORTOLAN_WORKERS)
    for service_name in sorted(services):
        iso3, version = service_to_path(service_name)
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
            enrich_service_catalog(version_dir, meta)

    try:
        _portolan(["stac-geoparquet"], cwd=original_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet reported no items, skipping")
