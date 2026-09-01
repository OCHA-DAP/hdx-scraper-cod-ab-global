"""Edge-match COD-AB boundaries from extended/ into matched/.

Clips each admin1+ layer to UN 1:1M international boundaries via topo_tools.
Admin0 is excluded; layer naming mirrors original/ and extended/ exactly.
"""

import logging
from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import (
    admin_layers,
    file_fingerprint,
    portolan_add,
    read_json_state,
    remove_stale_versions,
    write_json_state,
)
from hdx.scraper.cod_ab_global.config import PORTOLAN_WORKERS
from hdx.scraper.cod_ab_global.extended import enumerate_services

from ._bnda import ensure_bnda
from ._clip import process_service

logger = logging.getLogger(__name__)


def run(extended_dir: Path, matched_dir: Path, catalogs_dir: Path) -> None:
    """Edge-match admin1+ layers from extended/ into matched/."""
    services = enumerate_services(extended_dir)
    if not services:
        logger.warning("No services found in %s, run extended first", extended_dir)
        return
    logger.info("Found %d services to process for matched", len(services))

    remove_stale_versions(set(services), matched_dir)
    bnda_path = ensure_bnda(catalogs_dir)
    workers = str(PORTOLAN_WORKERS)

    fingerprints_path = matched_dir / ".state" / "fingerprints.json"
    stored_fingerprints = read_json_state(fingerprints_path)
    new_fingerprints: dict[str, list[int]] = {}

    for iso3, version in services:
        key = f"{iso3}/{version}"
        extended_version_dir = extended_dir / iso3 / version
        matched_version_dir = matched_dir / iso3 / version

        layers = admin_layers(extended_version_dir, iso3)
        if not layers:
            continue
        deepest_level, deepest_dir = layers[-1]
        deepest_path = deepest_dir / f"{iso3}_admin{deepest_level}.parquet"
        fingerprint = file_fingerprint(deepest_path)
        if fingerprint is None:
            continue
        new_fingerprints[key] = fingerprint

        if fingerprint == stored_fingerprints.get(key) and matched_version_dir.exists():
            logger.debug("Skipping unchanged matched for %s/%s", iso3, version)
            continue

        logger.info("Processing matched for %s/%s", iso3, version)
        if not process_service(
            iso3, version, extended_version_dir, matched_version_dir, bnda_path
        ):
            logger.warning(
                "Matched processing failed for %s/%s, will retry next run",
                iso3,
                version,
            )
            continue

        portolan_add(matched_dir, f"{iso3}/{version}/", workers)

    write_json_state(fingerprints_path, new_fingerprints)
