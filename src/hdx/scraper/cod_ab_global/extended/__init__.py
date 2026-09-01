"""Edge-extend COD-AB boundaries from original/ into extended/.

Layer naming mirrors original/ exactly; only the root segment differs.
"""

import logging
from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import (
    file_fingerprint,
    portolan_add,
    read_json_state,
    remove_stale_versions,
    write_json_state,
)
from hdx.scraper.cod_ab_global.config import PORTOLAN_WORKERS

from ._process import process_service
from ._scan import enumerate_services, get_admin_level_full
from ._write import write_gpq2 as _write_gpq2

__all__ = ["_write_gpq2", "enumerate_services", "run"]

logger = logging.getLogger(__name__)


def run(original_dir: Path, extended_dir: Path) -> None:
    """Edge-extend admin-polygon layers from original/ into extended/."""
    services = enumerate_services(original_dir)
    if not services:
        logger.warning("No services found in %s, run original first", original_dir)
        return
    logger.info("Found %d services to process for extended", len(services))

    remove_stale_versions(set(services), extended_dir)

    fingerprints_path = extended_dir / ".state" / "fingerprints.json"
    stored_fingerprints = read_json_state(fingerprints_path)
    new_fingerprints: dict[str, list[int]] = {}
    workers = str(PORTOLAN_WORKERS)

    for iso3, version in services:
        key = f"{iso3}/{version}"
        original_version_dir = original_dir / iso3 / version
        extended_version_dir = extended_dir / iso3 / version

        admin_level_full = get_admin_level_full(original_version_dir, iso3)
        if admin_level_full is None:
            logger.warning("Cannot determine admin_level_full for %s/%s", iso3, version)
            continue
        layer_name = f"{iso3}_admin{admin_level_full}"
        seed_src = original_version_dir / layer_name / f"{layer_name}.parquet"
        fingerprint = file_fingerprint(seed_src)
        if fingerprint is None:
            continue
        new_fingerprints[key] = fingerprint

        if (
            fingerprint == stored_fingerprints.get(key)
            and extended_version_dir.exists()
        ):
            logger.debug("Skipping unchanged extended for %s/%s", iso3, version)
            continue

        logger.info("Processing extended for %s/%s", iso3, version)
        if not process_service(
            iso3, version, original_version_dir, extended_version_dir, admin_level_full
        ):
            logger.warning(
                "Extended processing failed for %s/%s, will retry next run",
                iso3,
                version,
            )
            continue

        portolan_add(extended_dir, f"{iso3}/{version}/", workers)

    write_json_state(fingerprints_path, new_fingerprints)
