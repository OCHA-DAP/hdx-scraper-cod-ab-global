"""Assemble global COD-AB matched boundaries by admin level, into global/.

A fingerprint over all contributing matched parquets skips the rebuild when
nothing changed since the last successful run.
"""

import logging
from pathlib import Path

import duckdb

from hdx.scraper.cod_ab_global.catalog import read_json_state, write_json_state

from ._assemble import assemble_and_clean, dissolve_level
from ._catalog import build_catalog
from ._scan import get_service_meta, latest_versioned_per_iso3
from ._state import combined_fingerprint, parquets_exist, pmtiles_exist

logger = logging.getLogger(__name__)


def _build_parquets(services_meta: list[dict], global_dir: Path) -> None:
    """Assemble and write all four admin-level GeoParquet files."""
    admin4_dir = global_dir / "admin4"
    admin4_dir.mkdir(parents=True, exist_ok=True)
    admin4_path = admin4_dir / "admin4.parquet"
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        assemble_and_clean(services_meta, con, admin4_path)
        for level in (3, 2, 1):
            out_dir = global_dir / f"admin{level}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"admin{level}.parquet"
            dissolve_level(con, admin4_path, out_path, level)
    finally:
        con.close()


def run(matched_dir: Path, global_dir: Path) -> None:
    """Assemble global COD-AB matched boundaries into global/."""
    latest = latest_versioned_per_iso3(matched_dir)
    services_meta = []
    for iso3, version_dir in sorted(latest.items()):
        meta = get_service_meta(version_dir, iso3)
        if meta:
            services_meta.append(meta)
    logger.info(
        "Found %d latest-versioned services for global composite", len(services_meta)
    )

    if not services_meta:
        logger.warning("No matched services available, skipping global build")
        return

    fingerprints_path = global_dir / ".state" / "fingerprint.json"
    stored = read_json_state(fingerprints_path)
    current = combined_fingerprint(services_meta)

    needs_rebuild = (
        current != stored
        or not parquets_exist(global_dir)
        or not pmtiles_exist(global_dir)
    )

    if needs_rebuild:
        logger.info("Building global adm4-equivalent layer...")
        _build_parquets(services_meta, global_dir)
        build_catalog(global_dir)
        write_json_state(fingerprints_path, current)
    else:
        logger.info("Matched layers unchanged, skipping global rebuild")

    logger.info("Global dataset complete")
