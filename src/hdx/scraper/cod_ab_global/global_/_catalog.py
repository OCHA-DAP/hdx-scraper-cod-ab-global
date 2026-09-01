"""Finalize the global/ portolan catalog: add, PMTiles, metadata, readme."""

import logging
from pathlib import Path
from subprocess import CalledProcessError

from hdx.scraper.cod_ab_global.catalog import _portolan
from hdx.scraper.cod_ab_global.config import PORTOLAN_WORKERS

logger = logging.getLogger(__name__)


def build_catalog(global_dir: Path) -> None:
    """Run portolan add at the global/ catalog root, generate PMTiles, finalize."""
    workers = str(PORTOLAN_WORKERS)
    try:
        _portolan(
            ["add", ".", "--workers", workers, "--pmtiles", "--force"],
            cwd=global_dir,
        )
    except CalledProcessError:
        logger.exception("portolan add failed for global/")
    try:
        _portolan(["stac-geoparquet"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet reported no items, skipping")
    try:
        _portolan(["check", "--metadata", "--fix"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan check --metadata --fix returned errors (continuing)")
    try:
        _portolan(["readme"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan readme failed (continuing)")
