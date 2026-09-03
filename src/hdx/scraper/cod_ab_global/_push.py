"""Push each catalog tree, plus the root catalog, to source.coop."""

import logging
from pathlib import Path
from subprocess import CalledProcessError

from .catalog import (
    _portolan,
    _push_catalog_files,
    find_incomplete_collections,
    normalize_asset_hrefs,
    push_top_catalog,
    repair_collections,
    sync_tree_deletions,
)
from .config import PORTOLAN_WORKERS, SOURCECOOP_REMOTE

logger = logging.getLogger(__name__)


def _push_tree(catalog_dir: Path, remote: str, workers: str) -> list[str]:
    """Push one tree and repair any collection missing from remote afterward."""
    normalize_asset_hrefs(catalog_dir)
    try:
        _portolan(["push", remote, "--workers", workers, "--verbose"], cwd=catalog_dir)
    except CalledProcessError:
        logger.warning(
            "portolan push failed for %s (verifying remote state)", catalog_dir.name
        )
    _push_catalog_files(catalog_dir, remote)
    sync_tree_deletions(catalog_dir, remote)

    incomplete = find_incomplete_collections(catalog_dir, remote)
    if incomplete:
        logger.warning(
            "%d collection(s) missing from remote for %s, repairing",
            len(incomplete),
            catalog_dir.name,
        )
        repair_collections(catalog_dir, remote, incomplete)
        incomplete = find_incomplete_collections(catalog_dir, remote)

    try:
        _portolan(["check", "--verbose"], cwd=catalog_dir)
    except CalledProcessError:
        logger.warning(
            "portolan check reported issues for %s (continuing)", catalog_dir.name
        )
    return incomplete


def push_all(trees: dict[str, Path], work_dir: Path) -> None:
    """Push every tree in `trees` to its own source.coop subpath, then the root."""
    workers = str(PORTOLAN_WORKERS)
    still_incomplete: dict[str, list[str]] = {}
    for catalog_dir in trees.values():
        remote = f"{SOURCECOOP_REMOTE.rstrip('/')}/{catalog_dir.name}/"
        incomplete = _push_tree(catalog_dir, remote, workers)
        if incomplete:
            still_incomplete[catalog_dir.name] = incomplete
    push_top_catalog(work_dir, SOURCECOOP_REMOTE)
    if still_incomplete:
        msg = f"Collections still missing from remote after repair: {still_incomplete}"
        raise RuntimeError(msg)
