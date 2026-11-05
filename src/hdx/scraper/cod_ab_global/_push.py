"""Push each catalog tree, plus the root catalog, to source.coop."""

import logging
from pathlib import Path
from subprocess import CalledProcessError

from .catalog import (
    _portolan,
    push_top_catalog,
    remove_nested_index_versions,
    sync_tree,
)
from .config import SOURCECOOP_REMOTE

logger = logging.getLogger(__name__)


def _push_tree(catalog_dir: Path, remote: str) -> None:
    """Drop nested index versions.json files, then mirror one tree to remote."""
    remove_nested_index_versions(catalog_dir)
    sync_tree(catalog_dir, remote)
    try:
        _portolan(["check", "--verbose"], cwd=catalog_dir)
    except CalledProcessError:
        logger.warning(
            "portolan check reported issues for %s (continuing)", catalog_dir.name
        )


def push_all(trees: dict[str, Path], work_dir: Path) -> None:
    """Push every tree in `trees` to its own source.coop subpath, then the root."""
    for catalog_dir in trees.values():
        remote = f"{SOURCECOOP_REMOTE.rstrip('/')}/{catalog_dir.name}/"
        _push_tree(catalog_dir, remote)
    push_top_catalog(work_dir, SOURCECOOP_REMOTE)
