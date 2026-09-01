"""Push each catalog tree, plus the root catalog, to source.coop."""

import logging
from pathlib import Path
from subprocess import CalledProcessError

from .catalog import _portolan, _push_catalog_files, push_top_catalog
from .config import PORTOLAN_WORKERS, SOURCECOOP_REMOTE

logger = logging.getLogger(__name__)


def push_all(trees: dict[str, Path], work_dir: Path) -> None:
    """Push every tree in `trees` to its own source.coop subpath, then the root."""
    workers = str(PORTOLAN_WORKERS)
    for catalog_dir in trees.values():
        remote = f"{SOURCECOOP_REMOTE.rstrip('/')}/{catalog_dir.name}/"
        _portolan(["push", remote, "--workers", workers, "--verbose"], cwd=catalog_dir)
        _push_catalog_files(catalog_dir, remote)
        try:
            _portolan(["check", "--verbose"], cwd=catalog_dir)
        except CalledProcessError:
            logger.warning(
                "portolan check reported issues for %s (continuing)", catalog_dir.name
            )
    push_top_catalog(work_dir, SOURCECOOP_REMOTE)
