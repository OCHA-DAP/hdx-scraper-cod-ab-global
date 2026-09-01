"""Pipeline orchestration: mirror, extend, match, stitch, push, HDX export."""

import logging
from pathlib import Path
from tempfile import mkdtemp

from ._push import push_all
from .catalog import _ensure_root_catalog, write_top_catalog
from .config import (
    HDX_EXPORT_OUTPUT_DIR,
    HDX_EXPORT_PUSH,
    PORTOLAN_WORK_DIR,
)
from .extended import run as extended_run
from .global_ import run as global_run
from .hdx_export import run as hdx_export_run
from .matched import run as matched_run
from .original import run as original_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TREE_NAMES = ("original", "extended", "matched", "global")


def main() -> None:
    """Run the full pipeline: mirror, extend, match, stitch, push, HDX export."""
    work_dir = (
        Path(PORTOLAN_WORK_DIR)
        if PORTOLAN_WORK_DIR
        else Path(mkdtemp(prefix="portolan-cod-ab-"))
    )
    trees = {name: work_dir / name for name in TREE_NAMES}
    original_dir = trees["original"]
    extended_dir = trees["extended"]
    matched_dir = trees["matched"]
    global_dir = trees["global"]

    for catalog_dir in trees.values():
        _ensure_root_catalog(catalog_dir)

    original_run(original_dir)
    extended_run(original_dir, extended_dir)
    matched_run(extended_dir, matched_dir, work_dir)
    global_run(matched_dir, global_dir)
    write_top_catalog(work_dir, list(TREE_NAMES))

    # Consolidated push after all stages complete, so users never see partial state.
    push_all(trees, work_dir)

    # Pushing to HDX needs HDX_EXPORT_PUSH, plus ~/.hdx_configuration.yaml's hdx_site.
    hdx_export_output_dir = (
        Path(HDX_EXPORT_OUTPUT_DIR)
        if HDX_EXPORT_OUTPUT_DIR
        else work_dir.parent / "hdx_export_build"
    )
    if HDX_EXPORT_PUSH:
        from hdx.api.configuration import Configuration  # noqa: PLC0415

        Configuration.create(
            user_agent_config_yaml=Path("~").expanduser() / ".useragents.yaml",
            user_agent_lookup="hdx-scraper-cod-global",
        )
    hdx_export_run(
        original_dir,
        extended_dir,
        matched_dir,
        work_dir,
        hdx_export_output_dir,
        push_to_hdx=HDX_EXPORT_PUSH,
    )
