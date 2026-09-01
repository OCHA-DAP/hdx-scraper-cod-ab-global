"""Assemble per-stage global GDBs for HDX from original/, extended/, matched/.

Extended/matched fill a shallower country up to each target level; original
never fills, since every level it has is real per-level ArcGIS data.
"""

import logging
from pathlib import Path
from shutil import make_archive, rmtree
from tempfile import TemporaryDirectory

import duckdb

from ._assemble import _MAX_ADMIN, assemble_admin_level
from ._gdb import append_layer_to_gdb, max_original_level

logger = logging.getLogger(__name__)


def build_boundaries_gdb(
    version_dirs: list[tuple[str, Path]], run_version: str, stage: str, output_dir: Path
) -> Path:
    """Assemble one stage's global GDB for a run_version. Returns the zip path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    gdb_path = output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb"
    rmtree(gdb_path, ignore_errors=True)

    min_level = 1 if stage == "matched" else 0
    max_level = (
        max(_MAX_ADMIN, max_original_level(version_dirs))
        if stage == "original"
        else _MAX_ADMIN
    )
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        with TemporaryDirectory(prefix="hdx-export-boundaries-") as tmp:
            tmp_path = Path(tmp)
            for admin_level in range(min_level, max_level + 1):
                level_parquet = tmp_path / f"admin{admin_level}.parquet"
                wrote = assemble_admin_level(
                    stage, admin_level, version_dirs, con, level_parquet, min_level
                )
                if wrote:
                    append_layer_to_gdb(level_parquet, gdb_path, admin_level)
    finally:
        con.close()

    logger.info("Assembled %s", gdb_path)
    make_archive(str(gdb_path), "zip", gdb_path)
    rmtree(gdb_path)
    return gdb_path.with_suffix(".gdb.zip")
