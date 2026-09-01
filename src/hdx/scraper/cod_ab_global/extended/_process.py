"""Run edge extension for one service: where-filter, extend, dissolve."""

import logging
import tempfile
from pathlib import Path
from shutil import copy

import duckdb
from topo_tools import edge_extend as extend

from hdx.scraper.cod_ab_global.config import where_filter as _where_filter

from ._dissolve import dissolve_all_levels

logger = logging.getLogger(__name__)


def _apply_where_filter(path: Path, iso3_upper: str) -> None:
    """Apply config.where_filter to a parquet in-place before edge extension."""
    raw = _where_filter.get(iso3_upper)
    if not raw:
        return
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        available = {r[0] for r in described}
        conditions = [
            c.strip()
            for c in raw.split(" and ")
            if c.strip().split()[0].lower() in available
        ]
        if not conditions:
            return
        where = " and ".join(conditions)
        tmp = path.with_suffix(".tmp.parquet")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{path}') WHERE {where})"
            f" TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        path.unlink()
        tmp.rename(path)
        logger.debug("Applied where filter for %s: %s", iso3_upper, where)
    finally:
        con.close()


def process_service(
    iso3: str,
    version: str,
    original_version_dir: Path,
    extended_version_dir: Path,
    admin_level_full: int,
) -> bool:
    """Run edge extension for one service in an isolated temp dir."""
    layer_name = f"{iso3}_admin{admin_level_full}"
    seed_src = original_version_dir / layer_name / f"{layer_name}.parquet"
    if not seed_src.exists():
        logger.warning("Seed parquet not found: %s", seed_src)
        return False

    with tempfile.TemporaryDirectory(prefix="portolan-extended-") as tmp:
        temp_path = Path(tmp)
        pre_path = temp_path / "pre.parquet"
        copy(seed_src, pre_path)
        _apply_where_filter(pre_path, iso3.upper())

        post_path = temp_path / "post.parquet"
        try:
            extend(pre_path, post_path, overwrite=True)
        except Exception:
            logger.exception("Edge extension failed for %s/%s", iso3, version)
            return False

        for level in range(admin_level_full + 2):
            stale_dir = extended_version_dir / f"{iso3}_admin{level}"
            if stale_dir.exists():
                (stale_dir / f"{iso3}_admin{level}.parquet").unlink(missing_ok=True)

        try:
            dissolve_all_levels(post_path, iso3, admin_level_full, extended_version_dir)
        except Exception:
            logger.exception("Postprocessing failed for %s/%s", iso3, version)
            return False

    logger.info("Extended %s/%s successfully", iso3, version)
    return True
