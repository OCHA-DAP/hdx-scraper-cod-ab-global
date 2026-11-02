"""Build one label point per unit as `admin{N}/admin{N}_points.pmtiles`."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from hdx.scraper.cod_ab_global.catalog import tile_parquet

logger = logging.getLogger(__name__)

_MAX_ADMIN = 4


def points_path(global_dir: Path, level: int) -> Path:
    """Return the label-points PMTiles path for one admin level."""
    return global_dir / f"admin{level}" / f"admin{level}_points.pmtiles"


def build_points(global_dir: Path) -> None:
    """Write maximum-inscribed-circle centers of each admin level to PMTiles."""
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        for level in range(1, _MAX_ADMIN + 1):
            src = global_dir / f"admin{level}" / f"admin{level}.parquet"
            with TemporaryDirectory() as tmp:
                tmp_points = Path(tmp) / f"admin{level}_points.parquet"
                con.execute(f"""
                    COPY (
                        SELECT * REPLACE (
                            ST_MaximumInscribedCircle(geometry, 1e-6).center
                            AS geometry
                        )
                        FROM read_parquet('{src}')
                    ) TO '{tmp_points}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
                tile_parquet(
                    tmp_points, points_path(global_dir, level), tmp_points.stem
                )
            logger.info("Written admin%d label points", level)
    finally:
        con.close()
