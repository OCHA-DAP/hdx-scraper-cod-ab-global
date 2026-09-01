"""Union per-country deepest matched layers, stitch seams, dissolve down."""

import logging
import tempfile
from pathlib import Path

import duckdb
from topo_tools import dissolve
from topo_tools import edge_stitch as topo_stitch

from hdx.scraper.cod_ab_global.config import ADMIN_SCHEMA_PATH
from hdx.scraper.cod_ab_global.extended import _write_gpq2

logger = logging.getLogger(__name__)


def _build_service_select(meta: dict) -> str:
    """Return a SELECT SQL fragment for one service's deepest matched parquet."""
    return f"SELECT * FROM read_parquet('{meta['parquet_path']}')"


def assemble_and_clean(
    services_meta: list[dict],
    con: duckdb.DuckDBPyConnection,
    adm4_path: Path,
) -> None:
    """UNION ALL BY NAME per-country deepest admin parquets, stitch seams, write."""
    selects = [_build_service_select(meta) for meta in services_meta]
    union_sql = "\nUNION ALL BY NAME\n".join(selects)

    with tempfile.TemporaryDirectory(prefix="portolan-global-") as tmp:
        tmp_path = Path(tmp)
        tmp_raw = tmp_path / "adm4_raw.parquet"
        tmp_stitched = tmp_path / "adm4_stitched.parquet"
        tmp_issues = tmp_path / "adm4_issues.parquet"

        con.execute(
            f"COPY (\n{union_sql}\n) TO '{tmp_raw}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp_raw}')").fetchone()[0]
        logger.info("Union assembled: %d features", n)

        topo_stitch(
            tmp_raw,
            tmp_stitched,
            tmp_issues,
            target_schema_path=ADMIN_SCHEMA_PATH,
            fill_schema=True,
            overwrite=True,
        )
        logger.info("Seams stitched")

        _write_gpq2(tmp_stitched, adm4_path)
    logger.info("Written adm4 (%s)", adm4_path)


def dissolve_level(
    con: duckdb.DuckDBPyConnection,
    adm4_path: Path,
    out_path: Path,
    level: int,
) -> None:
    """Dissolve adm4-equivalent parquet to a coarser level and write GeoParquet."""
    pcode_col = f"adm{level}_pcode"

    with tempfile.TemporaryDirectory(prefix="portolan-dissolve-") as tmp:
        tmp_path = Path(tmp)
        filtered_path = tmp_path / f"adm{level}_filtered.parquet"
        dissolved_path = tmp_path / f"adm{level}.parquet"
        issues_path = tmp_path / f"adm{level}_issues.parquet"

        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{adm4_path}')
                WHERE {pcode_col} IS NOT NULL
            ) TO '{filtered_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        dissolve(
            filtered_path,
            dissolved_path,
            issues_path,
            group_by=["iso3", pcode_col],
            target_schema_path=ADMIN_SCHEMA_PATH,
            overwrite=True,
        )
        if issues_path.exists():
            logger.warning(
                "Dissolve to adm%d reported topology issues: %s", level, issues_path
            )
        _write_gpq2(dissolved_path, out_path)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
    logger.info("Written adm%d: %d features (%s)", level, n, out_path)
