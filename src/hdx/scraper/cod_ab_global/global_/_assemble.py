"""Union per-country deepest matched layers, clean seams, dissolve down."""

import logging
import tempfile
from pathlib import Path

import duckdb
from topo_tools import package_polygons, schema_fill, topo_clean, topo_detect

from hdx.scraper.cod_ab_global.config import ADMIN_CODE_FIELD, ADMIN_NAME_FIELD
from hdx.scraper.cod_ab_global.extended import _write_gpq2

logger = logging.getLogger(__name__)

_SEAM_GAP_M = 0.001


def _build_service_select(meta: dict) -> str:
    """Return a SELECT SQL fragment for one service's deepest matched parquet."""
    return f"SELECT * FROM read_parquet('{meta['parquet_path']}')"


def _count(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchall()[0][0]


def assemble_and_clean(
    services_meta: list[dict],
    con: duckdb.DuckDBPyConnection,
    adm4_path: Path,
) -> None:
    """UNION ALL BY NAME per-country deepest admin parquets, clean seams, write."""
    selects = [_build_service_select(meta) for meta in services_meta]
    union_sql = "\nUNION ALL BY NAME\n".join(selects)

    with tempfile.TemporaryDirectory(prefix="portolan-global-") as tmp:
        tmp_path = Path(tmp)
        tmp_raw = tmp_path / "adm4_raw.parquet"
        tmp_detect = tmp_path / "adm4_detect.parquet"
        tmp_clean = tmp_path / "adm4_clean.parquet"
        tmp_filled = tmp_path / "adm4_filled.parquet"

        con.execute(
            f"COPY (\n{union_sql}\n) TO '{tmp_raw}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        logger.info("Union assembled: %d features", _count(con, tmp_raw))

        source = tmp_raw
        topo_detect(tmp_raw, tmp_detect, tmp_dir=tmp_path)
        # Wide gaps are real holes in the union (e.g. Botswana), not seam defects.
        defects = con.execute(
            f"SELECT count(*) FROM read_parquet('{tmp_detect}') "
            f"WHERE kind <> 'gap' OR max_width_m <= {_SEAM_GAP_M}"
        ).fetchall()[0][0]
        if defects:
            logger.warning("Union has %d coverage defects, cleaning", defects)
            topo_clean(
                tmp_raw, tmp_clean, tmp_path / "adm4_issues.parquet", tmp_dir=tmp_path
            )
            source = tmp_clean
            logger.info("Cleaned union: %d features", _count(con, tmp_clean))

        schema_fill(
            source,
            tmp_filled,
            name_field=ADMIN_NAME_FIELD,
            code_field=ADMIN_CODE_FIELD,
            tmp_dir=tmp_path,
        )
        _write_gpq2(tmp_filled, adm4_path)
    logger.info("Written adm4 (%s)", adm4_path)


def dissolve_levels(adm4_path: Path, global_dir: Path, levels: tuple[int, ...]) -> None:
    """Dissolve the adm4-equivalent parquet to each coarser level in one pass."""
    with tempfile.TemporaryDirectory(prefix="portolan-dissolve-") as tmp:
        tmp_path = Path(tmp)
        package_polygons(
            adm4_path,
            str(tmp_path / "admin{n}.parquet"),
            str(tmp_path / "admin{n}_issues.parquet"),
            name_field=ADMIN_NAME_FIELD,
            code_field=ADMIN_CODE_FIELD,
            min_level=min(levels),
            tmp_dir=tmp_path,
        )
        con = duckdb.connect()
        try:
            for level in levels:
                issues_path = tmp_path / f"admin{level}_issues.parquet"
                if issues_path.exists():
                    logger.warning(
                        "Dissolve to adm%d reported %d topology issues",
                        level,
                        _count(con, issues_path),
                    )
                out_dir = global_dir / f"admin{level}"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"admin{level}.parquet"
                _write_gpq2(tmp_path / f"admin{level}.parquet", out_path)
                logger.info(
                    "Written adm%d: %d features (%s)",
                    level,
                    _count(con, out_path),
                    out_path,
                )
        finally:
            con.close()
