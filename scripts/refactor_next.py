"""Refactor global/admin{1..4} to the draft COD-AB standard and write to next/."""

# ruff: noqa: INP001

import argparse
import logging
from pathlib import Path

import duckdb

from hdx.scraper.cod_ab_global._push import _push_tree
from hdx.scraper.cod_ab_global.catalog import _ensure_root_catalog
from hdx.scraper.cod_ab_global.config import (
    PORTOLAN_WORK_DIR,
    PORTOLAN_WORKERS,
    SOURCECOOP_REMOTE,
)
from hdx.scraper.cod_ab_global.global_._catalog import build_catalog

logger = logging.getLogger(__name__)

LEVELS = range(1, 5)
NAMES = ["name", "name1", "name2", "name3"]


def _old_keys(n: int, prefix: str = "") -> list[str]:
    return [f"{prefix}adm{i}_pcode" for i in range(1, n + 1)]


def build_pcodes(con: duckdb.DuckDBPyConnection, src4: Path) -> None:
    """Create tables p1..p4 mapping (iso3, old code chain) to new P-codes."""
    for n in LEVELS:
        keys = _old_keys(n)
        names = [f"adm{n}_{x}" for x in NAMES]
        con.execute(f"""
            CREATE OR REPLACE TABLE u{n} AS
            SELECT iso3, {", ".join(keys)},
                {", ".join(f"min({c}) AS {c}" for c in names)},
                count(DISTINCT ({", ".join(names)})) AS n_names
            FROM read_parquet('{src4}')
            WHERE adm{n}_pcode IS NOT NULL AND adm_lvl >= {n}
            GROUP BY ALL
        """)
        conflicts = con.execute(
            f"SELECT count(*) FROM u{n} WHERE n_names > 1"
        ).fetchone()[0]
        if conflicts:
            logger.warning("adm%d: %d units with differing names", n, conflicts)

        if n == 1:
            parent_join, parent_code, partition = "", "u.iso3", "u.iso3"
        else:
            on = " AND ".join(
                ["u.iso3 = p.iso3"] + [f"u.{k} = p.{k}" for k in _old_keys(n - 1)]
            )
            parent_join = f"JOIN p{n - 1} p ON {on}"
            parent_code = f"p.adm{n - 1}_new"
            partition = parent_code
        order = ", ".join(f"u.{c} NULLS LAST" for c in [f"adm{n}_pcode", *names])
        # lpad truncates past its width, so codes past 999 are left unpadded.
        con.execute(f"""
            CREATE OR REPLACE TABLE p{n} AS
            SELECT iso3, {", ".join(keys)},
                parent_new || '.' || CASE WHEN rn < 1000 THEN lpad(rn::VARCHAR, 3, '0')
                    ELSE rn::VARCHAR END AS adm{n}_new
            FROM (
                SELECT u.iso3, {", ".join(f"u.{k}" for k in keys)},
                    {parent_code} AS parent_new,
                    row_number() OVER (PARTITION BY {partition} ORDER BY {order}) AS rn
                FROM u{n} u {parent_join}
            )
        """)


def refactor_level(
    con: duckdb.DuckDBPyConnection, src: Path, dest: Path, level: int
) -> None:
    """Write one admin level in the new schema; geometry passes through unchanged."""
    cols = []
    joins = []
    for n in range(level, -1, -1):
        # Levels below the row's real depth repeat the deepest real level's code.
        codes = ["s.iso3", *(f"p{k}.adm{k}_new" for k in range(1, n + 1))]
        cases = " ".join(f"WHEN {k} THEN {code}" for k, code in enumerate(codes))
        cols += [f"CASE least(s.adm_lvl, {n}) {cases} END AS adm{n}_pcode"]
        cols += [f"s.adm{n}_{x}" for x in NAMES]
        cols += [f"NULL::VARCHAR AS adm{n}_type", f"s.adm{n}_pcode AS adm{n}_srcid"]
        if n:
            on = " AND ".join(
                [f"s.iso3 = p{n}.iso3"] + [f"s.{k} = p{n}.{k}" for k in _old_keys(n)]
            )
            joins.append(f"LEFT JOIN p{n} ON {on}")
    cols += [
        "1::INTEGER AS version",
        "NULL::DATE AS updated",
        "s.valid_on::DATE AS valid_from",
        "s.valid_to::DATE AS valid_to",
        "s.lang",
        "s.lang1",
        "s.lang2",
        "s.lang3",
        "ST_Area(ST_Transform(s.geometry, 'EPSG:4326', 'EPSG:6933', always_xy := true))"
        " / 1e6 AS area_sqkm",
        f"least(s.adm_lvl, {level})::INTEGER AS level",
        "s.valid_to IS NULL AS latest",
        "s.geometry",
    ]
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT {", ".join(cols)}
            FROM read_parquet('{src}') s
            {" ".join(joins)}
            ORDER BY adm{level}_pcode
        ) TO '{dest}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            GEOPARQUET_VERSION 'V2'
        )
    """)
    logger.info("Written %s", dest)


def main() -> None:
    """Refactor every admin level into the next/ catalog, optionally pushing it."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--push", action="store_true", help="push the next/ catalog to source.coop"
    )
    args = parser.parse_args()

    work_dir = Path(PORTOLAN_WORK_DIR or "portolan")
    src_dir, out_dir = work_dir / "global", work_dir / "next"
    con = duckdb.connect()
    con.execute("LOAD spatial")

    build_pcodes(con, src_dir / "admin4" / "admin4.parquet")
    for n in LEVELS:
        refactor_level(
            con,
            src_dir / f"admin{n}" / f"admin{n}.parquet",
            out_dir / f"admin{n}" / f"admin{n}.parquet",
            n,
        )

    _ensure_root_catalog(out_dir)
    build_catalog(out_dir)

    if args.push:
        remote = f"{SOURCECOOP_REMOTE.rstrip('/')}/next/"
        incomplete = _push_tree(out_dir, remote, str(PORTOLAN_WORKERS))
        if incomplete:
            msg = f"Collections still missing from remote: {incomplete}"
            raise RuntimeError(msg)
        logger.info("Pushed to %s", remote)


if __name__ == "__main__":
    main()
