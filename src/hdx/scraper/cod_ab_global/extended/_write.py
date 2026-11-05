"""Shared GeoParquet 2.0 writer used across original/extended/matched/global stages."""

import re
from pathlib import Path

import duckdb

_PCODE_COLUMN = re.compile(r"^adm(\d)_pcode$")


def _sort_key(con: duckdb.DuckDBPyConnection, src: str) -> str:
    """Return ORDER BY terms: every adm{N}_pcode top-down, then the deepest name."""
    described = con.execute(f"DESCRIBE FROM read_parquet('{src}')").fetchall()
    columns = {row[0] for row in described}
    levels = sorted(int(m.group(1)) for c in columns if (m := _PCODE_COLUMN.match(c)))
    terms = [f'"adm{n}_pcode" NULLS LAST' for n in levels]
    if levels and f"adm{levels[-1]}_name" in columns:
        terms.append(f'"adm{levels[-1]}_name" NULLS LAST')
    return "".join(f"{term}, " for term in terms)


def write_gpq2(src: Path, dest: Path) -> None:
    """Write src as GeoParquet 2.0, CRS labelled OGC:CRS84, rows sorted by P-code."""
    with duckdb.connect() as con:
        con.load_extension("spatial")
        con.execute("SET geometry_always_xy = true")
        sort_key = _sort_key(con, str(src))
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE (file_row_number)
                    REPLACE (ST_SetCRS(geometry, 'OGC:CRS84') AS geometry)
                FROM read_parquet('{src}', file_row_number = true)
                ORDER BY {sort_key}file_row_number
            ) TO '{dest}' (
                FORMAT PARQUET, GEOPARQUET_VERSION 'V2',
                COMPRESSION ZSTD, COMPRESSION_LEVEL 22
            )
        """)
