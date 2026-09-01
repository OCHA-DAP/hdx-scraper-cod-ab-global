"""Project a country's admin parquet to the canonical global GDB schema."""

from pathlib import Path

import duckdb
from hdx.location.country import Country

_ADM_SUFFIXES = ("_name", "_name1", "_name2", "_name3", "_pcode")


def _admin_col_pairs(max_level: int) -> list[tuple[int, str]]:
    """Return (level, suffix) pairs for levels in [max_level, 0], descending."""
    return [
        (level, suffix)
        for level in range(max_level, -1, -1)
        for suffix in _ADM_SUFFIXES
    ]


def project_original(
    iso3: str, admin_level: int, parquet_path: Path, con: duckdb.DuckDBPyConnection
) -> str:
    """Return a SELECT fragment projecting original.parquet to canonical schema."""
    iso3_upper = iso3.upper()
    iso2 = Country.get_iso2_from_iso3(iso3_upper) or ""
    existing = {
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
    }

    admin_cols = [
        f"adm{level}{suffix}" for level, suffix in _admin_col_pairs(admin_level)
    ]
    parts = [
        col if col in existing else f"CAST(NULL AS VARCHAR) AS {col}"
        for col in [*admin_cols, "lang", "lang1", "lang2", "lang3"]
    ]
    parts.append(f"'{iso2}' AS iso2")
    parts.append(f"'{iso3_upper}' AS iso3")
    parts.append("version" if "version" in existing else "NULL AS version")
    parts.append("valid_on" if "valid_on" in existing else "NULL AS valid_on")
    parts.append(
        "CAST(valid_to AS DATE) AS valid_to"
        if "valid_to" in existing
        else "NULL AS valid_to"
    )
    parts.append("geometry")

    cols_str = ",\n        ".join(parts)
    return f"    SELECT\n        {cols_str}\n    FROM read_parquet('{parquet_path}')"


def project_filled(target_level: int, deepest_level: int, parquet_path: Path) -> str:
    """SELECT fragment for extended/matched, filled up to target_level.

    Above deepest_level, duplicates the deepest level's real columns upward.
    """
    parts = []
    for level, suffix in _admin_col_pairs(target_level):
        source_level = min(level, deepest_level)
        col = f"adm{level}{suffix}"
        source_col = f"adm{source_level}{suffix}"
        parts.append(col if source_col == col else f"{source_col} AS {col}")
    parts.extend(
        [
            "lang",
            "lang1",
            "lang2",
            "lang3",
            "iso2",
            "iso3",
            "version",
            "valid_on",
            "valid_to",
        ]
    )
    parts.append(f"{deepest_level} AS adm_origin")
    parts.append("geometry")
    cols_str = ",\n        ".join(parts)
    return f"    SELECT\n        {cols_str}\n    FROM read_parquet('{parquet_path}')"
