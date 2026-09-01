"""Build per-admin-level parquets by UNION-ing every included country's SELECT."""

from pathlib import Path

import duckdb

from ._project import project_filled, project_original

_MAX_ADMIN = 4


def _deepest_level(
    version_dir: Path, iso3: str, min_level: int, max_level: int
) -> int | None:
    """Return the highest N in [min_level, max_level] with a native admin parquet."""
    levels = [
        n
        for n in range(min_level, max_level + 1)
        if (version_dir / f"{iso3}_admin{n}" / f"{iso3}_admin{n}.parquet").exists()
    ]
    return max(levels) if levels else None


def _original_selects(
    admin_level: int,
    version_dirs: list[tuple[str, Path]],
    con: duckdb.DuckDBPyConnection,
) -> list[str]:
    """Return one projected SELECT per country with a native admin_level layer."""
    selects = []
    for iso3, version_dir in version_dirs:
        layer_name = f"{iso3}_admin{admin_level}"
        parquet_path = version_dir / layer_name / f"{layer_name}.parquet"
        if parquet_path.exists():
            selects.append(project_original(iso3, admin_level, parquet_path, con))
    return selects


def _filled_selects(
    admin_level: int, version_dirs: list[tuple[str, Path]], min_level: int
) -> list[str]:
    """Return one projected, filled-up-to-admin_level SELECT per country."""
    selects = []
    for iso3, version_dir in version_dirs:
        deepest = _deepest_level(version_dir, iso3, min_level, _MAX_ADMIN)
        if deepest is None:
            continue
        source_level = min(admin_level, deepest)
        source_layer_name = f"{iso3}_admin{source_level}"
        parquet_path = version_dir / source_layer_name / f"{source_layer_name}.parquet"
        selects.append(project_filled(admin_level, deepest, parquet_path))
    return selects


def assemble_admin_level(  # noqa: PLR0913
    stage: str,
    admin_level: int,
    version_dirs: list[tuple[str, Path]],
    con: duckdb.DuckDBPyConnection,
    out_path: Path,
    min_level: int,
) -> bool:
    """UNION ALL every included country's parquet for one admin level.

    Returns True if any input existed (and out_path was written).
    """
    selects = (
        _original_selects(admin_level, version_dirs, con)
        if stage == "original"
        else _filled_selects(admin_level, version_dirs, min_level)
    )
    if not selects:
        return False

    union_sql = "\nUNION ALL\n".join(selects)
    con.execute(
        f"COPY (\n{union_sql}\n) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return True
