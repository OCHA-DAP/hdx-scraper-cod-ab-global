"""Assemble per-stage global GDBs for HDX from the portolan catalog.

Replaces process/boundaries.py. Reads directly from the persistent portolan
catalog instead of a throwaway download tree, and only includes services
resolved by `services.py` for the given run_version.

`original.parquet` is a raw ArcGIS extract and needs projecting to the old
pipeline's canonical schema (iso2/iso3 injected, fixed column set/order) —
done via DuckDB SQL, no GDAL CLI. `extended.parquet`/`matched.parquet` are
already canonical but need one more transform: per the published resource
notes ("lower levels are filled in with higher ones if they don't exist...
only layers with full coverage are used for these two resources"), a
country whose real depth stops at e.g. admin2 must still appear in the
admin3/admin4 layers, with its admin2 polygons duplicated upward and an
`adm_origin` column recording the true native depth — this exactly matches
the old process/extended_post.py::_adm_dissolve_up behavior. `original` has
no such filling (it only ever contains real per-level data) and has no
adm_origin column, matching the old pipeline's schema.

DuckDB's `COPY ... TO (FORMAT GDAL)` cannot append a new layer to an
already-existing .gdb file (verified experimentally: each COPY call either
creates the file fresh or raises "already exists") — so the final multi-layer
GDB write still uses the `gdal vector set-field-type --append` CLI loop, but
now once per admin level (at most 5 calls per stage) instead of once per
country (hundreds of calls in the old pipeline).
"""

import logging
from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run
from tempfile import TemporaryDirectory

import duckdb
from hdx.location.country import Country

from .services import iter_included_version_dirs

logger = logging.getLogger(__name__)

_MAX_ADMIN = 4
_ADM_SUFFIXES = ("_name", "_name1", "_name2", "_name3", "_pcode")

# The catalog.json field whose change triggers each stage's own upstream
# reprocessing (see portolan/extended.py and portolan/matched.py's own
# change-detection) — used by hdx_export/state.py's fingerprint check.
# Declared here, next to the stage-specific logic it mirrors, rather than
# in a generic cross-cutting lookup disconnected from that logic.
FINGERPRINT_KEYS = {
    "original": "cod_ab:original_updated",
    "extended": "cod_ab:original_updated",
    "matched": "cod_ab:extended_updated",
}


def _admin_col_pairs(max_level: int) -> list[tuple[int, str]]:
    """Return (level, suffix) pairs for levels in [max_level, 0], descending.

    Shared enumeration used by both `_project_original` (NULL-fallback per
    column) and `_project_filled` (fallback to a shallower real level).
    """
    return [
        (level, suffix)
        for level in range(max_level, -1, -1)
        for suffix in _ADM_SUFFIXES
    ]


def _project_original(
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


def _deepest_level(
    version_dir: Path, stage: str, min_level: int, max_level: int
) -> int | None:
    """Return the highest N in [min_level, max_level] with adm{N}/{stage}.parquet."""
    levels = [
        n
        for n in range(min_level, max_level + 1)
        if (version_dir / f"adm{n}" / f"{stage}.parquet").exists()
    ]
    return max(levels) if levels else None


def _project_filled(target_level: int, deepest_level: int, parquet_path: Path) -> str:
    """SELECT fragment for extended/matched, filled up to target_level.

    When target_level > deepest_level, duplicates the deepest level's real
    name/pcode columns upward (same geometry, no re-dissolve) — see the
    module docstring. Always stamps adm_origin = deepest_level.
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


def _assemble_admin_level(  # noqa: PLR0913
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
    selects = []
    for iso3, version_dir in version_dirs:
        if stage == "original":
            parquet_path = version_dir / f"adm{admin_level}" / "original.parquet"
            if not parquet_path.exists():
                continue
            selects.append(_project_original(iso3, admin_level, parquet_path, con))
            continue

        deepest = _deepest_level(version_dir, stage, min_level, _MAX_ADMIN)
        if deepest is None:
            continue
        source_level = min(admin_level, deepest)
        parquet_path = version_dir / f"adm{source_level}" / f"{stage}.parquet"
        selects.append(_project_filled(admin_level, deepest, parquet_path))

    if not selects:
        return False

    union_sql = "\nUNION ALL\n".join(selects)
    con.execute(
        f"COPY (\n{union_sql}\n) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return True


def _append_layer_to_gdb(parquet_path: Path, gdb_path: Path, admin_level: int) -> None:
    """Append one admin-level parquet as a layer into the shared GDB.

    Kept as the one narrow GDAL-CLI fallback in this module: DuckDB's
    `COPY ... TO (FORMAT GDAL)` cannot append a layer to an existing .gdb.
    """
    mode = ["--append"] if gdb_path.exists() else []
    run(
        [
            *["gdal", "vector", "set-field-type"],
            *[parquet_path, gdb_path],
            *mode,
            "--quiet",
            f"--output-layer=admin{admin_level}",
            *["--src-field-type=Date", "--dst-field-type=DateTime"],
        ],
        check=False,
        capture_output=True,
    )


def _max_original_level(
    version_dirs: list[tuple[str, Path]], upper_bound: int = 9
) -> int:
    """Return the deepest adm{N}/original.parquet actually present, up to upper_bound.

    Unlike extended/matched (which never exceed a country's own
    admin_level_full, itself never above `_MAX_ADMIN` today — verified
    against the current catalog), "original" mirrors whatever raw ArcGIS
    layers exist regardless of the "official" depth — e.g. MMR has real
    admin4/admin5 data despite an `admin_level_full` of 3, and production's
    own original GDB includes that admin5 layer.
    """
    max_seen = 0
    for _iso3, version_dir in version_dirs:
        for n in range(upper_bound, -1, -1):
            if (version_dir / f"adm{n}" / "original.parquet").exists():
                max_seen = max(max_seen, n)
                break
    return max_seen


def build_boundaries_gdb(
    work_dir: Path, run_version: str, stage: str, output_dir: Path
) -> Path:
    """Assemble one stage's global GDB for a run_version. Returns the zip path."""
    version_dirs = iter_included_version_dirs(work_dir, run_version)
    output_dir.mkdir(parents=True, exist_ok=True)
    gdb_path = output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb"
    rmtree(gdb_path, ignore_errors=True)

    min_level = 1 if stage == "matched" else 0
    max_level = (
        max(_MAX_ADMIN, _max_original_level(version_dirs))
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
                wrote = _assemble_admin_level(
                    stage, admin_level, version_dirs, con, level_parquet, min_level
                )
                if wrote:
                    _append_layer_to_gdb(level_parquet, gdb_path, admin_level)
    finally:
        con.close()

    logger.info("Assembled %s", gdb_path)
    make_archive(str(gdb_path), "zip", gdb_path)
    rmtree(gdb_path)
    return gdb_path.with_suffix(".gdb.zip")
