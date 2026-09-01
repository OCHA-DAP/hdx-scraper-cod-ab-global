"""Assemble global COD-AB matched boundaries by admin level, into global/.

A fingerprint over all contributing matched parquets skips the rebuild when
nothing changed since the last successful run.
"""

import logging
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

import duckdb
from topo_tools import dissolve
from topo_tools import edge_stitch as topo_stitch

from .catalog import (
    _portolan,
    admin_layers,
    file_fingerprint,
    iter_version_dirs,
    read_json_state,
    write_json_state,
)
from .config import ADMIN_SCHEMA_PATH, PORTOLAN_WORKERS
from .extended import _write_gpq2

logger = logging.getLogger(__name__)

_MAX_ADMIN = 4


def _latest_versioned_per_iso3(matched_dir: Path) -> dict[str, Path]:
    """Return {iso3: version_dir} for the highest-versioned service per iso3."""
    best: dict[str, tuple[int, Path]] = {}
    for iso3, version, version_dir in iter_version_dirs(matched_dir):
        if version.startswith("v") and version[1:].isdigit():
            n = int(version[1:])
            if iso3 not in best or n > best[iso3][0]:
                best[iso3] = (n, version_dir)
    return {iso3: info[1] for iso3, info in best.items()}


def _get_service_meta(version_dir: Path, iso3: str) -> dict | None:
    """Return the deepest matched admin layer's metadata for one service."""
    layers = [(level, d) for level, d in admin_layers(version_dir, iso3) if level > 0]
    if not layers:
        logger.warning(
            "No usable matched parquet for %s/%s, skipping", iso3, version_dir.name
        )
        return None
    level, layer_dir = layers[-1]
    return {
        "service_name": f"{iso3}/{version_dir.name}",
        "admin_level_full": level,
        "iso3": iso3,
        "parquet_path": layer_dir / f"{iso3}_admin{level}.parquet",
    }


def _build_service_select(meta: dict) -> str:
    """Return a SELECT SQL fragment for one service's deepest matched parquet."""
    return f"SELECT * FROM read_parquet('{meta['parquet_path']}')"


def _assemble_and_clean(
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


def _dissolve_level(
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


def _combined_fingerprint(services_meta: list[dict]) -> dict[str, list[int]]:
    """Return {service_name: [size, mtime_ns]} across all contributing parquets."""
    result = {}
    for meta in services_meta:
        fp = file_fingerprint(meta["parquet_path"])
        if fp is not None:
            result[meta["service_name"]] = fp
    return result


def _parquets_exist(global_dir: Path) -> bool:
    """Return True if all four output parquets are present."""
    return all(
        (global_dir / f"admin{level}" / f"admin{level}.parquet").exists()
        for level in range(1, _MAX_ADMIN + 1)
    )


def _pmtiles_exist(global_dir: Path) -> bool:
    """Return True if all four PMTiles files are present."""
    return all(
        (global_dir / f"admin{level}" / f"admin{level}.pmtiles").exists()
        for level in range(1, _MAX_ADMIN + 1)
    )


def _build_parquets(services_meta: list[dict], global_dir: Path) -> None:
    """Assemble and write all four admin-level GeoParquet files."""
    admin4_dir = global_dir / "admin4"
    admin4_dir.mkdir(parents=True, exist_ok=True)
    admin4_path = admin4_dir / "admin4.parquet"
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        _assemble_and_clean(services_meta, con, admin4_path)
        for level in (3, 2, 1):
            out_dir = global_dir / f"admin{level}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"admin{level}.parquet"
            _dissolve_level(con, admin4_path, out_path, level)
    finally:
        con.close()


def _build_catalog(global_dir: Path) -> None:
    """Run portolan add at the global/ catalog root, generate PMTiles, finalize."""
    workers = str(PORTOLAN_WORKERS)
    try:
        _portolan(
            ["add", ".", "--workers", workers, "--pmtiles", "--force"],
            cwd=global_dir,
        )
    except CalledProcessError:
        logger.exception("portolan add failed for global/")
    try:
        _portolan(["stac-geoparquet"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet reported no items, skipping")
    try:
        _portolan(["check", "--metadata", "--fix"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan check --metadata --fix returned errors (continuing)")
    try:
        _portolan(["readme"], cwd=global_dir)
    except CalledProcessError:
        logger.warning("portolan readme failed (continuing)")


def run(matched_dir: Path, global_dir: Path) -> None:
    """Assemble global COD-AB matched boundaries into global/."""
    latest = _latest_versioned_per_iso3(matched_dir)
    services_meta = []
    for iso3, version_dir in sorted(latest.items()):
        meta = _get_service_meta(version_dir, iso3)
        if meta:
            services_meta.append(meta)
    logger.info(
        "Found %d latest-versioned services for global composite", len(services_meta)
    )

    if not services_meta:
        logger.warning("No matched services available, skipping global build")
        return

    fingerprints_path = global_dir / ".state" / "fingerprint.json"
    stored = read_json_state(fingerprints_path)
    current = _combined_fingerprint(services_meta)

    needs_rebuild = (
        current != stored
        or not _parquets_exist(global_dir)
        or not _pmtiles_exist(global_dir)
    )

    if needs_rebuild:
        logger.info("Building global adm4-equivalent layer...")
        _build_parquets(services_meta, global_dir)
        _build_catalog(global_dir)
        write_json_state(fingerprints_path, current)
    else:
        logger.info("Matched layers unchanged, skipping global rebuild")

    logger.info("Global dataset complete")
