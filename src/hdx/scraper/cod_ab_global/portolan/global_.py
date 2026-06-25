"""Assemble global COD-AB matched boundaries by admin level.

Reads the latest-versioned matched service per iso3 from portolan/matched/, builds
a global admin4-equivalent layer (each country contributes its deepest available
level), applies ST_CoverageClean to fix inter-country boundary slivers, then
dissolves down to admin3/admin2/admin1 guaranteeing perfect topological consistency.

Output is four GeoParquet files (admin1-admin4) pushed to source.coop.
"""

import contextlib
import json
import logging
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

import duckdb

from .config import PORTOLAN_WORKERS, SOURCECOOP_REMOTE
from .extended import _write_gpq2
from .original import _portolan, _push_catalog_files

logger = logging.getLogger(__name__)

_MAX_ADMIN = 4
_SERVICE_PARTS = 4  # cod_ab_xxx_v## splits into exactly 4 parts
_ADM_SUFFIXES = ("_name", "_name1", "_name2", "_name3", "_pcode")
_COMMON_COLS = [
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
_SNAPPING = 1e-9
_STATE_FILE = ".global_state.json"


def _latest_versioned_per_iso3(matched_dir: Path) -> dict[str, Path]:
    """Return {iso3: service_dir} for the highest-versioned service per iso3.

    Unversioned services (cod_ab_afg) are ignored - they can be stale relative
    to versioned counterparts. Only cod_ab_xxx_v## directories are considered.
    """
    best: dict[str, tuple[int, Path]] = {}
    for d in matched_dir.iterdir():
        if not d.is_dir() or d.name.startswith("."):
            continue
        parts = d.name.split("_")
        if (
            len(parts) == _SERVICE_PARTS
            and parts[3].startswith("v")
            and parts[3][1:].isdigit()
        ):
            iso3 = parts[2]
            v = int(parts[3][1:])
            if iso3 not in best or v > best[iso3][0]:
                best[iso3] = (v, d)
    return {iso3: info[1] for iso3, info in best.items()}


def _get_service_meta(service_dir: Path) -> dict | None:
    """Return admin level, parquet path, and change-detection key for one service.

    iso3 and admin level are derived from the directory structure directly.
    Only cod_ab:extended_updated is read from catalog.json, for change detection.
    """
    iso3_lower = service_dir.name.split("_")[2]
    available = [
        int(d.name[-1])
        for d in service_dir.iterdir()
        if d.is_dir()
        and not d.name.endswith("0")
        and (d / f"{d.name}.parquet").exists()
    ]
    if not available:
        logger.warning("No usable parquet for %s - skipping", service_dir.name)
        return None
    level = max(available)
    layer = f"{iso3_lower}_admin{level}"
    parquet_path = service_dir / layer / f"{layer}.parquet"
    extended_updated: dict[str, str] = {}
    catalog_path = service_dir / "catalog.json"
    if catalog_path.exists():
        with contextlib.suppress(json.JSONDecodeError, TypeError, KeyError):
            raw = json.loads(catalog_path.read_text()).get("cod_ab:extended_updated")
            if raw:
                extended_updated = json.loads(raw)
    return {
        "service_name": service_dir.name,
        "service_dir": service_dir,
        "admin_level_full": level,
        "iso3": iso3_lower,
        "parquet_path": parquet_path,
        "extended_updated": extended_updated,
    }


def _build_service_select(meta: dict, con: duckdb.DuckDBPyConnection) -> str:
    """Return a SELECT SQL fragment coercing one service to the full admin4 schema."""
    level = meta["admin_level_full"]
    parquet_path = meta["parquet_path"]

    existing = {
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
    }

    parts = []
    for lvl in range(_MAX_ADMIN, -1, -1):
        parts.extend(
            col if (lvl <= level and col in existing) else f"NULL AS {col}"
            for suffix in _ADM_SUFFIXES
            for col in (f"adm{lvl}{suffix}",)
        )
    parts.extend(col if col in existing else f"NULL AS {col}" for col in _COMMON_COLS)
    parts.append(f"{level} AS adm_origin")
    parts.append("geometry")

    cols_str = ",\n        ".join(parts)
    return f"    SELECT\n        {cols_str}\n    FROM read_parquet('{parquet_path}')"


def _assemble_and_clean(
    services_meta: list[dict],
    con: duckdb.DuckDBPyConnection,
    admin4_path: Path,
) -> None:
    """UNION ALL per-country deepest admin parquets, apply ST_CoverageClean, write."""
    selects = [_build_service_select(meta, con) for meta in services_meta]
    union_sql = "\nUNION ALL\n".join(selects)

    with tempfile.TemporaryDirectory(prefix="portolan-global-") as tmp:
        tmp_path = Path(tmp)
        tmp_raw = tmp_path / "admin4_raw.parquet"
        tmp_clean = tmp_path / "admin4_clean.parquet"

        con.execute(
            f"COPY (\n{union_sql}\n) TO '{tmp_raw}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp_raw}')").fetchone()[0]
        logger.info("Union assembled: %d features", n)

        snapping = _SNAPPING
        con.execute(f"""
            COPY (
                WITH numbered AS (
                    SELECT row_number() OVER () AS rn, *
                    FROM read_parquet('{tmp_raw}')
                ),
                cleaned_coll AS (
                    SELECT ST_CoverageClean(list(geometry ORDER BY rn), {snapping}) AS c
                    FROM numbered
                ),
                dumped AS (
                    SELECT unnest(ST_Dump(c)) AS d FROM cleaned_coll
                ),
                cleaned AS (
                    SELECT d.path[1] AS idx, d.geom AS geometry FROM dumped
                )
                SELECT n.* EXCLUDE (rn, geometry), c.geometry
                FROM numbered n JOIN cleaned c ON n.rn = c.idx
                WHERE NOT ST_IsEmpty(c.geometry)
            ) TO '{tmp_clean}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        logger.info("ST_CoverageClean applied")

        admin4_path.parent.mkdir(parents=True, exist_ok=True)
        _write_gpq2(tmp_clean, admin4_path)
    logger.info("Written admin4 (%s)", admin4_path)


def _dissolve_level(
    con: duckdb.DuckDBPyConnection,
    admin4_path: Path,
    out_path: Path,
    level: int,
) -> None:
    """Dissolve admin4-equivalent parquet to a coarser level and write GeoParquet."""
    group_key = (
        "COALESCE(" + ", ".join(f"adm{lvl}_pcode" for lvl in range(level, 0, -1)) + ")"
    )

    select_parts = []
    for lvl in range(level, -1, -1):
        select_parts.extend(f"first(adm{lvl}{s}) AS adm{lvl}{s}" for s in _ADM_SUFFIXES)
    select_parts.extend(f"first({col}) AS {col}" for col in _COMMON_COLS)
    select_parts.append("max(adm_origin) AS adm_origin")
    select_parts.append("ST_MakeValid(ST_Union_Agg(geometry)) AS geometry")

    cols_str = ",\n        ".join(select_parts)

    with tempfile.TemporaryDirectory(prefix="portolan-dissolve-") as tmp:
        tmp_out = Path(tmp) / f"admin{level}.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    {cols_str}
                FROM read_parquet('{admin4_path}')
                WHERE {group_key} IS NOT NULL
                GROUP BY iso3, {group_key}
            ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _write_gpq2(tmp_out, out_path)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
    logger.info("Written admin%d: %d features (%s)", level, n, out_path)


def _collect_matched_state(services_meta: list[dict]) -> dict:
    """Return {service_name: extended_updated} fingerprint for change detection."""
    return {m["service_name"]: m["extended_updated"] for m in services_meta}


def _load_stored_state(global_dir: Path) -> dict:
    """Read persisted state from .global_state.json."""
    state_path = global_dir / _STATE_FILE
    if not state_path.exists():
        return {}
    with contextlib.suppress(json.JSONDecodeError, OSError):
        return json.loads(state_path.read_text())
    return {}


def _store_state(global_dir: Path, state: dict) -> None:
    """Write state to .global_state.json (hidden, not pushed by portolan)."""
    (global_dir / _STATE_FILE).write_text(json.dumps(state, indent=2))


def _parquets_exist(global_dir: Path) -> bool:
    """Return True if all four output parquets are present."""
    return all(
        (global_dir / f"admin{level}" / f"admin{level}.parquet").exists()
        for level in range(1, _MAX_ADMIN + 1)
    )


def _build_parquets(
    services_meta: list[dict],
    global_dir: Path,
) -> None:
    """Assemble and write all four admin-level GeoParquet files."""
    admin4_path = global_dir / "admin4" / "admin4.parquet"
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        _assemble_and_clean(services_meta, con, admin4_path)
        for level in (3, 2, 1):
            out_path = global_dir / f"admin{level}" / f"admin{level}.parquet"
            _dissolve_level(con, admin4_path, out_path, level)
    finally:
        con.close()


def _build_catalog(global_dir: Path, work_dir: Path) -> None:
    """Run portolan add for all four layers and finalize the catalog."""
    workers = str(PORTOLAN_WORKERS)
    for level in (_MAX_ADMIN, 3, 2, 1):
        if not (global_dir / f"admin{level}").exists():
            continue
        try:
            _portolan(
                ["add", f"global/admin{level}/", "--workers", workers, "--pmtiles"],
                cwd=work_dir,
            )
        except CalledProcessError:
            logger.exception("portolan add failed for admin%d", level)
    try:
        _portolan(["stac-geoparquet"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet: no items - skipping")
    _portolan(["check", "--metadata", "--fix"], cwd=work_dir)
    _portolan(["readme"], cwd=work_dir)


def run(work_dir: Path) -> None:
    """Assemble global COD-AB matched boundaries and push to source.coop."""
    matched_dir = work_dir / "matched"
    if not matched_dir.exists():
        logger.error("matched/ not found at %s - run matched first", matched_dir)
        return

    global_dir = work_dir / "global"
    global_dir.mkdir(parents=True, exist_ok=True)

    latest = _latest_versioned_per_iso3(matched_dir)
    services_meta = []
    for _iso3, svc_dir in sorted(latest.items()):
        meta = _get_service_meta(svc_dir)
        if meta:
            services_meta.append(meta)
    logger.info("Found %d latest-versioned services", len(services_meta))

    current_state = _collect_matched_state(services_meta)
    stored = _load_stored_state(global_dir)
    needs_rebuild = current_state != stored.get("matched_state") or not _parquets_exist(
        global_dir
    )

    if needs_rebuild:
        logger.info("Building global admin4-equivalent layer...")
        _build_parquets(services_meta, global_dir)
        _build_catalog(global_dir, work_dir)
    else:
        logger.info("Matched layers unchanged - skipping rebuild")

    workers = str(PORTOLAN_WORKERS)
    try:
        _portolan(
            ["push", SOURCECOOP_REMOTE, "--workers", workers, "--verbose"],
            cwd=work_dir,
        )
    except CalledProcessError:
        logger.exception("portolan push failed - global catalog not synced to S3")
        return

    _push_catalog_files(work_dir, SOURCECOOP_REMOTE)
    _portolan(["check", "--verbose"], cwd=work_dir)
    _store_state(global_dir, {"matched_state": current_state})
    logger.info("Global dataset complete")
