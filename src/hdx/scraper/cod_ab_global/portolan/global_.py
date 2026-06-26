"""Assemble global COD-AB matched boundaries by admin level.

Reads the latest-versioned matched service per iso3 from the unified catalog,
builds a global admin4-equivalent layer (each country contributes its deepest
available level), applies ST_CoverageClean to fix inter-country boundary
slivers, then dissolves down to admin3/admin2/admin1 guaranteeing perfect
topological consistency.

Output is four GeoParquet files (adm1-adm4) written to wld/ and pushed to
source.coop.
"""

import contextlib
import json
import logging
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

import duckdb

from .config import PORTOLAN_WORKERS
from .extended import _write_gpq2
from .original import _portolan

logger = logging.getLogger(__name__)

_MAX_ADMIN = 4
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


def _latest_versioned_per_iso3(work_dir: Path) -> dict[str, Path]:
    """Return {iso3: version_dir} for the highest-versioned service per iso3.

    Unversioned services (latest/) are ignored — only v{N}/ directories are
    considered for the global composite.
    """
    best: dict[str, tuple[int, Path]] = {}
    for country_dir in work_dir.iterdir():
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        if country_dir.name == "wld":
            continue
        iso3 = country_dir.name
        for version_dir in country_dir.iterdir():
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            v = version_dir.name
            if v.startswith("v") and v[1:].isdigit():
                n = int(v[1:])
                if iso3 not in best or n > best[iso3][0]:
                    best[iso3] = (n, version_dir)
    return {iso3: info[1] for iso3, info in best.items()}


def _get_service_meta(version_dir: Path) -> dict | None:
    """Return admin level, parquet path, and change-detection key for one service.

    Only adm1+ matched parquets are candidates; adm0 is always excluded.
    """
    iso3_lower = version_dir.parent.name
    available = [
        int(d.name[3:])
        for d in version_dir.iterdir()
        if d.is_dir()
        and d.name.startswith("adm")
        and d.name[3:].isdigit()
        and d.name != "adm0"
        and (d / "matched.parquet").exists()
    ]
    if not available:
        logger.warning(
            "No usable matched parquet for %s/%s — skipping",
            iso3_lower,
            version_dir.name,
        )
        return None
    level = max(available)
    parquet_path = version_dir / f"adm{level}" / "matched.parquet"
    extended_updated: dict[str, str] = {}
    catalog_path = version_dir / "catalog.json"
    if catalog_path.exists():
        with contextlib.suppress(json.JSONDecodeError, TypeError, KeyError):
            raw = json.loads(catalog_path.read_text()).get("cod_ab:extended_updated")
            if raw:
                extended_updated = json.loads(raw)
    return {
        "service_name": f"{iso3_lower}/{version_dir.name}",
        "service_dir": version_dir,
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
    adm4_path: Path,
) -> None:
    """UNION ALL per-country deepest admin parquets, apply ST_CoverageClean, write."""
    selects = [_build_service_select(meta, con) for meta in services_meta]
    union_sql = "\nUNION ALL\n".join(selects)

    with tempfile.TemporaryDirectory(prefix="portolan-global-") as tmp:
        tmp_path = Path(tmp)
        tmp_raw = tmp_path / "adm4_raw.parquet"
        tmp_clean = tmp_path / "adm4_clean.parquet"

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

        _write_gpq2(tmp_clean, adm4_path)
    logger.info("Written adm4 (%s)", adm4_path)


def _dissolve_level(
    con: duckdb.DuckDBPyConnection,
    adm4_path: Path,
    out_path: Path,
    level: int,
) -> None:
    """Dissolve adm4-equivalent parquet to a coarser level and write GeoParquet."""
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
        tmp_out = Path(tmp) / f"adm{level}.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    {cols_str}
                FROM read_parquet('{adm4_path}')
                WHERE {group_key} IS NOT NULL
                GROUP BY iso3, {group_key}
            ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        _write_gpq2(tmp_out, out_path)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
    logger.info("Written adm%d: %d features (%s)", level, n, out_path)


def _collect_matched_state(services_meta: list[dict]) -> dict:
    """Return {service_name: extended_updated} fingerprint for change detection."""
    return {m["service_name"]: m["extended_updated"] for m in services_meta}


def _load_stored_state(wld_dir: Path) -> dict:
    """Read persisted state from .global_state.json."""
    state_path = wld_dir / _STATE_FILE
    if not state_path.exists():
        return {}
    with contextlib.suppress(json.JSONDecodeError, OSError):
        return json.loads(state_path.read_text())
    return {}


def _store_state(wld_dir: Path, state: dict) -> None:
    """Write state to .global_state.json (hidden, not pushed by portolan)."""
    (wld_dir / _STATE_FILE).write_text(json.dumps(state, indent=2))


def _parquets_exist(wld_dir: Path) -> bool:
    """Return True if all four output parquets are present."""
    return all(
        (wld_dir / f"adm{level}.parquet").exists() for level in range(1, _MAX_ADMIN + 1)
    )


def _build_parquets(services_meta: list[dict], wld_dir: Path) -> None:
    """Assemble and write all four admin-level GeoParquet files."""
    wld_dir.mkdir(parents=True, exist_ok=True)
    adm4_path = wld_dir / "adm4.parquet"
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        _assemble_and_clean(services_meta, con, adm4_path)
        for level in (3, 2, 1):
            out_path = wld_dir / f"adm{level}.parquet"
            _dissolve_level(con, adm4_path, out_path, level)
    finally:
        con.close()


def _fix_stale_wld_link(work_dir: Path) -> None:
    """Replace wld/catalog.json link with wld/collection.json in root catalog."""
    root = work_dir / "catalog.json"
    if not root.exists():
        return
    data = json.loads(root.read_text())
    changed = False
    for link in data.get("links", []):
        if link.get("href") == "./wld/catalog.json":
            link["href"] = "./wld/collection.json"
            changed = True
    if changed:
        root.write_text(json.dumps(data, indent=2))


def _build_catalog(_wld_dir: Path, work_dir: Path) -> None:
    """Run portolan add for the flat wld/ dir and finalize the catalog."""
    _fix_stale_wld_link(work_dir)
    workers = str(PORTOLAN_WORKERS)
    try:
        _portolan(
            ["add", "wld/", "--workers", workers],
            cwd=work_dir,
        )
    except CalledProcessError:
        logger.exception("portolan add failed for wld/")
    try:
        _portolan(["stac-geoparquet"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet: no items — skipping")
    try:
        _portolan(["check", "--metadata", "--fix"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan check --metadata --fix returned errors (continuing)")
    try:
        _portolan(["readme"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan readme failed (continuing)")


def run(work_dir: Path) -> None:
    """Assemble global COD-AB matched boundaries. Push handled by __main__.py."""
    wld_dir = work_dir / "wld"
    wld_dir.mkdir(parents=True, exist_ok=True)

    latest = _latest_versioned_per_iso3(work_dir)
    services_meta = []
    for _iso3, version_dir in sorted(latest.items()):
        meta = _get_service_meta(version_dir)
        if meta:
            services_meta.append(meta)
    logger.info(
        "Found %d latest-versioned services for global composite", len(services_meta)
    )

    if not services_meta:
        logger.warning("No matched services available — skipping global build")
        return

    current_state = _collect_matched_state(services_meta)
    stored = _load_stored_state(wld_dir)
    needs_rebuild = current_state != stored.get("matched_state") or not _parquets_exist(
        wld_dir
    )

    if needs_rebuild:
        logger.info("Building global adm4-equivalent layer...")
        _build_parquets(services_meta, wld_dir)
        _build_catalog(wld_dir, work_dir)
        _store_state(wld_dir, {"matched_state": current_state})
    else:
        logger.info("Matched layers unchanged — skipping global rebuild")

    logger.info("Global dataset complete")
