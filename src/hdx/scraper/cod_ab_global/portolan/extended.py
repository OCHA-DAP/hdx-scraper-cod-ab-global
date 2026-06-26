"""Mirror edge-extended COD-AB boundaries to source.coop.

Reads from the local unified catalog (no ArcGIS calls), runs the edge extension
pipeline for services whose original layers have changed, and injects extended
assets into each layer's collection.json.

The extended variant contains only polygon admin boundary layers (adm0-admN).
Non-polygon types (lines, points, capitals, regions) are excluded.
"""

import contextlib
import json
import logging
import re
import tempfile
from pathlib import Path
from shutil import copy

import duckdb
import geoparquet_io as gpio
from hdx.location.country import Country

from hdx.scraper.cod_ab_global.config import where_filter as _where_filter
from hdx.scraper.cod_ab_global.edge_extender import edge_extender

from .config import PORTOLAN_WORKERS
from .original import (
    _generate_variant_pmtiles,
    inject_variant_assets,
)

logger = logging.getLogger(__name__)

# Matches adm0, adm1, ..., adm9. Excludes lines, points, capitals, regions.
_ADMIN_POLYGON_RE = re.compile(r"^adm\d$")


def _get_admin_updated_map(version_dir: Path) -> dict[str, str]:
    """Return {layer_short: updated_iso} for adm* polygon layers in a version dir."""
    result = {}
    if not version_dir.exists():
        return result
    for layer_dir in sorted(version_dir.iterdir()):
        if not layer_dir.is_dir() or layer_dir.name.startswith("."):
            continue
        if not _ADMIN_POLYGON_RE.match(layer_dir.name):
            continue
        collection_path = layer_dir / "collection.json"
        if collection_path.exists():
            updated = json.loads(collection_path.read_text()).get("updated")
            if updated:
                result[layer_dir.name] = updated
    return result


def _load_stored_original_updated(version_dir: Path) -> dict[str, str]:
    """Return stored original updated map from the version catalog.json."""
    catalog_path = version_dir / "catalog.json"
    if not catalog_path.exists():
        return {}
    raw = json.loads(catalog_path.read_text()).get("cod_ab:original_updated")
    if not raw:
        return {}
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        return json.loads(raw)
    return {}


def _get_admin_level_full(version_dir: Path) -> int | None:
    """Return admin_level_full from catalog.json, verified against actual parquets.

    Falls back to the highest adm{N} dir with an existing original.parquet.
    """
    catalog_path = version_dir / "catalog.json"
    if catalog_path.exists():
        with contextlib.suppress(TypeError, ValueError):
            val = json.loads(catalog_path.read_text()).get("cod_ab:admin_level_full")
            if val is not None:
                level = int(val)
                seed_dir = version_dir / f"adm{level}"
                if (seed_dir / "original.parquet").exists():
                    return level
    levels = [
        int(d.name[3:])
        for d in version_dir.iterdir()
        if d.is_dir()
        and _ADMIN_POLYGON_RE.match(d.name)
        and (d / "original.parquet").exists()
    ]
    return max(levels) if levels else None


def _admin_group_cols(all_cols: list[str], level: int) -> list[str]:
    """Return columns to SELECT/GROUP BY when dissolving to admin level.

    Matches the canonical schema from standardize.py: adm*_name*, adm*_pcode,
    lang*, version, valid_on, valid_to. iso2/iso3 are injected as literals in
    _dissolve_all_levels since the portolan originals don't carry those columns.

    Returns empty list if no pcode column exists for this level — caller skips
    the level rather than dissolving by date fields only (wrong semantics).
    """
    keep = {
        f"adm{L}{s}"
        for L in range(level + 1)
        for s in ("_name", "_name1", "_name2", "_name3", "_pcode")
    } | {"lang", "lang1", "lang2", "lang3", "version", "valid_on", "valid_to"}
    cols = [c for c in all_cols if c in keep]
    pcodes = {f"adm{L}_pcode" for L in range(level + 1)}
    if not any(c in pcodes for c in cols):
        return []
    return cols


def _write_gpq2(src: Path, dest: Path) -> None:
    """Read a GeoParquet file, Hilbert-sort it, and write as GeoParquet 2.0."""
    gpio.read(str(src)).sort_hilbert().write(
        str(dest), compression_level=22, geoparquet_version="2.0"
    )


def _dissolve_all_levels(
    seed_path: Path,
    iso3: str,
    admin_level_full: int,
    version_dir: Path,
) -> None:
    """Write edge-extended parquet + dissolved lower levels into the version dir.

    Writes {version_dir}/adm{N}/extended.parquet for N from 0 to admin_level_full.
    Drops GDAL/ArcGIS artifacts and injects iso2/iso3 literals.
    """
    iso3_upper = iso3.upper()
    iso2 = Country.get_iso2_from_iso3(iso3_upper) or ""
    iso_suffix = f"'{iso2}' AS iso2, '{iso3_upper}' AS iso3"

    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        con.execute(f"CREATE TABLE seed AS SELECT * FROM read_parquet('{seed_path}')")
        all_cols = [row[0] for row in con.execute("DESCRIBE seed").fetchall()]

        with tempfile.TemporaryDirectory(prefix="portolan-dissolve-") as tmp:
            tmp_path = Path(tmp)
            for level in range(admin_level_full, -1, -1):
                group_cols = _admin_group_cols(all_cols, level)
                if not group_cols:
                    continue
                layer_short = f"adm{level}"
                out_dir = version_dir / layer_short
                out_dir.mkdir(parents=True, exist_ok=True)
                cols_str = ", ".join(group_cols)
                tmp_out = tmp_path / f"{layer_short}.parquet"
                if level == admin_level_full:
                    con.execute(
                        f"COPY (SELECT {cols_str}, {iso_suffix}, geometry FROM seed)"
                        f" TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                else:
                    con.execute(
                        f"COPY ("
                        f"  SELECT {cols_str}, {iso_suffix},"
                        f"  ST_Union_Agg(geometry) AS geometry"
                        f"  FROM seed GROUP BY {cols_str}"
                        f") TO '{tmp_out}'"
                        " (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                dest = out_dir / "extended.parquet"
                _write_gpq2(tmp_out, dest)
    finally:
        con.close()


def _apply_where_filter(path: Path, iso3_upper: str) -> None:
    """Apply config.where_filter to a parquet in-place before edge extension."""
    raw = _where_filter.get(iso3_upper)
    if not raw:
        return
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        available = {r[0] for r in described}
        conditions = [
            c.strip()
            for c in raw.split(" and ")
            if c.strip().split()[0].lower() in available
        ]
        if not conditions:
            return
        where = " and ".join(conditions)
        tmp = path.with_suffix(".tmp.parquet")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{path}') WHERE {where})"
            f" TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        path.unlink()
        tmp.rename(path)
        logger.debug("Applied where filter for %s: %s", iso3_upper, where)
    finally:
        con.close()


def _process_service(iso3: str, version: str, version_dir: Path) -> bool:
    """Run edge extension for one service in an isolated temp dir.

    Returns True on success. Writes extended.parquet into each adm{N} layer dir.
    Cleans up any stale extended parquets before writing new ones so shrinking
    admin_level_full doesn't leave orphan files.
    """
    admin_level_full = _get_admin_level_full(version_dir)
    if admin_level_full is None:
        logger.warning("Cannot determine admin_level_full for %s/%s", iso3, version)
        return False

    layer_short = f"adm{admin_level_full}"
    seed_src = version_dir / layer_short / "original.parquet"
    if not seed_src.exists():
        logger.warning("Seed parquet not found: %s", seed_src)
        return False

    # Use the old-style layer name the edge extender expects internally
    internal_layer = f"{iso3}_admin{admin_level_full}"

    with tempfile.TemporaryDirectory(prefix="portolan-extended-") as tmp:
        temp_path = Path(tmp)
        pre_dir = temp_path / "country" / "extended_pre"
        pre_dir.mkdir(parents=True, exist_ok=True)
        copy(seed_src, pre_dir / f"{internal_layer}.parquet")
        _apply_where_filter(pre_dir / f"{internal_layer}.parquet", iso3.upper())

        try:
            edge_extender(temp_path)
        except Exception:
            logger.exception("Edge extension failed for %s/%s", iso3, version)
            return False

        post_path = (
            temp_path / "country" / "extended_post" / f"{internal_layer}.parquet"
        )
        if not post_path.exists():
            logger.warning("Edge extender produced no output for %s/%s", iso3, version)
            return False

        # Remove stale extended parquets/pmtiles before writing new ones
        for level in range(admin_level_full + 2):
            stale_dir = version_dir / f"adm{level}"
            if stale_dir.exists():
                for stale in ("extended.parquet", "extended.pmtiles"):
                    (stale_dir / stale).unlink(missing_ok=True)

        try:
            _dissolve_all_levels(post_path, iso3, admin_level_full, version_dir)
        except Exception:
            logger.exception("Postprocessing failed for %s/%s", iso3, version)
            return False

    logger.info("Extended %s/%s successfully", iso3, version)
    return True


def _enrich_extended_catalog(version_dir: Path, original_map: dict[str, str]) -> None:
    """Write cod_ab:original_updated marker into the version catalog.json."""
    catalog_path = version_dir / "catalog.json"
    if catalog_path.exists() and original_map:
        data = json.loads(catalog_path.read_text())
        data["cod_ab:original_updated"] = json.dumps(original_map)
        catalog_path.write_text(json.dumps(data, indent=2))


def _inject_all_extended_assets(version_dir: Path, workers: str) -> None:
    """Inject extended assets into all adm{N} collection.json files.

    Called for every service on every run to ensure portolan add (which
    regenerates collection.json with only original assets) doesn't leave
    extended assets behind.
    """
    for layer_dir in sorted(version_dir.iterdir()):
        if not layer_dir.is_dir() or not _ADMIN_POLYGON_RE.match(layer_dir.name):
            continue
        parquet = layer_dir / "extended.parquet"
        if not parquet.exists():
            continue
        if not (layer_dir / "extended.pmtiles").exists():
            _generate_variant_pmtiles(parquet, layer_dir, workers)
        inject_variant_assets(layer_dir / "collection.json", "extended")


def _enumerate_services(work_dir: Path) -> list[tuple[str, str]]:
    """Return [(iso3, version), ...] for all service dirs in work_dir."""
    services = []
    for country_dir in sorted(work_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        if country_dir.name == "wld":
            continue
        for version_dir in sorted(country_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            v = version_dir.name
            if (v.startswith("v") and v[1:].isdigit()) or v == "latest":
                services.append((country_dir.name, v))
    return services


def run(work_dir: Path) -> None:
    """Mirror edge-extended COD-AB boundaries into the unified catalog."""
    services = _enumerate_services(work_dir)
    if not services:
        logger.warning("No services found in %s — run original first", work_dir)
        return
    logger.info("Found %d services to process for extended", len(services))

    workers = str(PORTOLAN_WORKERS)

    for iso3, version in services:
        version_dir = work_dir / iso3 / version
        original_map = _get_admin_updated_map(version_dir)
        if not original_map:
            continue

        stored = _load_stored_original_updated(version_dir)
        if original_map != stored:
            logger.info("Processing extended for %s/%s", iso3, version)
            # Synthesise the service name for _service_to_path roundtrip (not used here)
            if _process_service(iso3, version, version_dir):
                _enrich_extended_catalog(version_dir, original_map)
            else:
                logger.warning(
                    "Extended processing failed for %s/%s — will retry next run",
                    iso3,
                    version,
                )
        else:
            logger.debug("Skipping unchanged %s/%s", iso3, version)

        # portolan add regenerates collection.json — always re-inject extended assets
        _inject_all_extended_assets(version_dir, workers)
