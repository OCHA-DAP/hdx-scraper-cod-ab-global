"""Mirror edge-extended COD-AB boundaries to source.coop.

Reads from the local portolan/original/ catalog (no ArcGIS calls), runs the
edge extension pipeline for services whose original layers have changed, and
pushes to source.coop.

The extended catalog contains only polygon admin boundary layers (admin0-adminN).
Non-polygon types (adminlines, adminpoints, capitals, regions) are excluded.
"""

import contextlib
import json
import logging
import re
import tempfile
from pathlib import Path
from shutil import copy, rmtree
from subprocess import CalledProcessError

import duckdb
import geoparquet_io as gpio
from hdx.location.country import Country

from hdx.scraper.cod_ab_global.config import where_filter as _where_filter
from hdx.scraper.cod_ab_global.edge_extender import edge_extender

from .config import PORTOLAN_WORKERS, SOURCECOOP_REMOTE
from .original import (
    _enrich_service_catalog,
    _portolan,
    _push_catalog_files,
    _remove_stale_services,
    _write_service_metadata,
)

logger = logging.getLogger(__name__)

# Matches afg_admin0, afg_admin1, etc. Excludes adminlines, adminpoints,
# admincapitals, regions, and _em variants.
_ADMIN_POLYGON_RE = re.compile(r"^[a-z]{3}_admin\d$")


def _get_admin_updated_map(service_dir: Path) -> dict[str, str]:
    """Return {layer_name: updated_iso} for admin polygon layers in a service dir."""
    result = {}
    if not service_dir.exists():
        return result
    for layer_dir in sorted(service_dir.iterdir()):
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


def _load_stored_original_updated(service_dir: Path) -> dict[str, str]:
    """Return stored original updated map from extended catalog.json."""
    catalog_path = service_dir / "catalog.json"
    if not catalog_path.exists():
        return {}
    raw = json.loads(catalog_path.read_text()).get("cod_ab:original_updated")
    if not raw:
        return {}
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        return json.loads(raw)
    return {}


def _get_admin_level_full(service_name: str, original_dir: Path) -> int | None:
    """Return admin_level_full for the service, verified against actual parquets.

    Reads cod_ab:admin_level_full from catalog.json as the authoritative source.
    Falls back to the highest admin level with an existing parquet only when the
    catalog value is absent or its parquet is missing (e.g. stale unversioned
    metadata, or a layer that was never downloaded).
    """
    service_dir = original_dir / service_name
    catalog_path = service_dir / "catalog.json"
    if catalog_path.exists():
        with contextlib.suppress(TypeError, ValueError):
            val = json.loads(catalog_path.read_text()).get("cod_ab:admin_level_full")
            if val is not None:
                level = int(val)
                seed = service_dir / f"{service_name.split('_')[2]}_admin{level}"
                if (seed / f"{seed.name}.parquet").exists():
                    return level
    # Fallback: highest admin level whose parquet actually exists
    levels = [
        int(d.name[-1])
        for d in service_dir.iterdir()
        if d.is_dir()
        and _ADMIN_POLYGON_RE.match(d.name)
        and (d / f"{d.name}.parquet").exists()
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
    service_name: str,
    admin_level_full: int,
    service_out: Path,
) -> None:
    """Write edge-extended parquet + dissolved lower levels to portolan structure.

    All levels (including the highest) are written via DuckDB column selection so
    that GDAL/ArcGIS artifacts (fid, objectid, area_sqkm, SHAPE__Area, center_lat,
    center_lon, etc.) are dropped and stale derived fields don't pollute the output.
    """
    iso3 = service_name.split("_")[2]
    iso3_upper = iso3.upper()
    iso2 = Country.get_iso2_from_iso3(iso3_upper) or ""
    # Literal iso2/iso3 suffix added to every SELECT — not in source data
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
                layer_name = f"{iso3}_admin{level}"
                out_dir = service_out / layer_name
                out_dir.mkdir(parents=True, exist_ok=True)
                cols_str = ", ".join(group_cols)
                tmp_out = tmp_path / f"{layer_name}.parquet"
                if level == admin_level_full:
                    # Highest level: select columns only, geometry already extended
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
                _write_gpq2(tmp_out, out_dir / f"{layer_name}.parquet")
    finally:
        con.close()


def _apply_where_filter(path: Path, iso3_upper: str) -> None:
    """Apply config.where_filter to a parquet in-place before edge extension.

    Conditions referencing fields not present in the parquet (e.g. adm2_pcode on an
    admin1 layer) are silently dropped so the call is safe for any admin level.
    """
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


def _process_service(service_name: str, original_dir: Path, extended_dir: Path) -> bool:
    """Run edge extension for one service in an isolated temp dir.

    Returns True on success. Deletes the existing extended service dir first
    so stale layers are removed when admin_level_full shrinks between updates.
    """
    admin_level_full = _get_admin_level_full(service_name, original_dir)
    if admin_level_full is None:
        logger.warning("Cannot determine admin_level_full for %s", service_name)
        return False

    iso3 = service_name.split("_")[2]
    layer_name = f"{iso3}_admin{admin_level_full}"
    seed_src = original_dir / service_name / layer_name / f"{layer_name}.parquet"
    if not seed_src.exists():
        logger.warning("Seed parquet not found: %s", seed_src)
        return False

    with tempfile.TemporaryDirectory(prefix="portolan-extended-") as tmp:
        temp_path = Path(tmp)
        pre_dir = temp_path / "country" / "extended_pre"
        pre_dir.mkdir(parents=True, exist_ok=True)
        copy(seed_src, pre_dir / f"{layer_name}.parquet")
        _apply_where_filter(pre_dir / f"{layer_name}.parquet", iso3.upper())

        try:
            edge_extender(temp_path)
        except Exception:
            logger.exception("Edge extension failed for %s", service_name)
            return False

        post_path = temp_path / "country" / "extended_post" / f"{layer_name}.parquet"
        if not post_path.exists():
            logger.warning("Edge extender produced no output for %s", service_name)
            return False

        dest = extended_dir / service_name
        dest_tmp = extended_dir / f"{service_name}.tmp"
        if dest_tmp.exists():
            rmtree(dest_tmp)
        try:
            _dissolve_all_levels(post_path, service_name, admin_level_full, dest_tmp)
        except Exception:
            logger.exception("Postprocessing failed for %s", service_name)
            rmtree(dest_tmp, ignore_errors=True)
            return False
        # Swap only after fully written — preserves existing data on failure
        if dest.exists():
            rmtree(dest)
        dest_tmp.rename(dest)

    logger.info("Extended %s successfully", service_name)
    return True


def _enrich_extended_catalog(
    service_dir: Path,
    meta: dict,
    original_map: dict[str, str],
) -> None:
    """Enrich catalog.json with cod_ab:* fields and original_updated marker."""
    if meta:
        _enrich_service_catalog(service_dir, meta)
    catalog_path = service_dir / "catalog.json"
    if catalog_path.exists() and original_map:
        data = json.loads(catalog_path.read_text())
        data["cod_ab:original_updated"] = json.dumps(original_map)
        catalog_path.write_text(json.dumps(data, indent=2))


def _enrich_extended_layer_collections(
    service_dir: Path, original_map: dict[str, str]
) -> None:
    """Write updated=max(original) into each extended layer's collection.json."""
    if not original_map:
        return
    max_updated = max(original_map.values())
    for layer_dir in sorted(service_dir.iterdir()):
        if not layer_dir.is_dir() or not _ADMIN_POLYGON_RE.match(layer_dir.name):
            continue
        collection_path = layer_dir / "collection.json"
        if not collection_path.exists():
            continue
        data = json.loads(collection_path.read_text())
        data["updated"] = max_updated
        collection_path.write_text(json.dumps(data, indent=2))


def _run_extensions(
    services: list[str],
    original_dir: Path,
    extended_dir: Path,
    stored_maps: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Run edge extension for services whose original layers have changed.

    Returns {service_name: original_map} for successfully processed services.
    """
    processed: dict[str, dict[str, str]] = {}
    for service_name in services:
        original_map = _get_admin_updated_map(original_dir / service_name)
        if not original_map:
            continue
        if original_map == stored_maps.get(service_name):
            logger.debug("Skipping unchanged service %s", service_name)
            continue
        logger.info("Processing extended for %s", service_name)
        if _process_service(service_name, original_dir, extended_dir):
            processed[service_name] = original_map
        else:
            logger.warning(
                "Extended processing failed for %s — will retry next run",
                service_name,
            )
    return processed


def _portolan_add_services(  # noqa: PLR0913
    services: list[str],
    extended_dir: Path,
    work_dir: Path,
    service_meta: dict[str, dict],
    original_maps: dict[str, dict[str, str]],
    workers: str,
) -> None:
    """Run portolan add for all extended services and re-apply enrichments.

    original_maps is the merged view: {service_name: original_updated_map}, with
    processed (newly extended) maps taking priority over stored maps.
    """
    for service_name in services:
        if not (extended_dir / service_name).exists():
            continue
        meta = service_meta.get(service_name, {})
        date_valid_on = (meta.get("date_valid_on") or "").strip()
        args = [
            "add",
            f"extended/{service_name}/",
            "--workers",
            workers,
            "--pmtiles",
        ]
        if date_valid_on:
            args += ["--datetime", date_valid_on]
        _write_service_metadata(extended_dir / service_name, service_name, meta or None)
        try:
            _portolan(args, cwd=work_dir)
        except CalledProcessError:
            logger.exception("portolan add failed for %s — skipping", service_name)
            continue
        original_map = original_maps.get(service_name, {})
        service_dir = extended_dir / service_name
        _enrich_extended_catalog(service_dir, meta, original_map)
        _enrich_extended_layer_collections(service_dir, original_map)


def run(work_dir: Path) -> None:
    """Mirror edge-extended COD-AB boundaries to source.coop."""
    original_dir = work_dir / "original"
    extended_dir = work_dir / "extended"
    extended_dir.mkdir(parents=True, exist_ok=True)

    if not original_dir.exists():
        logger.error("original/ not found at %s — run mirror first", original_dir)
        return

    services = sorted(
        d.name
        for d in original_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )
    logger.info("Found %d services in original catalog", len(services))

    service_meta: dict[str, dict] = {}
    for service_name in services:
        catalog_path = original_dir / service_name / "catalog.json"
        if catalog_path.exists():
            data = json.loads(catalog_path.read_text())
            service_meta[service_name] = {
                k[len("cod_ab:") :]: v
                for k, v in data.items()
                if k.startswith("cod_ab:")
            }

    # Read stored original_maps before portolan add regenerates catalog.json
    stored_maps: dict[str, dict[str, str]] = {
        svc: _load_stored_original_updated(extended_dir / svc)
        for svc in services
        if (extended_dir / svc).exists()
    }

    processed_maps = _run_extensions(services, original_dir, extended_dir, stored_maps)

    # Merge: stored maps as base, newly processed maps override
    all_original_maps: dict[str, dict[str, str]] = {**stored_maps, **processed_maps}

    workers = str(PORTOLAN_WORKERS)
    if extended_dir.exists() and any(
        d.is_dir() and not d.name.startswith(".") for d in extended_dir.iterdir()
    ):
        _remove_stale_services(services, extended_dir, "extended", work_dir)
    _portolan_add_services(
        services, extended_dir, work_dir, service_meta, all_original_maps, workers
    )

    try:
        _portolan(["stac-geoparquet"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet: no items — skipping")
    try:
        _portolan(
            ["push", SOURCECOOP_REMOTE, "--workers", workers, "--verbose"],
            cwd=work_dir,
        )
    except CalledProcessError:
        logger.exception("portolan push failed — extended catalog not synced to S3")
        return
    _push_catalog_files(work_dir, SOURCECOOP_REMOTE)
    _portolan(["check", "--verbose"], cwd=work_dir)
