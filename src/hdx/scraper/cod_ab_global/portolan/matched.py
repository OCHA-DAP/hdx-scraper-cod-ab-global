"""Mirror edge-matched COD-AB boundaries to source.coop.

Reads from the local portolan/extended/ catalog (no ArcGIS calls except for the
one-time BNDA download), clips each admin layer to UN 1:1M international boundaries,
and pushes to source.coop.

Admin0 is excluded — clipping a country boundary to its own reference is redundant.
Change detection uses the extended layers' updated timestamps; a service is re-clipped
only when its extended catalog has changed.
"""

import contextlib
import json
import logging
import tempfile
from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError

import duckdb
import geoparquet_io as gpio

from .config import ARCGIS_SERVICES_URL, PORTOLAN_WORKERS, SOURCECOOP_REMOTE
from .extended import _ADMIN_POLYGON_RE, _get_admin_updated_map, _write_gpq2
from .original import (
    _enrich_service_catalog,
    _portolan,
    _push_catalog_files,
    _remove_stale_services,
    _write_service_metadata,
)
from .utils import generate_token

logger = logging.getLogger(__name__)

_BNDA_URL = f"{ARCGIS_SERVICES_URL}/Global_AB_1M_fs_gray/FeatureServer/5"


def _load_stored_extended_updated(service_dir: Path) -> dict[str, str]:
    """Return stored extended updated map from matched catalog.json."""
    catalog_path = service_dir / "catalog.json"
    if not catalog_path.exists():
        return {}
    raw = json.loads(catalog_path.read_text()).get("cod_ab:extended_updated")
    if not raw:
        return {}
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        return json.loads(raw)
    return {}


def _ensure_bnda(work_dir: Path) -> Path:
    """Return path to bnda_cty.parquet, downloading if absent.

    Stored at work_dir.parent/.bnda/ — outside the portolan catalog tree — so
    it can never be swept up by portolan push or aws s3 sync.
    """
    bnda_dir = work_dir.parent / ".bnda"
    bnda_dir.mkdir(exist_ok=True)
    bnda_path = bnda_dir / "bnda_cty.parquet"
    if bnda_path.exists():
        return bnda_path
    logger.info("Downloading UN BNDA boundaries from %s", _BNDA_URL)
    token = generate_token()
    table = gpio.extract_arcgis(_BNDA_URL, token=token)
    table.sort_hilbert().write(
        str(bnda_path), compression_level=22, geoparquet_version="2.0"
    )
    logger.info("Saved BNDA to %s", bnda_path)
    return bnda_path


def _clip_to_bnda(input_path: Path, output_path: Path, bnda_path: Path) -> None:
    """Clip one extended admin layer to the UN international boundary via DuckDB."""
    iso3 = input_path.stem.split("_")[0].upper()
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        all_cols = [
            r[0]
            for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{input_path}')"
            ).fetchall()
            if r[0] != "geometry"
        ]
        cols_str = ", ".join(all_cols)
        with tempfile.TemporaryDirectory(prefix="portolan-matched-clip-") as tmp:
            tmp_out = Path(tmp) / input_path.name
            con.execute(
                f"""
                COPY (
                    WITH clip AS (
                        SELECT ST_Union_Agg(geometry) AS geom
                        FROM read_parquet('{bnda_path}')
                        WHERE iso3cd = '{iso3}'
                    ),
                    clipped AS (
                        SELECT {cols_str},
                               ST_MakeValid(ST_Intersection(i.geometry, c.geom))
                                   AS geometry
                        FROM read_parquet('{input_path}') i
                        CROSS JOIN clip c
                        WHERE ST_GeometryType(i.geometry)
                                  IN ('POLYGON', 'MULTIPOLYGON')
                          AND ST_Intersects(i.geometry, c.geom)
                    ),
                    unioned AS (
                        SELECT {cols_str}, ST_Union_Agg(geometry) AS geometry
                        FROM clipped
                        WHERE ST_GeometryType(geometry)
                                  IN ('POLYGON', 'MULTIPOLYGON')
                        GROUP BY {cols_str}
                    )
                    SELECT * FROM unioned
                    WHERE NOT ST_IsEmpty(geometry)
                ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            _write_gpq2(tmp_out, output_path)
    finally:
        con.close()


def _process_service(
    service_name: str,
    extended_dir: Path,
    matched_dir: Path,
    bnda_path: Path,
) -> bool:
    """Clip all admin1+ layers for one service to UN boundaries.

    Returns True on success. Deletes the existing matched service dir first so
    stale layers are removed when admin_level_full shrinks between updates.
    """
    service_extended = extended_dir / service_name
    dest = matched_dir / service_name
    dest_tmp = matched_dir / f"{service_name}.tmp"

    layers = sorted(
        d
        for d in service_extended.iterdir()
        if d.is_dir()
        and _ADMIN_POLYGON_RE.match(d.name)
        and not d.name.endswith("0")  # skip admin0
    )
    if not layers:
        logger.warning("No admin1+ layers found for %s — skipping", service_name)
        return False

    if dest_tmp.exists():
        rmtree(dest_tmp)

    try:
        for layer_dir in layers:
            input_path = layer_dir / f"{layer_dir.name}.parquet"
            if not input_path.exists():
                logger.warning(
                    "Missing parquet for %s — skipping layer", layer_dir.name
                )
                continue
            out_dir = dest_tmp / layer_dir.name
            out_dir.mkdir(parents=True, exist_ok=True)
            _clip_to_bnda(input_path, out_dir / f"{layer_dir.name}.parquet", bnda_path)
    except Exception:
        logger.exception("Matched clipping failed for %s", service_name)
        rmtree(dest_tmp, ignore_errors=True)
        return False

    # Atomic swap — preserves existing data on failure
    if dest.exists():
        rmtree(dest)
    dest_tmp.rename(dest)

    logger.info("Matched %s successfully", service_name)
    return True


def _enrich_matched_catalog(
    service_dir: Path,
    meta: dict,
    extended_map: dict[str, str],
) -> None:
    """Enrich catalog.json with cod_ab:* fields and extended_updated marker."""
    if meta:
        _enrich_service_catalog(service_dir, meta)
    catalog_path = service_dir / "catalog.json"
    if catalog_path.exists() and extended_map:
        data = json.loads(catalog_path.read_text())
        data["cod_ab:extended_updated"] = json.dumps(extended_map)
        catalog_path.write_text(json.dumps(data, indent=2))


def _enrich_matched_layer_collections(
    service_dir: Path, extended_map: dict[str, str]
) -> None:
    """Write updated=max(extended) into each matched layer's collection.json."""
    if not extended_map:
        return
    max_updated = max(extended_map.values())
    for layer_dir in sorted(service_dir.iterdir()):
        if not layer_dir.is_dir() or not _ADMIN_POLYGON_RE.match(layer_dir.name):
            continue
        collection_path = layer_dir / "collection.json"
        if not collection_path.exists():
            continue
        data = json.loads(collection_path.read_text())
        data["updated"] = max_updated
        collection_path.write_text(json.dumps(data, indent=2))


def _run_clipping(
    services: list[str],
    extended_dir: Path,
    matched_dir: Path,
    stored_maps: dict[str, dict[str, str]],
    bnda_path: Path,
) -> dict[str, dict[str, str]]:
    """Clip services whose extended layers have changed since last matched run.

    Returns {service_name: extended_map} for successfully processed services.
    """
    processed: dict[str, dict[str, str]] = {}
    for service_name in services:
        extended_map = _get_admin_updated_map(extended_dir / service_name)
        if not extended_map:
            continue
        if extended_map == stored_maps.get(service_name):
            logger.debug("Skipping unchanged service %s", service_name)
            continue
        logger.info("Processing matched for %s", service_name)
        if _process_service(service_name, extended_dir, matched_dir, bnda_path):
            processed[service_name] = extended_map
        else:
            logger.warning(
                "Matched processing failed for %s — will retry next run", service_name
            )
    return processed


def _portolan_add_services(  # noqa: PLR0913
    services: list[str],
    matched_dir: Path,
    work_dir: Path,
    service_meta: dict[str, dict],
    extended_maps: dict[str, dict[str, str]],
    workers: str,
) -> None:
    """Run portolan add for all matched services and re-apply enrichments."""
    for service_name in services:
        if not (matched_dir / service_name).exists():
            continue
        meta = service_meta.get(service_name, {})
        date_valid_on = (meta.get("date_valid_on") or "").strip()
        args = [
            "add",
            f"matched/{service_name}/",
            "--workers",
            workers,
            "--pmtiles",
        ]
        if date_valid_on:
            args += ["--datetime", date_valid_on]
        _write_service_metadata(matched_dir / service_name, service_name, meta or None)
        try:
            _portolan(args, cwd=work_dir)
        except CalledProcessError:
            logger.exception("portolan add failed for %s — skipping", service_name)
            continue
        extended_map = extended_maps.get(service_name, {})
        service_dir = matched_dir / service_name
        _enrich_matched_catalog(service_dir, meta, extended_map)
        _enrich_matched_layer_collections(service_dir, extended_map)


def run(work_dir: Path) -> None:
    """Mirror edge-matched COD-AB boundaries to source.coop."""
    extended_dir = work_dir / "extended"
    matched_dir = work_dir / "matched"
    matched_dir.mkdir(parents=True, exist_ok=True)

    if not extended_dir.exists():
        logger.error("extended/ not found at %s — run extended first", extended_dir)
        return

    bnda_path = _ensure_bnda(work_dir)

    services = sorted(
        d.name
        for d in extended_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )
    logger.info("Found %d services in extended catalog", len(services))

    service_meta: dict[str, dict] = {}
    for service_name in services:
        catalog_path = extended_dir / service_name / "catalog.json"
        if catalog_path.exists():
            data = json.loads(catalog_path.read_text())
            service_meta[service_name] = {
                k[len("cod_ab:") :]: v
                for k, v in data.items()
                if k.startswith("cod_ab:") and k != "cod_ab:extended_updated"
            }

    stored_maps: dict[str, dict[str, str]] = {
        svc: _load_stored_extended_updated(matched_dir / svc)
        for svc in services
        if (matched_dir / svc).exists()
    }

    processed_maps = _run_clipping(
        services, extended_dir, matched_dir, stored_maps, bnda_path
    )

    all_extended_maps: dict[str, dict[str, str]] = {**stored_maps, **processed_maps}

    workers = str(PORTOLAN_WORKERS)
    if matched_dir.exists() and any(
        d.is_dir() and not d.name.startswith(".") for d in matched_dir.iterdir()
    ):
        _remove_stale_services(services, matched_dir, "matched", work_dir)
    _portolan_add_services(
        services, matched_dir, work_dir, service_meta, all_extended_maps, workers
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
        logger.exception("portolan push failed — matched catalog not synced to S3")
        return
    _push_catalog_files(work_dir, SOURCECOOP_REMOTE)
    _portolan(["check", "--verbose"], cwd=work_dir)
