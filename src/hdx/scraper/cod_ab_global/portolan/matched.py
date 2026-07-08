"""Mirror edge-matched COD-AB boundaries to source.coop.

Reads from the local unified catalog (no ArcGIS calls except for the one-time
BNDA download), clips each admin layer to UN 1:1M international boundaries,
and injects matched assets into each layer's collection.json.

Admin0 is excluded — clipping a country boundary to its own reference is
redundant. Change detection uses the extended layers' updated timestamps; a
service is re-clipped only when its extended catalog has changed.
"""

import contextlib
import json
import logging
import math
import tempfile
from pathlib import Path

import duckdb
import geoparquet_io as gpio

from .config import ARCGIS_SERVICES_URL, PORTOLAN_WORKERS
from .extended import (
    _ADMIN_POLYGON_RE,
    _enumerate_services,
    _get_admin_updated_map,
    _write_gpq2,
)
from .original import (
    _generate_variant_pmtiles,
    inject_variant_assets,
    read_catalog,
)
from .utils import generate_token

logger = logging.getLogger(__name__)

_BNDA_URL = f"{ARCGIS_SERVICES_URL}/Global_AB_1M_fs_gray/FeatureServer/5"


def _load_stored_extended_updated(version_dir: Path) -> dict[str, str]:
    """Return stored extended updated map from the version catalog.json."""
    raw = read_catalog(version_dir).get("cod_ab:extended_updated")
    if not raw:
        return {}
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        return json.loads(raw)
    return {}


def _clean_bnda(raw_path: Path, out_path: Path) -> None:
    """Apply ST_CoverageClean + ST_MakeValid to raw BNDA.

    Matches the old download/admin0.py gdal pipeline's clean-coverage +
    make-valid steps, done here via DuckDB spatial instead of the gdal CLI.
    """
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        con.execute(f"""
            COPY (
                WITH numbered AS (
                    SELECT row_number() OVER () AS rn, *
                    FROM read_parquet('{raw_path}')
                ),
                cleaned_coll AS (
                    SELECT ST_CoverageClean(list(geometry ORDER BY rn)) AS c
                    FROM numbered
                ),
                dumped AS (
                    SELECT unnest(ST_Dump(c)) AS d FROM cleaned_coll
                ),
                cleaned AS (
                    SELECT d.path[1] AS idx, ST_MakeValid(d.geom) AS geometry
                    FROM dumped
                )
                SELECT n.* EXCLUDE (rn, geometry), c.geometry
                FROM numbered n JOIN cleaned c ON n.rn = c.idx
                WHERE NOT ST_IsEmpty(c.geometry)
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()


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
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-") as tmp:
        raw_path = Path(tmp) / "bnda_raw.parquet"
        clean_path = Path(tmp) / "bnda_clean.parquet"
        table.write(str(raw_path), compression_level=15, geoparquet_version="2.0")
        _clean_bnda(raw_path, clean_path)
        _write_gpq2(clean_path, bnda_path)
    logger.info("Saved BNDA to %s", bnda_path)
    return bnda_path


def _subdivide_boundary(con: duckdb.DuckDBPyConnection, clip_cell: float = 1.0) -> None:
    """Grid-tile the `clip_one` temp table into `btile (geom GEOMETRY)`.

    Splits the boundary polygon into small cells so each ST_Intersection call only
    sees one cell-sized piece — bounds peak memory for dense-polygon countries like
    PHL (7,000-island MultiPolygon). Only occupied longitude strips are iterated so
    antimeridian-spanning countries (USA/RUS) don't process empty ocean bands.
    """
    bbox = con.execute(
        "SELECT ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom)"
        " FROM clip_one"
    ).fetchone()
    x0, y0, _x1, y1 = bbox or (0.0, 0.0, 0.0, 0.0)
    ny = max(1, math.ceil((y1 - y0) / clip_cell))

    occupied = con.execute(f"""
        WITH parts AS (SELECT (UNNEST(ST_Dump(geom))).geom AS g FROM clip_one)
        SELECT DISTINCT UNNEST(range(
            CAST(floor((ST_XMin(g) - {x0}) / {clip_cell}) AS INTEGER),
            CAST(floor((ST_XMax(g) - {x0}) / {clip_cell}) AS INTEGER) + 1
        )) AS i
        FROM parts ORDER BY i
    """).fetchall()

    con.execute("CREATE TEMP TABLE btile (geom GEOMETRY)")
    for (i,) in occupied:
        sx0, sx1 = x0 + i * clip_cell, x0 + (i + 1) * clip_cell
        con.execute(f"""
            INSERT INTO btile
            WITH strip AS (
                SELECT ST_Intersection(
                    geom, ST_MakeEnvelope({sx0}, {y0}, {sx1}, {y1})
                ) AS g FROM clip_one
            ),
            gy AS (SELECT UNNEST(range({ny})) AS j)
            SELECT geom FROM (
                SELECT ST_Intersection(
                    strip.g,
                    ST_MakeEnvelope(
                        {sx0}, {y0} + j * {clip_cell},
                        {sx1}, {y0} + (j + 1) * {clip_cell}
                    )
                ) AS geom
                FROM strip, gy
                WHERE NOT ST_IsEmpty(strip.g)
            ) WHERE NOT ST_IsEmpty(geom)
        """)


def _clip_to_bnda(input_path: Path, output_path: Path, bnda_path: Path) -> None:
    """Clip one extended admin layer to the UN international boundary via DuckDB."""
    iso3 = input_path.parent.parent.parent.name.upper()
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        con.execute("SET preserve_insertion_order=false")

        con.execute(
            f"CREATE TEMP TABLE src_one AS SELECT * FROM read_parquet('{input_path}')"
        )
        con.execute(f"""
            CREATE TEMP TABLE clip_one AS
            SELECT geometry AS geom FROM read_parquet('{bnda_path}')
            WHERE iso3cd = '{iso3}'
        """)

        _subdivide_boundary(con)

        con.execute("""
            CREATE TEMP TABLE src_clipped AS
            SELECT s.* EXCLUDE (geometry),
                   ST_Multi(ST_CollectionExtract(
                       ST_Union_Agg(ST_Intersection(s.geometry, b.geom)), 3
                   ))::GEOMETRY AS geometry
            FROM src_one s
            JOIN btile b
              ON ST_XMax(b.geom) >= ST_XMin(s.geometry)
             AND ST_XMin(b.geom) <= ST_XMax(s.geometry)
             AND ST_YMax(b.geom) >= ST_YMin(s.geometry)
             AND ST_YMin(b.geom) <= ST_YMax(s.geometry)
            WHERE ST_GeometryType(s.geometry) IN ('POLYGON', 'MULTIPOLYGON')
            GROUP BY ALL
        """)

        with tempfile.TemporaryDirectory(prefix="portolan-matched-clip-") as tmp:
            tmp_out = Path(tmp) / "clipped.parquet"
            con.execute(f"""
                COPY (
                    SELECT * FROM src_clipped
                    WHERE geometry IS NOT NULL AND NOT ST_IsEmpty(geometry)
                ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            _write_gpq2(tmp_out, output_path)
    finally:
        con.close()


def _process_service(
    iso3: str,
    version: str,
    version_dir: Path,
    bnda_path: Path,
) -> bool:
    """Clip all adm1+ layers for one service to UN boundaries.

    Returns True on success. Removes stale matched parquets before writing new
    ones so shrinking admin_level_full doesn't leave orphan files.
    """
    layers = sorted(
        d
        for d in version_dir.iterdir()
        if d.is_dir()
        and _ADMIN_POLYGON_RE.match(d.name)
        and d.name != "adm0"
        and (d / "extended.parquet").exists()
    )
    if not layers:
        logger.warning(
            "No adm1+ extended layers found for %s/%s — skipping", iso3, version
        )
        return False

    # Remove stale matched parquets before writing new ones
    for level in range(10):
        stale_dir = version_dir / f"adm{level}"
        if stale_dir.exists():
            for stale in ("matched.parquet", "matched.pmtiles"):
                (stale_dir / stale).unlink(missing_ok=True)

    try:
        for layer_dir in layers:
            input_path = layer_dir / "extended.parquet"
            output_path = layer_dir / "matched.parquet"
            _clip_to_bnda(input_path, output_path, bnda_path)
    except Exception:
        logger.exception("Matched clipping failed for %s/%s", iso3, version)
        return False

    logger.info("Matched %s/%s successfully", iso3, version)
    return True


def _enrich_matched_catalog(version_dir: Path, extended_map: dict[str, str]) -> None:
    """Write cod_ab:extended_updated marker into the version catalog.json."""
    catalog_path = version_dir / "catalog.json"
    if catalog_path.exists() and extended_map:
        data = json.loads(catalog_path.read_text())
        data["cod_ab:extended_updated"] = json.dumps(extended_map)
        catalog_path.write_text(json.dumps(data, indent=2))


def _inject_all_matched_assets(version_dir: Path, workers: str) -> None:
    """Inject matched assets into all adm1+ collection.json files.

    Called for every service on every run to ensure portolan add (which
    regenerates collection.json with only original assets) doesn't lose
    matched assets.
    """
    for layer_dir in sorted(version_dir.iterdir()):
        if not layer_dir.is_dir() or not _ADMIN_POLYGON_RE.match(layer_dir.name):
            continue
        if layer_dir.name == "adm0":
            continue
        parquet = layer_dir / "matched.parquet"
        if not parquet.exists():
            continue
        if not (layer_dir / "matched.pmtiles").exists():
            _generate_variant_pmtiles(parquet, layer_dir, workers)
        inject_variant_assets(layer_dir / "collection.json", "matched")


def run(work_dir: Path) -> None:
    """Mirror edge-matched COD-AB boundaries into the unified catalog."""
    services = _enumerate_services(work_dir)
    if not services:
        logger.warning("No services found in %s — run extended first", work_dir)
        return
    logger.info("Found %d services to process for matched", len(services))

    bnda_path = _ensure_bnda(work_dir)
    workers = str(PORTOLAN_WORKERS)

    for iso3, version in services:
        version_dir = work_dir / iso3 / version
        extended_map = _get_admin_updated_map(version_dir)
        if not extended_map:
            continue

        stored = _load_stored_extended_updated(version_dir)
        if extended_map != stored:
            logger.info("Processing matched for %s/%s", iso3, version)
            if _process_service(iso3, version, version_dir, bnda_path):
                _enrich_matched_catalog(version_dir, extended_map)
            else:
                logger.warning(
                    "Matched processing failed for %s/%s — will retry next run",
                    iso3,
                    version,
                )
        else:
            logger.debug("Skipping unchanged %s/%s", iso3, version)

        # portolan add regenerates collection.json — always re-inject matched assets
        _inject_all_matched_assets(version_dir, workers)
