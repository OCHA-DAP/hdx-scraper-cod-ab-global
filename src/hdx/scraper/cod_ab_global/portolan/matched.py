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
import tempfile
from pathlib import Path

import duckdb
import geoparquet_io as gpio
from topo_tools import clean as topo_clean
from topo_tools import clip as topo_clip

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
    """Fix gap/overlap coverage defects in raw BNDA via topo_tools.

    Replaces the old raw ST_CoverageClean + ST_MakeValid pass with
    topo_tools.clean(), which validates area drift, collapsed features, and
    bad geometry types before accepting the coverage-clean result.
    """
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-clean-") as tmp:
        issues_path = Path(tmp) / "bnda_issues.parquet"
        topo_clean(raw_path, out_path, issues_path, overwrite=True)


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


def _clip_to_bnda(input_path: Path, output_path: Path, bnda_path: Path) -> None:
    """Clip one extended admin layer to the UN international boundary via topo_tools.

    The parent passed to topo_tools.clip() is pre-filtered to this country's
    own BNDA row (a single-row parquet), so assign-one's majority-vote parent
    selection has only one candidate — preserving the exact "clip to this
    iso3's own reference boundary" semantics rather than a geometry-based vote.
    """
    iso3 = input_path.parent.parent.parent.name.upper()
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        with tempfile.TemporaryDirectory(prefix="portolan-matched-clip-") as tmp:
            tmp_path = Path(tmp)
            parent_path = tmp_path / "parent.parquet"
            con.execute(f"""
                COPY (
                    SELECT geometry FROM read_parquet('{bnda_path}')
                    WHERE iso3cd = '{iso3}'
                ) TO '{parent_path}' (FORMAT PARQUET)
            """)

            tmp_out = tmp_path / "clipped.parquet"
            topo_clip(input_path, parent_path, tmp_out, overwrite=True)
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
