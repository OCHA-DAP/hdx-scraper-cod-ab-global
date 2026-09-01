"""Edge-match COD-AB boundaries from extended/ into matched/.

Clips each admin1+ layer to UN 1:1M international boundaries via topo_tools.
Admin0 is excluded; layer naming mirrors original/ and extended/ exactly.
"""

import logging
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

import duckdb
import geoparquet_io as gpio
from topo_tools import edge_clip as topo_clip
from topo_tools import topo_clean

from .config import ARCGIS_SERVICES_URL, PORTOLAN_WORKERS
from .extended import _write_gpq2, enumerate_services, file_fingerprint
from .original import (
    _portolan,
    admin_layer_pattern,
    read_json_state,
    remove_stale_versions,
    write_json_state,
)
from .utils import generate_token

logger = logging.getLogger(__name__)

_BNDA_URL = f"{ARCGIS_SERVICES_URL}/Global_AB_1M_fs_gray/FeatureServer/5"


def _clean_bnda(raw_path: Path, out_path: Path) -> None:
    """Fix gap/overlap coverage defects in raw BNDA via topo_tools."""
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-clean-") as tmp:
        issues_path = Path(tmp) / "bnda_issues.parquet"
        topo_clean(raw_path, out_path, issues_path, overwrite=True)


def _ensure_bnda(catalogs_dir: Path) -> Path:
    """Return catalogs_dir/.bnda/bnda_cty.parquet, downloading it if absent."""
    bnda_dir = catalogs_dir / ".bnda"
    bnda_dir.mkdir(exist_ok=True)
    bnda_path = bnda_dir / "bnda_cty.parquet"
    if bnda_path.exists():
        return bnda_path
    logger.info("Downloading UN BNDA boundaries from %s", _BNDA_URL)
    token = generate_token()
    table = gpio.extract_arcgis(_BNDA_URL, token=token, timeout=300)
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-") as tmp:
        raw_path = Path(tmp) / "bnda_raw.parquet"
        clean_path = Path(tmp) / "bnda_clean.parquet"
        table.write(str(raw_path), compression_level=15, geoparquet_version="2.0")
        _clean_bnda(raw_path, clean_path)
        _write_gpq2(clean_path, bnda_path)
    logger.info("Saved BNDA to %s", bnda_path)
    return bnda_path


def _clip_to_bnda(
    input_path: Path, output_path: Path, bnda_path: Path, iso3: str
) -> None:
    """Clip one extended admin layer to the UN international boundary."""
    iso3_upper = iso3.upper()
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        with tempfile.TemporaryDirectory(prefix="portolan-matched-clip-") as tmp:
            tmp_path = Path(tmp)
            parent_path = tmp_path / "parent.parquet"
            con.execute(f"""
                COPY (
                    SELECT geometry FROM read_parquet('{bnda_path}')
                    WHERE iso3cd = '{iso3_upper}'
                ) TO '{parent_path}' (FORMAT PARQUET)
            """)

            tmp_out = tmp_path / "clipped.parquet"
            topo_clip(input_path, parent_path, tmp_out, overwrite=True)
            _write_gpq2(tmp_out, output_path)
    finally:
        con.close()


def _admin_layers(version_dir: Path, iso3: str) -> list[tuple[int, Path]]:
    """Return [(level, layer_dir), ...] with an existing parquet, sorted by level."""
    if not version_dir.exists():
        return []
    pattern = admin_layer_pattern(iso3)
    found = []
    for d in version_dir.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if m and (d / f"{d.name}.parquet").exists():
            found.append((int(m.group(1)), d))
    return sorted(found)


def _process_service(
    iso3: str,
    version: str,
    extended_version_dir: Path,
    matched_version_dir: Path,
    bnda_path: Path,
) -> bool:
    """Clip all admin1+ layers for one service to UN boundaries."""
    layers = [
        (level, d)
        for level, d in _admin_layers(extended_version_dir, iso3)
        if level > 0
    ]
    if not layers:
        logger.warning(
            "No admin1+ extended layers found for %s/%s, skipping", iso3, version
        )
        return False

    pattern = admin_layer_pattern(iso3)
    current_levels = {level for level, _ in layers}
    if matched_version_dir.exists():
        for d in matched_version_dir.iterdir():
            m = d.is_dir() and pattern.match(d.name)
            if m and int(m.group(1)) not in current_levels:
                (d / f"{d.name}.parquet").unlink(missing_ok=True)

    try:
        for level, layer_dir in layers:
            layer_name = f"{iso3}_admin{level}"
            input_path = layer_dir / f"{layer_name}.parquet"
            out_dir = matched_version_dir / layer_name
            out_dir.mkdir(parents=True, exist_ok=True)
            output_path = out_dir / f"{layer_name}.parquet"
            _clip_to_bnda(input_path, output_path, bnda_path, iso3)
    except Exception:
        logger.exception("Matched clipping failed for %s/%s", iso3, version)
        return False

    logger.info("Matched %s/%s successfully", iso3, version)
    return True


def run(extended_dir: Path, matched_dir: Path, catalogs_dir: Path) -> None:
    """Edge-match admin1+ layers from extended/ into matched/."""
    services = enumerate_services(extended_dir)
    if not services:
        logger.warning("No services found in %s, run extended first", extended_dir)
        return
    logger.info("Found %d services to process for matched", len(services))

    remove_stale_versions(set(services), matched_dir)
    bnda_path = _ensure_bnda(catalogs_dir)
    workers = str(PORTOLAN_WORKERS)

    fingerprints_path = matched_dir / ".state" / "fingerprints.json"
    stored_fingerprints = read_json_state(fingerprints_path)
    new_fingerprints: dict[str, list[int]] = {}

    for iso3, version in services:
        key = f"{iso3}/{version}"
        extended_version_dir = extended_dir / iso3 / version
        matched_version_dir = matched_dir / iso3 / version

        layers = _admin_layers(extended_version_dir, iso3)
        if not layers:
            continue
        deepest_level, deepest_dir = layers[-1]
        deepest_path = deepest_dir / f"{iso3}_admin{deepest_level}.parquet"
        fingerprint = file_fingerprint(deepest_path)
        if fingerprint is None:
            continue
        new_fingerprints[key] = fingerprint

        if fingerprint == stored_fingerprints.get(key) and matched_version_dir.exists():
            logger.debug("Skipping unchanged matched for %s/%s", iso3, version)
            continue

        logger.info("Processing matched for %s/%s", iso3, version)
        if not _process_service(
            iso3, version, extended_version_dir, matched_version_dir, bnda_path
        ):
            logger.warning(
                "Matched processing failed for %s/%s, will retry next run",
                iso3,
                version,
            )
            continue

        try:
            _portolan(
                [
                    "add",
                    f"{iso3}/{version}/",
                    "--workers",
                    workers,
                    "--pmtiles",
                    "--force",
                ],
                cwd=matched_dir,
            )
        except CalledProcessError:
            logger.warning("portolan add failed for %s/%s (continuing)", iso3, version)

    write_json_state(fingerprints_path, new_fingerprints)
