"""Find each country's latest matched/ version and its deepest admin layer."""

import logging
from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import admin_layers, iter_version_dirs

logger = logging.getLogger(__name__)


def latest_versioned_per_iso3(matched_dir: Path) -> dict[str, Path]:
    """Return {iso3: version_dir} for the highest-versioned service per iso3."""
    best: dict[str, tuple[int, Path]] = {}
    for iso3, version, version_dir in iter_version_dirs(matched_dir):
        if version.startswith("v") and version[1:].isdigit():
            n = int(version[1:])
            if iso3 not in best or n > best[iso3][0]:
                best[iso3] = (n, version_dir)
    return {iso3: info[1] for iso3, info in best.items()}


def get_service_meta(version_dir: Path, iso3: str) -> dict | None:
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
