"""Build a content fingerprint over the version_dirs a builder will process."""

from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import admin_layers, file_fingerprint
from hdx.scraper.cod_ab_global.config import iso3_exclude, iso3_include


def _deepest_layer_fingerprint(version_dir: Path, iso3: str) -> list[int] | None:
    """Return [level, size, mtime_ns] for the deepest existing admin parquet."""
    layers = admin_layers(version_dir, iso3)
    if not layers:
        return None
    level, layer_dir = layers[-1]
    fp = file_fingerprint(layer_dir / f"{layer_dir.name}.parquet")
    return [level, *fp] if fp else None


def build_fingerprint(version_dirs: list[tuple[str, Path]]) -> dict:
    """Build a content fingerprint from the version_dirs a builder will process."""
    services_fp = {}
    for iso3, version_dir in version_dirs:
        fp = _deepest_layer_fingerprint(version_dir, iso3)
        if fp is not None:
            services_fp[f"{iso3}/{version_dir.name}"] = fp
    return {
        "services": services_fp,
        "iso3_include": sorted(iso3_include),
        "iso3_exclude": sorted(iso3_exclude),
    }
