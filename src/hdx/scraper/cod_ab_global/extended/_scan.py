"""Resolve services and each one's deepest real (non-filled) admin level."""

from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import (
    admin_layers,
    iter_version_dirs,
    read_catalog,
)


def get_admin_level_full(original_version_dir: Path, iso3: str) -> int | None:
    """Return admin_level_full from original/'s catalog.json, verified on disk.

    Falls back to the highest native admin dir with an existing parquet.
    """
    catalog = read_catalog(original_version_dir)
    val = catalog.get("cod_ab:admin_level_full")
    if val is not None:
        try:
            level = int(val)
        except (TypeError, ValueError):
            level = None
        if level is not None:
            layer_name = f"{iso3}_admin{level}"
            seed = original_version_dir / layer_name / f"{layer_name}.parquet"
            if seed.exists():
                return level
    layers = admin_layers(original_version_dir, iso3)
    return layers[-1][0] if layers else None


def enumerate_services(root_dir: Path) -> list[tuple[str, str]]:
    """Return [(iso3, version), ...] for all service dirs in root_dir."""
    return [
        (iso3, version)
        for iso3, version, _ in iter_version_dirs(root_dir)
        if (version.startswith("v") and version[1:].isdigit()) or version == "latest"
    ]
