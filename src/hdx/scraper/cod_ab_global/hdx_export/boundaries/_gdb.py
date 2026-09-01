"""Append assembled admin-level parquets as layers into the shared File GDB."""

from pathlib import Path

import geopandas as gpd
import pandas as pd


def append_layer_to_gdb(parquet_path: Path, gdb_path: Path, admin_level: int) -> None:
    """Append one admin-level parquet as a layer into the shared GDB."""
    gdf = gpd.read_parquet(parquet_path)
    for col in ("valid_on", "valid_to"):
        if col in gdf.columns:
            gdf[col] = pd.to_datetime(gdf[col])
    gdf.to_file(
        gdb_path,
        layer=f"admin{admin_level}",
        driver="OpenFileGDB",
        append=gdb_path.exists(),
        promote_to_multi=True,
    )


def max_original_level(
    version_dirs: list[tuple[str, Path]], upper_bound: int = 9
) -> int:
    """Return the deepest adm{N}/original.parquet actually present, up to upper_bound.

    Unlike extended/matched, "original" mirrors whatever raw ArcGIS layers
    exist regardless of the "official" admin_level_full depth.
    """
    max_seen = 0
    for iso3, version_dir in version_dirs:
        for n in range(upper_bound, -1, -1):
            layer_name = f"{iso3}_admin{n}"
            if (version_dir / layer_name / f"{layer_name}.parquet").exists():
                max_seen = max(max_seen, n)
                break
    return max_seen
