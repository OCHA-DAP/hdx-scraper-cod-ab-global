"""Shared GeoParquet 2.0 writer used across extended/matched/global stages."""

from pathlib import Path

import geoparquet_io as gpio


def write_gpq2(src: Path, dest: Path) -> None:
    """Read a GeoParquet file, Hilbert-sort it, and write as GeoParquet 2.0."""
    gpio.read(str(src)).sort_hilbert().write(
        str(dest), compression_level=22, geoparquet_version="2.0"
    )
