"""Build PMTiles with tylertoo beside each GeoParquet for `portolan add` to register."""

from pathlib import Path
from tempfile import TemporaryDirectory

import tylertoo

MAX_ZOOM = 11


def build_pmtiles(collections_dir: Path) -> None:
    """Write `<layer>.pmtiles` beside every `<layer>/<layer>.parquet` in a dir."""
    for parquet_path in sorted(collections_dir.glob("*/*.parquet")):
        with TemporaryDirectory() as tmp:
            overview_path = Path(tmp) / "overview.parquet"
            tylertoo.overview(str(parquet_path), str(overview_path), max_zoom=MAX_ZOOM)
            tylertoo.export_pmtiles(
                str(overview_path),
                str(parquet_path.with_suffix(".pmtiles")),
                layer_name=parquet_path.stem,
            )
