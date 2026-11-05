"""Build PMTiles with tylertoo beside each GeoParquet for `portolan add` to register."""

from pathlib import Path
from tempfile import TemporaryDirectory

import tylertoo
from portolan_cli.metadata.pmtiles import extract_pmtiles_metadata
from portolan_cli.viz.style import complete_style_sources

MAX_ZOOM = 11


def tile_parquet(parquet_path: Path, pmtiles_path: Path, layer_name: str) -> None:
    """Write one GeoParquet to a PMTiles archive with a single named layer."""
    with TemporaryDirectory() as tmp:
        overview_path = Path(tmp) / "overview.parquet"
        # Every feature at every zoom: only sub-pixel simplification drops detail.
        tylertoo.overview(
            str(parquet_path),
            str(overview_path),
            max_zoom=MAX_ZOOM,
            density_drop=False,
            point_thinning=0.0,
            line_thinning=0,
            line_visibility=0,
            polygon_thinning=0,
            polygon_visibility=0,
            profile="bounded",
        )
        # The default 512 KB limit drops features silently from oversized tiles.
        tylertoo.export_pmtiles(
            str(overview_path),
            str(pmtiles_path),
            layer_name=layer_name,
            tile_size_limit=None,
        )
    # `portolan add` only completes extract-written styles for archives it tiled itself.
    meta = extract_pmtiles_metadata(pmtiles_path)
    complete_style_sources(
        pmtiles_path.parent, pmtiles_path.name, meta.min_zoom, meta.max_zoom
    )


def build_pmtiles(collections_dir: Path) -> None:
    """Write `<layer>.pmtiles` beside every `<layer>/<layer>.parquet` in a dir."""
    for parquet_path in sorted(collections_dir.glob("*/*.parquet")):
        tile_parquet(
            parquet_path, parquet_path.with_suffix(".pmtiles"), parquet_path.stem
        )
