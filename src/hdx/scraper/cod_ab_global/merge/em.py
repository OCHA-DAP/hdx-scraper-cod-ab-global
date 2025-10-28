from pathlib import Path
from subprocess import run

from geopandas import read_parquet

from ..utils import to_parquet


def main(data_dir: Path) -> None:
    """Generate edge-matched boundaries for all datasets."""
    layers = ["0", "1", "2", "3", "4", "lines", "points"]
    for layer in layers:
        input_path = sorted((data_dir / "cod_em").rglob(f"*_admin{layer}.parquet"))
        output_path = data_dir / "cod_em" / f"admin{layer}.parquet"
        run(
            [
                *["gdal", "vector", "concat"],
                *[*input_path, output_path],
                "--overwrite",
                "--mode=single",
                "--lco=COMPRESSION=ZSTD",
            ],
            check=False,
        )
        if layer == "lines":
            gdf = read_parquet(data_dir / "cod_em" / "adminlines.parquet")
            layers = [
                "iso3",
                "cod_version",
                "valid_on",
                "valid_to",
                "adm_level",
                "adm0_type",
            ]
            gdf = gdf.dissolve(by=layers, dropna=False, as_index=False)
            gdf.to_file(data_dir / "cod_em" / "adminlines.gpkg", index=False)
            to_parquet(data_dir / "cod_em" / "adminlines.gpkg")
