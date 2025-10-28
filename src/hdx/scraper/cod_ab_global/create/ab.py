from pathlib import Path

from geopandas import read_parquet

from ..utils import to_parquet
from .utils import get_columns


def main(output_dir: Path, layer_name: str) -> None:
    """Standardize admin layer input."""
    iso3 = layer_name.split("_")[0].upper()
    admin_level = int(layer_name[-1])
    output_path = output_dir / f"{layer_name}.gpkg"
    gdf = read_parquet(output_dir / f"{layer_name}.parquet")
    gdf = gdf.rename(columns={"cod_version": "version"})
    gdf["valid_to"] = gdf["valid_to"].astype("date32[pyarrow]")
    gdf["version"] = gdf["version"].str.replace("V_", "v")
    gdf["iso3"] = iso3
    gdf = gdf[get_columns(admin_level)]
    gdf = gdf.sort_values(by=[f"adm{admin_level}_pcode"])
    gdf.to_file(output_path, index=False)
    to_parquet(output_path)
