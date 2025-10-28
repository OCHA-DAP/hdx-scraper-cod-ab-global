from pathlib import Path
from subprocess import run

from geopandas import GeoDataFrame, read_file, read_parquet
from pandas import NaT
from shapely import get_point

from ..utils import get_admin_level_full, to_parquet
from .utils import get_columns


def create_polygon(
    data_dir: Path,
    iso3: str,
    level_full: int,
    admin_level: int,
) -> GeoDataFrame:
    """Make an edge-matched polygon."""
    input_path = data_dir / "cod_ee" / f"{iso3.lower()}_admin{level_full}.parquet"
    output_path = (
        data_dir / "cod_em" / iso3.lower() / f"{iso3.lower()}_admin{admin_level}.gpkg"
    )
    filters = [("iso3cd", "=", iso3)]
    cty = read_parquet(data_dir / "bnda_cty.parquet", filters=filters)
    gdf = read_parquet(input_path)
    gdf = gdf.dissolve(f"adm{admin_level}_pcode", dropna=False, as_index=False)
    gdf = gdf.clip(cty, keep_geom_type=True)
    gdf["iso3"] = iso3
    gdf["area_sqkm"] = gdf.geometry.to_crs(6933).area / 1_000_000
    gdf["valid_to"] = gdf["valid_to"].astype("date32[pyarrow]")
    gdf = gdf[get_columns(admin_level)]
    gdf = gdf.sort_values(by=[f"adm{admin_level}_pcode"])
    gdf["adm_level"] = admin_level
    gdf.to_file(output_path, index=False)
    run(
        [
            *["gdal", "vector", "select"],
            *[output_path, output_path.with_suffix(".parquet")],
            *["--exclude", "fid"],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )
    output_path.unlink()
    return gdf


def create_points(gdf: GeoDataFrame, output_path: Path, admin_level: int) -> None:
    """Make an edge-matched point."""
    gdf = gdf.copy()
    gdf = gdf[get_columns(admin_level, is_point=True)]
    gdf = gdf.rename(
        columns={
            f"adm{admin_level}_name": "adm_name",
            f"adm{admin_level}_name1": "adm_name1",
            f"adm{admin_level}_name2": "adm_name2",
            f"adm{admin_level}_name3": "adm_name3",
            f"adm{admin_level}_pcode": "adm_pcode",
        },
    )
    gdf["adm_level"] = admin_level
    gdf.geometry = get_point(
        gdf.geometry.maximum_inscribed_circle(tolerance=0.000001),
        0,
    )
    gdf.to_file(output_path, append=True, index=False)


def create_adm0_lines(data_dir: Path, iso3: str, output_path: Path) -> None:
    """Make an edge-matched international lines."""
    gdf = read_parquet(data_dir / "bndl.parquet")
    gdf = gdf.rename(columns={"iso3cd": "iso3", "bdytyp": "adm0_type"})
    gdf["iso3"] = gdf["iso3"].fillna("")
    gdf = gdf[gdf["iso3"].str.contains(iso3)]
    gdf = gdf.dissolve(by=["adm0_type", "iso3"], dropna=False, as_index=False)
    for date in ["valid_on", "valid_to"]:
        gdf[date] = NaT
        gdf[date] = gdf[date].astype("date32[pyarrow]")
    gdf["cod_version"] = None
    columns = ["iso3", "adm0_type", "valid_on", "valid_to", "cod_version", "geometry"]
    gdf = gdf[columns]
    gdf["adm_level"] = 0
    gdf.to_file(output_path, index=False)


def create_lines(
    gdf: GeoDataFrame,
    data_dir: Path,
    output_path: Path,
    iso3: str,
    admin_level: int,
) -> None:
    """Make edge-matched lines."""
    if admin_level == 0:
        create_adm0_lines(data_dir, iso3, output_path)
    else:
        gdf = gdf.copy()
        gdf_parent = read_file(output_path, use_arrow=True)
        gdf.geometry = gdf.boundary
        gdf = gdf.overlay(gdf_parent, how="difference").dissolve(as_index=False)
        columns = ["iso3", "valid_on", "valid_to", "cod_version", "geometry"]
        gdf = gdf[columns]
        gdf["adm_level"] = admin_level
        gdf.to_file(output_path, append=True, index=False)


def main(data_dir: Path, iso3: str) -> None:
    """Generate edge-matched boundaries for all datasets."""
    level_full = get_admin_level_full(iso3)
    em_path = data_dir / "cod_em" / iso3.lower()
    em_path.mkdir(parents=True, exist_ok=True)
    points_path = em_path / f"{iso3.lower()}_adminpoints.gpkg"
    points_path.unlink(missing_ok=True)
    lines_path = em_path / f"{iso3.lower()}_adminlines.gpkg"
    lines_path.unlink(missing_ok=True)
    for admin_level in range(level_full + 1):
        gdf = create_polygon(data_dir, iso3, level_full, admin_level)
        create_lines(gdf, data_dir, lines_path, iso3, admin_level)
        create_points(gdf, points_path, admin_level)
    to_parquet(lines_path)
    to_parquet(points_path)
