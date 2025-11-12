from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run


def get_latest_layers(input_path: Path, level: int) -> list[Path]:
    """Get the latest layers."""
    latest_layers = []
    latest_parents: dict[str, Path] = {}
    for layer_path in sorted(input_path.glob("cod_ab_*")):
        iso3 = layer_path.name.split("_")[2]
        latest_parents[iso3] = layer_path
    for parent in latest_parents.values():
        latest_layers.extend(sorted(parent.glob(f"*_admin{level}.parquet")))
    return latest_layers


def gdal_concat_single(input_paths: list[Path], output_path: Path) -> None:
    """Run gdal concat."""
    run(
        [
            *["gdal", "vector", "concat"],
            *[*input_paths, output_path],
            "--overwrite",
            "--quiet",
            "--mode=single",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )


def gdal_concat_multi(input_paths: list[Path], output_path: Path) -> None:
    """Run gdal concat."""
    output_path.mkdir(parents=True, exist_ok=True)
    run(
        [
            *["gdal", "vector", "concat"],
            *[*input_paths, output_path / output_path.name],
            "--overwrite",
            "--quiet",
            "--lco=TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER",
        ],
        check=False,
    )
    make_archive(str(output_path), "zip", output_path)
    rmtree(output_path)


def create_all_boundaries(data_dir: Path) -> None:
    """Generate edge-matched boundaries for all datasets."""
    output_dir = data_dir / "global" / "original" / "all"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = [output_dir / f"admin{level}.parquet" for level in range(6)]
    output_path = output_dir / "global_admin_boundaries_original_all.gdb"
    for level in range(6):
        input_paths = sorted(
            (data_dir / "country" / "original").rglob(f"*_admin{level}.parquet"),
        )
        gdal_concat_single(input_paths, output_paths[level])
    gdal_concat_multi(output_paths, output_path)


def create_latest_boundaries(data_dir: Path) -> None:
    """Generate edge-matched boundaries for latest datasets."""
    output_dir = data_dir / "global" / "original" / "latest"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = [output_dir / f"admin{level}.parquet" for level in range(6)]
    output_path = output_dir / "global_admin_boundaries_original_latest.gdb"
    for level in range(6):
        input_paths = get_latest_layers(data_dir / "country" / "original", level)
        gdal_concat_single(input_paths, output_paths[level])
    gdal_concat_multi(output_paths, output_path)


def create_original_boundaries(data_dir: Path) -> None:
    """Generate datasets for HDX."""
    create_all_boundaries(data_dir)
    create_latest_boundaries(data_dir)
