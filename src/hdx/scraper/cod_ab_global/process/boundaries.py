from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run


def gdal_concat_single(input_paths: list[Path], output_path: Path, stage: str) -> None:
    """Run gdal concat to combine countries into a single-layer output."""
    clean_coverage = ["clean-coverage", "!"] if stage == "matched" else []
    run(
        [
            *["gdal", "vector", "pipeline", "!"],
            *["concat", "--mode=single", *input_paths, "!"],
            *clean_coverage,
            *["make-valid", "!"],
            *["write", output_path],
            "--overwrite",
            "--quiet",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=True,
    )


def gdal_concat_multi(input_paths: list[Path], output_path: Path) -> None:
    """Run gdal concat to assemble admin levels into a single multi-layer output."""
    output_path.mkdir(parents=True, exist_ok=True)
    run(
        [
            *["gdal", "vector", "concat"],
            *[*input_paths, output_path / output_path.name],
            "--overwrite",
            "--quiet",
            "--skip-errors",
            "--lco=TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER",
        ],
        check=True,
    )
    for path in input_paths:
        path.unlink()
    make_archive(str(output_path), "zip", output_path)
    rmtree(output_path)


def create_boundaries(data_dir: Path, run_version: str, stage: str) -> None:
    """Generate global boundaries for datasets."""
    lvl_max = 6 if stage == "original" else 5
    output_dir = data_dir / "global"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = [output_dir / f"admin{level}.parquet" for level in range(lvl_max)]
    output_path = output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb"
    for level in range(lvl_max):
        input_paths = sorted(
            (data_dir / "country" / stage).rglob(f"*_admin{level}.parquet"),
        )
        gdal_concat_single(input_paths, output_paths[level], "")
    gdal_concat_multi(output_paths, output_path)
