from pathlib import Path
from shutil import copy, rmtree
from subprocess import run
from venv import logger

from pandas import read_parquet

from ..config import gdal_parquet_options, where_filter


def _get_input_path(
    data_path: Path,
    layer: Path,
    iso3: str,
    version: str,
) -> Path | None:
    """Get admin level full."""
    df = read_parquet(
        data_path / "metadata/global_admin_boundaries_metadata_all.parquet",
        columns=["country_iso3", "version", "admin_level_full"],
    )
    try:
        level_full = df[(df["country_iso3"] == iso3) & (df["version"] == version)][
            "admin_level_full"
        ].iloc[0]
    except IndexError:
        level_full = int(sorted(layer.glob("*.parquet"))[-1].stem[-1])
    input_path = layer / f"{iso3.lower()}_admin{level_full}.parquet"
    if input_path.exists():
        return input_path
    for offset in range(5):
        input_path = layer / f"{iso3.lower()}_admin{level_full + offset}.parquet"
        if input_path.exists():
            logger.info(
                f"Using offset ADM{level_full}+{offset} "
                f"(ADM{level_full + offset}) for {iso3}_{version}",
            )
            return input_path
    for offset in range(5):
        input_path = layer / f"{iso3.lower()}_admin{level_full - offset}.parquet"
        if input_path.exists():
            logger.info(
                f"Using offset ADM{level_full}-{offset} "
                f"(ADM{level_full - offset}) for {iso3}_{version}",
            )
            return input_path
    logger.warning(f"No admin level found for {iso3}_{version}.")
    return None


def _gdal_filter(input_path: Path, output_path: Path, iso3: str) -> None:
    """Run gdal concat."""
    run(
        [
            *["gdal", "vector", "filter"],
            *[input_path, output_path],
            f"--where={where_filter[iso3]}",
            *gdal_parquet_options,
        ],
        check=False,
    )


def preprocess_extended(data_path: Path) -> None:
    """Preprocess extended boundaries."""
    preprocess_path = data_path / "country/extended_pre"
    preprocess_path.mkdir(parents=True, exist_ok=True)
    for layer in sorted((data_path / "country/original").glob("cod_ab_*")):
        iso3 = layer.name.split("_")[2].upper()
        version = layer.name.split("_")[-1]
        input_path = _get_input_path(data_path, layer, iso3, version)
        if input_path is None:
            continue
        output_path = (
            preprocess_path
            / input_path.with_stem(input_path.stem.replace("_", f"_{version}_")).name
        )
        if iso3 in where_filter:
            _gdal_filter(input_path, output_path, iso3)
        else:
            copy(input_path, output_path)
        rmtree(layer)
    rmtree(data_path / "country/original")
