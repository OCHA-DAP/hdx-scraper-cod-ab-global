from pathlib import Path
from shutil import copy
from venv import logger

from pandas import read_parquet


def preprocess_extended(data_path: Path) -> None:
    """Preprocess extended boundaries."""
    df = read_parquet(
        data_path / "metadata_all.parquet",
        columns=["country_iso3", "version", "admin_level_full"],
    )
    for layer in sorted((data_path / "country/original").glob("cod_ab_*")):
        iso3 = layer.name.split("_")[2].upper()
        version = layer.name.split("_")[-1]
        try:
            level_full = df[(df["country_iso3"] == iso3) & (df["version"] == version)][
                "admin_level_full"
            ].iloc[0]
        except IndexError:
            level_full = int(sorted(layer.glob("*.parquet"))[-1].stem[-1])
        admin_path = layer / f"{iso3.lower()}_admin{level_full}.parquet"
        preprocessed_path = data_path / "country/extended/preprocessed" / layer.name
        preprocessed_path.mkdir(parents=True, exist_ok=True)
        try:
            copy(admin_path, preprocessed_path / admin_path.name)
        except FileNotFoundError:
            for offset in range(5):
                try:
                    admin_path = (
                        layer / f"{iso3.lower()}_admin{level_full + offset}.parquet"
                    )
                    copy(admin_path, preprocessed_path / admin_path.name)
                    break
                except FileNotFoundError as e:
                    logger.info(e)
            for offset in range(5):
                try:
                    admin_path = (
                        layer / f"{iso3.lower()}_admin{level_full - offset}.parquet"
                    )
                    copy(admin_path, preprocessed_path / admin_path.name)
                    break
                except FileNotFoundError as e:
                    logger.info(e)
