"""Shared metadata-saving helpers."""

from pathlib import Path

from pandas import DataFrame


def _save_metadata_files(output_file: Path, df: DataFrame) -> None:
    """Save metadata in parquet and csv."""
    df.to_parquet(
        output_file,
        compression="zstd",
        compression_level=15,
        index=False,
    )
    df.to_csv(
        output_file.with_suffix(".csv"),
        index=False,
        encoding="utf-8-sig",
    )


def save_metadata(output_file: Path, df_all: DataFrame) -> None:
    """Save metadata in with all and latest versions."""
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_all"),
        df_all,
    )
    df_latest = df_all.drop_duplicates(subset=["country_iso3"], keep="last")
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_latest"),
        df_latest,
    )
    key_columns = ["country_iso3", "version"]
    df_historic = df_all.merge(
        df_latest[key_columns],
        on=key_columns,
        how="left",
        indicator=True,
    )
    df_historic = df_historic[df_historic["_merge"] == "left_only"].drop(
        columns=["_merge"],
    )
    _save_metadata_files(
        output_file.with_stem(output_file.stem + "_historic"),
        df_historic,
    )
