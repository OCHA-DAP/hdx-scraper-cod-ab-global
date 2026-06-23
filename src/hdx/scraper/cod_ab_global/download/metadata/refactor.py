"""Metadata table refactoring and enrichment."""

from pathlib import Path

import pandas as pd
from pandas import DataFrame, read_parquet

from hdx.scraper.cod_ab_global.config import (
    admin_level_full_overrides,
    date_valid_on_overrides,
    iso3_exclude,
    iso3_include,
)
from hdx.scraper.cod_ab_global.utils import save_metadata

ISO3_LEN = 3


count_columns = [
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
]

columns = [
    "country_name",
    "country_iso2",
    "country_iso3",
    "version",
    "admin_level_full",
    "admin_level_max",
    "admin_1_name",
    "admin_2_name",
    "admin_3_name",
    "admin_4_name",
    "admin_5_name",
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
    "admin_notes",
    "date_source",
    "date_updated",
    "date_reviewed",
    "date_metadata",
    "date_valid_on",
    "date_valid_to",
    "update_frequency",
    "update_type",
    "source",
    "contributor",
    "methodology_dataset",
    "methodology_pcodes",
    "caveats",
]


def _df_filter(df: DataFrame) -> DataFrame:
    """Filter DataFrame from iso3 include and exclude lists."""
    iso3_include_all = [x for x in iso3_include if len(x) == ISO3_LEN]
    iso3_exclude_all = [x for x in iso3_exclude if len(x) == ISO3_LEN]
    iso3_exclude_version = [x.replace("_V", "v") for x in iso3_exclude if "_V" in x]
    if len(iso3_include):
        df = df[df["country_iso3"].isin(iso3_include_all)]
    df = df[~df["country_iso3"].isin(iso3_exclude_all)]
    return df[~(df["country_iso3"] + df["version"]).isin(iso3_exclude_version)]


def refactor(output_file: Path) -> None:
    """Refactor file."""
    df = read_parquet(output_file)
    df["country_name"] = df["country_name"].str.replace("\u2019", "'", regex=False)
    df["admin_level_full"] = df["admin_level_full"].astype("Int32")
    for iso3, level in admin_level_full_overrides.items():
        df.loc[df["country_iso3"] == iso3, "admin_level_full"] = level
    for iso3, date in date_valid_on_overrides.items():
        mask = (df["country_iso3"] == iso3) & df["date_valid_on"].isna()
        df.loc[mask, "date_valid_on"] = pd.Timestamp(date)
    df[count_columns] = df[count_columns].astype("Int32")
    df = _df_filter(df)
    df = df[columns].sort_values(by=["country_iso3", "version"])
    save_metadata(output_file, df)
    output_file.unlink()
