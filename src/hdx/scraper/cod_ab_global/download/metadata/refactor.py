from pathlib import Path

from hdx.location.country import Country
from pandas import DataFrame, concat, read_parquet

from ...config import iso3_exclude, iso3_include
from ...utils import save_metadata

ISO3_LEN = 3

column_rename = {
    "date_valid_from": "date_valid_on",
    "caveates": "caveats",
}

contributor_updates = {
    "IRQ": "OCHA Middle East and North Africa (ROMENA)",
}

admin_level_full_updates = [
    ("PHL", "v03", 3),
]

extra_rows = [
    {"country_iso3": "CUB", "version": "v01", "admin_level_max": 2},
]


name_columns = [
    "admin_1_name",
    "admin_2_name",
    "admin_3_name",
    "admin_4_name",
    "admin_5_name",
]

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


def merge_unique(df1: DataFrame, df2: DataFrame, columns: list[str]) -> DataFrame:
    """Merge two dataframes and keep only unique rows from df1."""
    merged_df = df1.merge(df2[columns], on=columns, how="left", indicator=True)
    return merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])


def df_filter(df: DataFrame) -> DataFrame:
    """Filter DataFrame from iso3 include and exclude lists."""
    iso3_include_all = [x for x in iso3_include if len(x) == ISO3_LEN]
    iso3_include_version = [x.replace("_V", "v") for x in iso3_include if "_V" in x]
    iso3_exclude_all = [x for x in iso3_exclude if len(x) == ISO3_LEN]
    iso3_exclude_version = [x.replace("_V", "v") for x in iso3_exclude if "_V" in x]
    if len(iso3_include):
        df = df[df["country_iso3"].isin(iso3_include_all)]
        df = df[(df["country_iso3"] + df["version"]).isin(iso3_include_version)]
    df = df[~df["country_iso3"].isin(iso3_exclude_all)]
    return df[~(df["country_iso3"] + df["version"]).isin(iso3_exclude_version)]


def refactor(output_file: Path) -> None:
    """Refactor file."""
    df = read_parquet(output_file)
    df = df.rename(columns=column_rename)
    df_extra = merge_unique(DataFrame(extra_rows), df, ["country_iso3", "version"])
    df = concat([df, df_extra], ignore_index=True)
    for key, value in contributor_updates.items():
        df.loc[df["country_iso3"] == key, "contributor"] = value
    df["country_name"] = df["country_iso3"].apply(Country.get_country_name_from_iso3)
    df["country_iso2"] = df["country_iso3"].apply(Country.get_iso2_from_iso3)
    df[name_columns] = df[name_columns].replace("currently not known", None)
    df["admin_level_full"] = df["admin_level_full"].replace("Unknown", None)
    df["admin_level_full"] = df["admin_level_full"].fillna(
        df["admin_level_max"].astype("string"),
    )
    df["admin_level_full"] = df["admin_level_full"].astype("Int32")
    for iso3, version, level in admin_level_full_updates:
        df.loc[
            (df["country_iso3"] == iso3) & (df["version"] == version),
            "admin_level_full",
        ] = level
    df[count_columns] = df[count_columns].astype("Int32")
    df = df[df["version"] != ""]
    df = df[df["admin_level_max"].gt(0)]
    df = df_filter(df)
    df = df[columns].sort_values(by=["country_iso3", "version"])
    save_metadata(output_file, df)
    output_file.unlink()
