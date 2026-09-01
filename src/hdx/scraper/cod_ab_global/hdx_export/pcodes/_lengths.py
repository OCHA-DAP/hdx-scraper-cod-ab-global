"""Derive the global p-code length list (one row per country/admin level)."""

from pathlib import Path

import duckdb
from pandas import DataFrame

from ._content import headers_lengths
from ._io import read_level, save_outputs


def _get_adm0_pcode_lengths(
    original_dir: Path, con: duckdb.DuckDBPyConnection
) -> DataFrame:
    """Generate a global p-code length list."""
    df = read_level(original_dir, 0, ["adm0_pcode", "iso3"], con)
    df = df.rename(columns={"iso3": "Location"})
    df["Country Length"] = df["adm0_pcode"].str.len()
    return df[["Location", "Country Length"]]


def generate_pcode_lengths(
    original_dir: Path, pcodes_dir: Path, df: DataFrame, con: duckdb.DuckDBPyConnection
) -> None:
    """Generate a global p-code length list."""
    df = df[
        df.apply(
            lambda x: (
                x["Admin Level"] == 1 or x["P-Code"].startswith(x["Parent P-Code"])
            ),
            axis=1,
        )
    ].copy()
    df["P-Code Length"] = df.apply(
        lambda x: len(x["P-Code"]) - len(x["Parent P-Code"]),
        axis=1,
    )
    df_lengths = (
        df.groupby(["Location", "Admin Level"])["P-Code Length"]
        .apply(lambda x: "|".join([str(i) for i in sorted(x.unique())]))
        .reset_index()
        .pivot_table(
            index="Location",
            columns="Admin Level",
            values="P-Code Length",
            aggfunc="first",
        )
        .reset_index()
        .rename(
            columns={
                1: "Admin 1 Length",
                2: "Admin 2 Length",
                3: "Admin 3 Length",
                4: "Admin 4 Length",
                5: "Admin 5 Length",
            },
        )
    )
    df_country = _get_adm0_pcode_lengths(original_dir, con)
    df_lengths = df_lengths.merge(df_country, on="Location", how="left")
    df_lengths = df_lengths.reindex(columns=headers_lengths.keys())
    save_outputs(pcodes_dir, "global_pcode_lengths", headers_lengths, df_lengths)
