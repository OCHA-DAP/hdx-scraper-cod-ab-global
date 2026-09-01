"""Generate the global P-codes dataset from the portolan catalog (latest services only).

Reads `original.parquet` per included "latest" service from the portolan
catalog, injecting the `iso3` literal via DuckDB.
"""

from pathlib import Path

import duckdb
from pandas import DataFrame, concat

from ._content import ADMIN_2, headers_pcodes
from ._io import read_level, save_outputs
from ._lengths import generate_pcode_lengths


def _save_pcodes(pcodes_dir: Path, df_all: DataFrame) -> None:
    """Save global p-code list."""
    save_outputs(pcodes_dir, "global_pcodes", headers_pcodes, df_all)
    df_all = df_all[df_all["Admin Level"] <= ADMIN_2]
    save_outputs(pcodes_dir, "global_pcodes_adm_1_2", headers_pcodes, df_all)


def build_pcodes(original_dir: Path, output_dir: Path) -> Path:
    """Generate the global p-code list. Returns the pcodes output directory."""
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        df_all = DataFrame()
        pcodes_dir = output_dir / "pcodes"
        pcodes_dir.mkdir(parents=True, exist_ok=True)
        for level in range(1, 6):
            name_columns = [
                f"adm{level}_name",
                f"adm{level}_name1",
                f"adm{level}_name2",
                f"adm{level}_name3",
            ]
            columns = [
                *name_columns,
                f"adm{level}_pcode",
                f"adm{level - 1}_pcode",
                "iso3",
                "valid_on",
                "version",
            ]
            df = read_level(original_dir, level, columns, con)
            df["Admin Level"] = level
            df["Name"] = df[name_columns].bfill(axis=1).iloc[:, 0]
            df["Parent P-Code"] = df[f"adm{level - 1}_pcode"]
            rename_columns = {
                "iso3": "Location",
                f"adm{level}_pcode": "P-Code",
                "valid_on": "Valid from date",
                "version": "Version",
            }
            df = df.rename(columns=rename_columns)
            df = df[headers_pcodes.keys()]
            df = df[df["P-Code"].notna()]
            df = df[df["P-Code"].str.contains(r"\d")]
            if not df_all.empty:
                df = df[df["Parent P-Code"].isin(df_all["P-Code"])]
            df_all = concat([df_all, df]) if not df_all.empty else df
            df_all = (
                df_all.sort_values(by=["Location", "Admin Level", "P-Code", "Name"])
                .drop_duplicates()
                .drop_duplicates(subset=["P-Code"], keep=False)
            )
        generate_pcode_lengths(original_dir, pcodes_dir, df_all, con)
        df_all["Parent P-Code"] = df_all.apply(
            lambda x: x["Parent P-Code"] if x["Admin Level"] > 1 else x["Location"],
            axis=1,
        )
        _save_pcodes(pcodes_dir, df_all)
    finally:
        con.close()
    return pcodes_dir
