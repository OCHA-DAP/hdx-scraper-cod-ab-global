"""Generate the global P-codes dataset from the portolan catalog (latest services only).

Replaces process/pcodes.py. Same pcode-derivation algorithm as the old
module; only the input source changes — reads `original.parquet` per
included "latest" service from the portolan catalog instead of a throwaway
download tree, injecting the `iso3` literal (missing from portolan's raw
ArcGIS extracts) via DuckDB rather than pandas/GDAL.
"""

from pathlib import Path

import duckdb
from pandas import DataFrame, concat

from .services import resolve_services

ADMIN_2 = 2

# Reads original.parquet for latest-only services — depends on the same
# upstream field as the "original" boundaries stage. See boundaries.py's
# FINGERPRINT_KEYS for why this lives next to each module's own logic.
FINGERPRINT_KEY = "cod_ab:original_updated"

headers_pcodes = {
    "Location": ["#country+code"],
    "Admin Level": ["#geo+admin_level"],
    "P-Code": ["#adm+code"],
    "Name": ["#adm+name"],
    "Parent P-Code": ["#adm+code+parent"],
    "Valid from date": ["#date+start"],
    "Version": ["#meta+version"],
}

headers_lengths = {
    "Location": ["#country+code"],
    "Country Length": ["#country+len"],
    "Admin 1 Length": ["#adm1+len"],
    "Admin 2 Length": ["#adm2+len"],
    "Admin 3 Length": ["#adm3+len"],
    "Admin 4 Length": ["#adm4+len"],
    "Admin 5 Length": ["#adm5+len"],
}


def _save_outputs(pcodes_dir: Path, stem: str, headers: dict, df: DataFrame) -> None:
    """Save parquet, plain CSV, and HXL CSV for a dataframe."""
    df.to_parquet(
        pcodes_dir / f"{stem}.parquet",
        index=False,
        compression_level=15,
        compression="zstd",
    )
    df.to_csv(pcodes_dir / f"{stem}.csv", index=False, encoding="utf-8-sig")
    concat([DataFrame(headers), df]).to_csv(
        pcodes_dir / f"{stem}_hxl.csv", index=False, encoding="utf-8-sig"
    )


def _read_level(
    work_dir: Path, level: int, columns: list[str], con: duckdb.DuckDBPyConnection
) -> DataFrame:
    """Read one admin level across all included latest services, iso3 injected."""
    services = resolve_services(work_dir, "latest")
    cols_str = ", ".join(c for c in columns if c != "iso3")
    selects = []
    for iso3, version_dirs in services.items():
        parquet_path = version_dirs[0] / f"adm{level}" / "original.parquet"
        if not parquet_path.exists():
            continue
        selects.append(
            f"SELECT {cols_str}, '{iso3.upper()}' AS iso3"
            f" FROM read_parquet('{parquet_path}')"
        )
    if not selects:
        return DataFrame(columns=columns)
    return con.execute("\nUNION ALL\n".join(selects)).df()


def _get_adm0_pcode_lengths(
    work_dir: Path, con: duckdb.DuckDBPyConnection
) -> DataFrame:
    """Generate a global p-code length list."""
    df = _read_level(work_dir, 0, ["adm0_pcode", "iso3"], con)
    df = df.rename(columns={"iso3": "Location"})
    df["Country Length"] = df["adm0_pcode"].str.len()
    return df[["Location", "Country Length"]]


def _generate_pcode_lengths(
    work_dir: Path, pcodes_dir: Path, df: DataFrame, con: duckdb.DuckDBPyConnection
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
    df_country = _get_adm0_pcode_lengths(work_dir, con)
    df_lengths = df_lengths.merge(df_country, on="Location", how="left")
    df_lengths = df_lengths[headers_lengths.keys()]
    _save_outputs(pcodes_dir, "global_pcode_lengths", headers_lengths, df_lengths)


def _save_pcodes(pcodes_dir: Path, df_all: DataFrame) -> None:
    """Save global p-code list."""
    _save_outputs(pcodes_dir, "global_pcodes", headers_pcodes, df_all)
    df_all = df_all[df_all["Admin Level"] <= ADMIN_2]
    _save_outputs(pcodes_dir, "global_pcodes_adm_1_2", headers_pcodes, df_all)


def build_pcodes(work_dir: Path, output_dir: Path) -> Path:
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
            df = _read_level(work_dir, level, columns, con)
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
        _generate_pcode_lengths(work_dir, pcodes_dir, df_all, con)
        df_all["Parent P-Code"] = df_all.apply(
            lambda x: x["Parent P-Code"] if x["Admin Level"] > 1 else x["Location"],
            axis=1,
        )
        _save_pcodes(pcodes_dir, df_all)
    finally:
        con.close()
    return pcodes_dir
