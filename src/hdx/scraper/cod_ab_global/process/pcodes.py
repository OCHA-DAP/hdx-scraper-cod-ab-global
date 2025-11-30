from pathlib import Path

from pandas import DataFrame, concat, read_parquet

ADMIN_2 = 2

headers_pcodes = {
    "Location": ["#country+code"],
    "Admin Level": ["#geo+admin_level"],
    "P-Code": ["#adm+code"],
    "Name": ["#adm+name"],
    "Parent P-Code": ["#adm+code+parent"],
    "Valid from date": ["#date+start"],
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


def get_adm0_pcode_lenths(data_dir: Path) -> DataFrame:
    """Generate a global p-code length list."""
    df = read_parquet(
        data_dir / "global" / "original" / "latest" / "admin0.parquet",
        columns=["adm0_pcode", "iso3"],
    ).rename(columns={"iso3": "Location"})
    df["Country Length"] = df["adm0_pcode"].str.len()
    return df[["Location", "Country Length"]]


def generate_pcode_lengths(data_dir: Path, pcodes_dir: Path, df: DataFrame) -> None:
    """Generate a global p-code length list."""
    df = df[
        df.apply(
            lambda x: x["Admin Level"] == 1
            or x["P-Code"].startswith(x["Parent P-Code"]),
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
        .pivot(index="Location", columns="Admin Level", values="P-Code Length")
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
    df_country = get_adm0_pcode_lenths(data_dir)
    df_lengths = df_lengths.merge(df_country, on="Location", how="left")
    df_lengths = df_lengths[headers_lengths.keys()]
    df_lengths.to_parquet(
        pcodes_dir / "global_pcode_lengths.parquet",
        index=False,
        compression_level=15,
        compression="zstd",
    )
    concat([DataFrame(headers_lengths), df_lengths]).to_csv(
        pcodes_dir / "global_pcode_lengths.csv",
        index=False,
        encoding="utf-8-sig",
    )


def create_pcodes(data_dir: Path) -> None:
    """Generate a global p-code list."""
    df_all = DataFrame()
    pcodes_dir = data_dir / "pcodes"
    pcodes_dir.mkdir(parents=True, exist_ok=True)
    for level in range(1, 6):
        name_columns = [
            f"adm{level}_name",
            f"adm{level}_name1",
            f"adm{level}_name2",
            f"adm{level}_name3",
        ]
        df = read_parquet(
            data_dir / "global" / "original" / "latest" / f"admin{level}.parquet",
            columns=[
                *name_columns,
                f"adm{level}_pcode",
                f"adm{level - 1}_pcode",
                "iso3",
                "valid_on",
            ],
        )
        df["Admin Level"] = level
        df["Name"] = df[name_columns].bfill(axis=1).iloc[:, 0]
        df["Parent P-Code"] = df[f"adm{level - 1}_pcode"]
        rename_columns = {
            "iso3": "Location",
            f"adm{level}_pcode": "P-Code",
            "valid_on": "Valid from date",
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
    generate_pcode_lengths(data_dir, pcodes_dir, df_all)
    df_all["Parent P-Code"] = df_all.apply(
        lambda x: x["Parent P-Code"] if x["Admin Level"] > 1 else x["Location"],
        axis=1,
    )
    df_all.to_parquet(
        pcodes_dir / "global_pcodes.parquet",
        index=False,
        compression_level=15,
        compression="zstd",
    )
    concat([DataFrame(headers_pcodes), df_all]).to_csv(
        pcodes_dir / "global_pcodes.csv",
        index=False,
        encoding="utf-8-sig",
    )
    df_all = df_all[df_all["Admin Level"] <= ADMIN_2]
    df_all.to_parquet(
        pcodes_dir / "global_pcodes_adm_1_2.parquet",
        index=False,
        compression_level=15,
        compression="zstd",
    )
    concat([DataFrame(headers_pcodes), df_all]).to_csv(
        pcodes_dir / "global_pcodes_adm_1_2.csv",
        index=False,
        encoding="utf-8-sig",
    )
