"""Read admin-level layers from the catalog and save p-code output files."""

from pathlib import Path

import duckdb
from pandas import DataFrame, concat

from hdx.scraper.cod_ab_global.hdx_export.services import resolve_services


def save_outputs(pcodes_dir: Path, stem: str, headers: dict, df: DataFrame) -> None:
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


def read_level(
    original_dir: Path, level: int, columns: list[str], con: duckdb.DuckDBPyConnection
) -> DataFrame:
    """Read one admin level across all included latest services, iso3 injected."""
    services = resolve_services(original_dir, "latest")
    cols_str = ", ".join(c for c in columns if c != "iso3")
    selects = []
    for iso3, version_dirs in services.items():
        layer_name = f"{iso3}_admin{level}"
        parquet_path = version_dirs[0] / layer_name / f"{layer_name}.parquet"
        if not parquet_path.exists():
            continue
        selects.append(
            f"SELECT {cols_str}, '{iso3.upper()}' AS iso3"
            f" FROM read_parquet('{parquet_path}')"
        )
    if not selects:
        return DataFrame(columns=columns)
    return con.execute("\nUNION ALL\n".join(selects)).df()
