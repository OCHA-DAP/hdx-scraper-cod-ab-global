from pathlib import Path
from venv import logger

from hdx.location.country import Country
from pandas import DataFrame, concat, read_parquet

from ..utils import save_metadata


def fix_metadata(data_dir: Path, df: DataFrame, missing_metadata: list[str]) -> None:
    """Add missing boundaries to metadata."""
    boundary_dir = data_dir / "country" / "original"
    output_file = data_dir / "metadata/global_admin_boundaries_metadata.parquet"
    extra_rows = []
    for service in missing_metadata:
        iso3 = service.split("_")[2].upper()
        service_dir = boundary_dir / service
        admin_level_max = int(sorted(service_dir.glob("*.parquet"))[-1].stem[-1])
        row = {
            "country_name": Country.get_country_name_from_iso3(iso3),
            "country_iso2": Country.get_iso2_from_iso3(iso3),
            "country_iso3": iso3,
            "version": service.split("_")[-1],
            "admin_level_full": admin_level_max,
            "admin_level_max": admin_level_max,
        }
        for level in range(1, admin_level_max + 1):
            layer = service_dir / f"{iso3.lower()}_admin{level}.parquet"
            if layer.exists():
                row[f"admin_{level}_count"] = len(read_parquet(layer, columns=["iso3"]))
        extra_rows.append(row)
    df = concat([df, DataFrame(extra_rows)])
    df = df.sort_values(by=["country_iso3", "version"])
    save_metadata(output_file, df)


def check_lists(data_dir: Path) -> None:
    """Check metadata list against file path list."""
    df = read_parquet(
        data_dir / "metadata/global_admin_boundaries_metadata_all.parquet",
    )
    metadata_set = set(
        "cod_ab_" + df["country_iso3"].str.lower() + "_" + df["version"],
    )
    boundary_set = {
        x.name for x in sorted((data_dir / "country" / "original").glob("cod_ab_*"))
    }
    missing_metadata = sorted(boundary_set.difference(metadata_set))
    missing_boundaries = sorted(metadata_set.difference(boundary_set))
    if missing_metadata and missing_boundaries:
        err_msg = (
            f"Missing metadata: {missing_metadata}, "
            f"Missing boundaries: {missing_boundaries}"
        )
        logger.error(err_msg)
    if missing_metadata:
        err_msg = f"Missing metadata: {missing_metadata}"
        logger.error(err_msg)
    if missing_boundaries:
        err_msg = f"Missing boundaries: {missing_boundaries}"
        logger.error(err_msg)
    fix_metadata(data_dir, df, missing_metadata)
