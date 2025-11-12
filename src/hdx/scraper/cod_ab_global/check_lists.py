from pathlib import Path

from pandas import read_parquet


def check_lists(data_dir: Path) -> None:
    """Check metadata list against file path list."""
    df = read_parquet(data_dir / "metadata.parquet")
    metadata_set = set(
        "cod_ab_" + df["country_iso3"].str.lower() + "_" + df["version"],
    )
    boundary_set = {
        x.name for x in sorted((data_dir / "country" / "original").glob("cod_ab_*"))
    }
    missing_metadata = sorted(boundary_set.difference(metadata_set))
    missing_boundaries = sorted(metadata_set.difference(boundary_set))
    if missing_metadata and missing_boundaries:
        err_msg = f"Missing metadata: {missing_metadata}, "
        "Missing boundaries: {missing_boundaries}"
        raise ValueError(err_msg)
    if missing_metadata:
        err_msg = f"Missing metadata: {missing_metadata}"
        raise ValueError(err_msg)
    if missing_boundaries:
        err_msg = f"Missing boundaries: {missing_boundaries}"
        raise ValueError(err_msg)
