"""Create and upload global administrative boundaries datasets to HDX."""

from pathlib import Path

from hdx.data.dataset import Dataset
from pandas import read_parquet

from hdx.scraper.cod_ab_global.dataset import base_dataset

from ._dataset_info import get_dataset_info, get_resource
from ._notes import get_notes
from ._resources import add_metadata_resource, add_resources, dataset_create_in_hdx


def _initialize_dataset(output_dir: Path, run_version: str) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        output_dir / f"metadata/global_admin_boundaries_metadata_{run_version}.parquet",
        columns=["date_valid_on", "date_reviewed"],
    )
    start_date = df[df["date_valid_on"].notna()]["date_valid_on"].min().isoformat()
    end_date = df[df["date_reviewed"].notna()]["date_reviewed"].max().isoformat()
    layer_count = len(df)
    dataset_info = get_dataset_info(run_version)
    dataset_info["notes"] = get_notes(layer_count, run_version)
    dataset = base_dataset(dataset_info)
    dataset.set_time_period(start_date, end_date)
    return dataset


def create_boundaries_dataset(output_dir: Path, run_version: str, info: dict) -> None:
    """Create a dataset for the world."""
    for stage in ["matched", "original", "extended"]:
        dataset = _initialize_dataset(output_dir, run_version)
        resource = get_resource(run_version, stage)
        dataset = add_resources(output_dir, dataset, resource)
        dataset_create_in_hdx(dataset, info)
    dataset = _initialize_dataset(output_dir, run_version)
    dataset = add_metadata_resource(output_dir, run_version, dataset)
    dataset_create_in_hdx(dataset, info)
