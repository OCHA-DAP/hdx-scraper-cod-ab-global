"""Create and upload the global P-codes dataset to HDX."""

from datetime import UTC, datetime
from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

from hdx.scraper.cod_ab_global.config import UPDATED_BY_SCRIPT
from hdx.scraper.cod_ab_global.dataset import base_dataset

from ._content import dataset_info, resources


def _initialize_dataset(output_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        output_dir / "pcodes/global_pcodes.parquet",
        columns=["Valid from date"],
    )
    start_date = df["Valid from date"].min().isoformat()
    end_date = datetime.now(tz=UTC).date().isoformat()
    dataset = base_dataset(dataset_info)
    dataset.set_time_period(start_date, end_date)
    return dataset


def _add_resources(output_dir: Path, dataset: Dataset) -> Dataset:
    """Add resources to a dataset."""
    for resource_data in resources:
        resource = Resource(resource_data)
        resource.set_file_to_upload(str(output_dir / "pcodes" / resource["name"]))
        resource.set_format("CSV")
        dataset.add_update_resource(resource)
    return dataset


def create_pcodes_dataset(output_dir: Path, info: dict) -> None:
    """Create a dataset for the world.

    Does not delete `output_dir/pcodes/` afterward, since hdx_export's
    fingerprint-based skip needs the output to persist for later runs.
    """
    dataset = _initialize_dataset(output_dir)
    dataset = _add_resources(output_dir, dataset)
    dataset.create_in_hdx(
        remove_additional_resources=True,
        match_resource_order=True,
        updated_by_script=UPDATED_BY_SCRIPT,
        batch=info["batch"],
    )
