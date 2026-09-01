"""Attach resources to a boundaries Dataset and create it in HDX."""

from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

from hdx.scraper.cod_ab_global.config import UPDATED_BY_SCRIPT


def add_resources(output_dir: Path, dataset: Dataset, resource_data: dict) -> Dataset:
    """Add resources to a dataset.

    Uploads whatever's currently in `output_dir` directly, hdx_export's own
    fingerprint check already ensures this is only reached when it changed.
    """
    resource_data["p_coded"] = "True"
    resource = Resource(resource_data)
    resource.set_file_to_upload(output_dir / resource["name"])
    resource.set_format("Geodatabase")
    dataset.add_update_resource(resource)
    return dataset


def add_metadata_resource(
    output_dir: Path,
    run_version: str,
    dataset: Dataset,
) -> Dataset:
    """Add resources to a dataset."""
    resource_data = {
        "name": f"global_admin_boundaries_metadata_{run_version}.csv",
        "description": "Associated metadata for administrative boundaries.",
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(output_dir / "metadata" / resource_data["name"])
    resource.set_format("CSV")
    dataset.add_update_resource(resource)
    return dataset


def dataset_create_in_hdx(dataset: Dataset, info: dict) -> None:
    """Create a dataset in HDX."""
    dataset.create_in_hdx(
        remove_additional_resources=False,
        match_resource_order=False,
        updated_by_script=UPDATED_BY_SCRIPT,
        batch=info["batch"],
    )
