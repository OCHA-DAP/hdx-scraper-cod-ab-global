from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

cwd = Path(__file__).parent

dataset_info = {
    "name": "cod-ab-global",
    "title": "OCHA Global Subnational Administrative Boundaries",
    "notes": "Geodatabase containing subnational boundaries.",
}

resources = [
    {
        "name": "global_admin_boundaries_matched_latest.gdb.zip",
        "description": (
            "Edge-matched geometry (no gaps or overlaps), latest versions only."
        ),
    },
    {
        "name": "global_admin_boundaries_original_latest.gdb.zip",
        "description": (
            "Original geometry (with gaps and overlaps), latest versions only."
        ),
    },
    {
        "name": "global_admin_boundaries_extended_latest.gdb.zip",
        "description": "Extended geometry (pre-edge-matching), latest versions only.",
    },
]

resources_all = [
    {
        "name": "global_admin_boundaries_matched_all.gdb.zip",
        "description": (
            "Edge-matched geometry (no gaps or overlaps), "
            "all versions (current and historic)."
        ),
    },
    {
        "name": "global_admin_boundaries_original_all.gdb.zip",
        "description": (
            "Original geometry (with gaps and overlaps), "
            "all versions (current and historic)."
        ),
    },
    {
        "name": "global_admin_boundaries_extended_all.gdb.zip",
        "description": (
            "Extended geometry (pre-edge-matching), "
            "all versions (current and historic)."
        ),
    },
]


def initialize_dataset(data_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        data_dir / "metadata/global_admin_boundaries_metadata_latest.parquet",
        columns=["date_valid_on", "date_reviewed"],
    )
    start_date = df["date_valid_on"].min().isoformat()
    end_date = df[df["date_reviewed"].notna()]["date_reviewed"].max().isoformat()
    dataset = Dataset(dataset_info)
    dataset.update_from_yaml(path=str(cwd / "../config/hdx_dataset_static.yaml"))
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions"])
    dataset.set_time_period(start_date, end_date)
    return dataset


def add_resources(data_dir: Path, dataset: Dataset, resource_data: dict) -> Dataset:
    """Add resources to a dataset."""
    processing = resource_data["name"].replace(".", "_").split("_")[3]
    extent = resource_data["name"].replace(".", "_").split("_")[4]
    resource_data["p_coded"] = "True"
    resource = Resource(resource_data)
    resource.set_file_to_upload(
        str(data_dir / "global" / processing / extent / resource["name"]),
    )
    resource.set_format("Geodatabase")
    dataset.add_update_resource(resource)
    return dataset


def add_metadata(data_dir: Path, dataset: Dataset) -> Dataset:
    """Add resources to a dataset."""
    resource_data = {
        "name": "global_admin_boundaries_metadata_latest.csv",
        "description": "Associated metadata for administrative boundaries.",
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(str(data_dir / "metadata" / resource_data["name"]))
    resource.set_format("CSV")
    dataset.add_update_resource(resource)
    return dataset


def dataset_create_in_hdx(dataset: Dataset, info: dict, script_name: str) -> None:
    """Create a dataset in HDX."""
    dataset.create_in_hdx(
        remove_additional_resources=False,
        match_resource_order=True,
        hxl_update=False,
        updated_by_script=script_name,
        batch=info["batch"],
    )


def create_boundaries_dataset(data_dir: Path, info: dict, script_name: str) -> None:
    """Create a dataset for the world."""
    dataset = initialize_dataset(data_dir)
    for resource in resources:
        dataset = add_resources(data_dir, dataset, resource)
        dataset_create_in_hdx(dataset, info, script_name)
    dataset = add_metadata(data_dir, dataset)
    dataset_create_in_hdx(dataset, info, script_name)
