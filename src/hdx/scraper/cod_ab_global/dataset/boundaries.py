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
        "name": "global_admin_boundaries_original_latest.gdb.zip",
        "description": (
            "Original geometry (with gaps and overlaps), latest versions only."
        ),
    },
    {
        "name": "global_admin_boundaries_original_all.gdb.zip",
        "description": (
            "Original geometry (with gaps and overlaps), "
            "all versions (current and historic)."
        ),
    },
]


def initialize_dataset(data_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        data_dir / "metadata_latest.parquet",
        columns=["date_valid_on", "date_reviewed"],
    )
    start_date = df["date_valid_on"].min().isoformat()
    end_date = df["date_reviewed"].max().isoformat()
    dataset = Dataset(dataset_info)
    dataset.update_from_yaml(path=str(cwd / "../config/hdx_dataset_static.yaml"))
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions"])
    dataset.set_time_period(start_date, end_date)
    return dataset


def add_resources(data_dir: Path, dataset: Dataset) -> Dataset:
    """Add resources to a dataset."""
    for resource_data in resources:
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


def create_boundaries_dataset(data_dir: Path, info: dict, script_name: str) -> None:
    """Create a dataset for the world."""
    dataset = initialize_dataset(data_dir)
    dataset = add_resources(data_dir, dataset)
    dataset.create_in_hdx(
        remove_additional_resources=True,
        match_resource_order=False,
        hxl_update=False,
        updated_by_script=script_name,
        batch=info["batch"],
    )
