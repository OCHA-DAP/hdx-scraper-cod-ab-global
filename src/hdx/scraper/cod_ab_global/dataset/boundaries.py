from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

cwd = Path(__file__).parent

dataset_info = {
    "name": "cod-ab-global",
    "title": "OCHA Global Subnational Administrative Boundaries",
    "methodology_other": (
        "Data taken from the latest administrative boundary layers available on the "
        "UN OCHA FIS ArcGIS server (gis.unocha.org). Edge-extending of original "
        "geometries done using an algorithm (github.com/fieldmaps/edge-extender). "
        "Edge-matching done using UN Geodata 1:1M international boundaries "
        "(geoservices.un.org)."
    ),
    "caveats": (
        "In the extended and edge-matched resources, lower levels are filled in with "
        "higher ones if they don't exist. Example: Admin 2 is used to fill in Admin 3 "
        "and 4 if they don't exist. Also, only layers with full coverage are used for "
        "these two resources. Example: if Admin 3 only covers part of a location, "
        "Admin 2 is used instead."
    ),
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


def get_notes(admin_count: int) -> str:
    """Get notes for a dataset."""
    return (
        "Global administrative level 0-4 boundaries (COD-AB) dataset for "
        f"{admin_count} countries / territories, latest versions."
        "  \n  \n"
        "This is an aggregation of "
        "[subnational administrative boundaries](https://data.humdata.org/dataset/?dataseries_name=COD+-+Subnational+Administrative+Boundaries&cod_level=cod-enhanced)"
        " available in 3 variations:"
        "  \n  \n"
        "**Edge-Matched**: Subnational boundaries are aligned to fit UN Geodata 1:1M "
        "international boundaries. This process results in simplification at the "
        "international border, but gaps and overlaps are eliminated. The internal "
        "resolution of each subnational boundary layer is not modified by this "
        "process. Recommended to use for most use cases."
        "  \n  \n"
        "**Original**: Subnational boundaries are unmodified from their original "
        "source. There will be gaps and overlaps at the international border. "
        "Recomended if maintaining the integrity of the initial source is important."
        "  \n  \n"
        "**Extended**: A specialty output to help those performing their own "
        "edge-matching when not using UN Geodata 1:1M international boundaries."
        "  \n  \n"
        "Metadata about sources used is also available as a table."
    )


def initialize_dataset(data_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        data_dir / "metadata/global_admin_boundaries_metadata_latest.parquet",
        columns=["date_valid_on", "date_reviewed"],
    )
    start_date = df[df["date_valid_on"].notna()]["date_valid_on"].min().isoformat()
    end_date = df[df["date_reviewed"].notna()]["date_reviewed"].max().isoformat()
    layer_count = len(df)
    dataset_info["notes"] = get_notes(layer_count)
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


def add_metadata_resource(data_dir: Path, dataset: Dataset) -> Dataset:
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
        match_resource_order=False,
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
    dataset = add_metadata_resource(data_dir, dataset)
    dataset_create_in_hdx(dataset, info, script_name)
