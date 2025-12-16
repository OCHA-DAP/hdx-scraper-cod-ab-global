from pathlib import Path
from shutil import rmtree

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

from ..config import UPDATED_BY_SCRIPT

cwd = Path(__file__).parent


def get_dataset_info(run_version: str) -> dict:
    """Get dataset info for a dataset."""
    name_extra = ""
    title_extra = ""
    if run_version != "latest":
        name_extra = f"-{run_version}"
        title_extra = f" ({run_version.title()})"
    return {
        "name": f"cod-ab-global{name_extra}",
        "title": f"OCHA Global Subnational Administrative Boundaries{title_extra}",
        "methodology_other": (
            f"Data taken from {run_version} administrative boundary layers available "
            "on the UN OCHA FIS ArcGIS server (gis.unocha.org). Edge-extending of "
            "original geometries done using an algorithm "
            "(github.com/fieldmaps/edge-extender). "
            "Edge-matching done using UN Geodata 1:1M international boundaries "
            "(geohub.un.org)."
        ),
        "caveats": (
            "In the extended and edge-matched resources, lower levels are filled in "
            "with higher ones if they don't exist. Example: Admin 2 is used to fill in "
            "Admin 3 and 4 if they don't exist. Also, only layers with full coverage "
            "are used for these two resources. Example: if Admin 3 only covers part of "
            "a location, Admin 2 is used instead."
        ),
    }


def get_resource(run_version: str, stage: str) -> dict:
    """Get resource info for a dataset."""
    resources = {
        "matched": {
            "name": f"global_admin_boundaries_matched_{run_version}.gdb.zip",
            "description": (
                "Edge-matched geometry (no gaps or overlaps), "
                f"{run_version} versions only."
            ),
        },
        "original": {
            "name": f"global_admin_boundaries_original_{run_version}.gdb.zip",
            "description": (
                "Original geometry (with gaps and overlaps), "
                f"{run_version} versions only."
            ),
        },
        "extended": {
            "name": f"global_admin_boundaries_extended_{run_version}.gdb.zip",
            "description": (
                f"Extended geometry (pre-edge-matching), {run_version} versions only."
            ),
        },
    }
    return resources[stage]


def get_notes(admin_count: int, run_version: str) -> str:
    """Get notes for a dataset."""
    other_link = (
        "[historic boundaries](https://data.humdata.org/dataset/cod-ab-global-historic)"
        if run_version == "latest"
        else "[latest boundaries](https://data.humdata.org/dataset/cod-ab-global)"
    )
    return (
        "Global administrative level 0-4 boundaries (COD-AB) dataset for "
        f"{admin_count} countries / territories, {run_version} versions."
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
        "  \n  \n"
        f"A version of this dataset is also available with {other_link}."
    )


def initialize_dataset(data_dir: Path, run_version: str) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        data_dir / f"metadata/global_admin_boundaries_metadata_{run_version}.parquet",
        columns=["date_valid_on", "date_reviewed"],
    )
    start_date = df[df["date_valid_on"].notna()]["date_valid_on"].min().isoformat()
    end_date = df[df["date_reviewed"].notna()]["date_reviewed"].max().isoformat()
    layer_count = len(df)
    dataset_info = get_dataset_info(run_version)
    dataset_info["notes"] = get_notes(layer_count, run_version)
    dataset = Dataset(dataset_info)
    dataset.update_from_yaml(path=str(cwd / "../config/hdx_dataset_static.yaml"))
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions"])
    dataset.set_time_period(start_date, end_date)
    return dataset


def add_resources(data_dir: Path, dataset: Dataset, resource_data: dict) -> Dataset:
    """Add resources to a dataset."""
    resource_data["p_coded"] = "True"
    resource = Resource(resource_data)
    resource.set_file_to_upload(str(data_dir / "global" / resource["name"]))
    resource.set_format("Geodatabase")
    dataset.add_update_resource(resource)
    return dataset


def add_metadata_resource(
    data_dir: Path,
    run_version: str,
    dataset: Dataset,
) -> Dataset:
    """Add resources to a dataset."""
    resource_data = {
        "name": f"global_admin_boundaries_metadata_{run_version}.csv",
        "description": "Associated metadata for administrative boundaries.",
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(str(data_dir / "metadata" / resource_data["name"]))
    resource.set_format("CSV")
    dataset.add_update_resource(resource)
    return dataset


def dataset_create_in_hdx(dataset: Dataset, info: dict) -> None:
    """Create a dataset in HDX."""
    dataset.create_in_hdx(
        remove_additional_resources=False,
        match_resource_order=False,
        hxl_update=False,
        updated_by_script=UPDATED_BY_SCRIPT,
        batch=info["batch"],
    )


def create_boundaries_dataset(
    data_dir: Path,
    run_version: str,
    stage: str,
    info: dict,
) -> None:
    """Create a dataset for the world."""
    dataset = initialize_dataset(data_dir, run_version)
    resource = get_resource(run_version, stage)
    dataset = add_resources(data_dir, dataset, resource)
    if stage == "matched":
        dataset = add_metadata_resource(data_dir, run_version, dataset)
    dataset_create_in_hdx(dataset, info)
    rmtree(data_dir / "global")
    if stage == "matched":
        rmtree(data_dir / "metadata")
        (data_dir / "bnda_cty.parquet").unlink()
