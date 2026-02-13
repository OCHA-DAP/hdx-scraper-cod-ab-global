from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

from ..config import UPDATED_BY_SCRIPT

cwd = Path(__file__).parent

dataset_info = {
    "name": "global-pcodes",
    "title": "Global P-code List",
    "notes": (
        "CSV containing subnational p-codes, their corresponding administrative names, "
        "parent p-codes, and reference dates for the world (where available). "
        "Latin names are used where available."
    ),
    "methodology_other": (
        "P-codes taken from the latest administrative boundary layers available on the "
        "OCHA ISS ArcGIS server (gis.unocha.org)."
    ),
}

resources = [
    {
        "name": "global_pcodes.csv",
        "description": (
            "Table contains the 3-digit ISO code, admin level, p-code, "
            "administrative name, parent p-code, and date."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcodes_adm_1_2.csv",
        "description": (
            "Data for admin levels 1 and 2. Table contains the 3-digit ISO "
            "code, admin level, p-code, administrative name, parent p-code, and date."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcode_lengths.csv",
        "description": "P-code lengths for all countries at all levels.",
    },
]


def _initialize_dataset(data_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        data_dir / "pcodes/global_pcodes.parquet",
        columns=["Valid from date"],
    )
    start_date = df["Valid from date"].min().isoformat()
    end_date = datetime.now(tz=UTC).date().isoformat()
    dataset = Dataset(dataset_info)
    dataset.update_from_yaml(path=str(cwd / "../config/hdx_dataset_static.yaml"))
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions", "hxl"])
    dataset.set_time_period(start_date, end_date)
    return dataset


def _add_resources(data_dir: Path, dataset: Dataset) -> Dataset:
    """Add resources to a dataset."""
    for resource_data in resources:
        resource = Resource(resource_data)
        resource.set_file_to_upload(str(data_dir / "pcodes" / resource["name"]))
        resource.set_format("CSV")
        dataset.add_update_resource(resource)
    return dataset


def create_pcodes_dataset(data_dir: Path, info: dict) -> None:
    """Create a dataset for the world."""
    dataset = _initialize_dataset(data_dir)
    dataset = _add_resources(data_dir, dataset)
    dataset.create_in_hdx(
        remove_additional_resources=True,
        match_resource_order=True,
        hxl_update=False,
        updated_by_script=UPDATED_BY_SCRIPT,
        batch=info["batch"],
    )
    rmtree(data_dir / "pcodes")
