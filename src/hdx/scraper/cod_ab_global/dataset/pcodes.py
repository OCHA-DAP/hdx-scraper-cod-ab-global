"""Create and upload the global P-codes dataset to HDX."""

from datetime import UTC, datetime
from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from pandas import read_parquet

from hdx.scraper.cod_ab_global.config import UPDATED_BY_SCRIPT

cwd = Path(__file__).parent

dataset_info = {
    "name": "global-pcodes",
    "title": "Global P-code List",
    "notes": (
        "CSV containing subnational p-codes, their corresponding administrative names, "
        "parent p-codes, and reference dates for the world (where available). "
        "Latin names are used where available."
        "\n  \n  "
        "For actual boundaries: [Global Subnational Administrative Boundaries](https://data.humdata.org/dataset/cod-ab-global)"
    ),
    "methodology_other": (
        "P-codes taken from the latest administrative boundary layers available on the "
        "OCHA ArcGIS server (gis.unocha.org)."
    ),
    "caveats": (
        "There may be a delay of a few days between when new country boundaries "
        "are added to HDX and when they are aggregated into this global dataset."
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
    {
        "name": "global_pcodes_hxl.csv",
        "description": (
            "Table contains the 3-digit ISO code, admin level, p-code, "
            "administrative name, parent p-code, and date. Includes HXL hashtags."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcodes_adm_1_2_hxl.csv",
        "description": (
            "Data for admin levels 1 and 2. Table contains the 3-digit ISO code, "
            "admin level, p-code, administrative name, parent p-code, and date. "
            "Includes HXL hashtags."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcode_lengths_hxl.csv",
        "description": (
            "P-code lengths for all countries at all levels. Includes HXL hashtags."
        ),
    },
]


def _initialize_dataset(output_dir: Path) -> Dataset:
    """Initialize a dataset."""
    df = read_parquet(
        output_dir / "pcodes/global_pcodes.parquet",
        columns=["Valid from date"],
    )
    start_date = df["Valid from date"].min().isoformat()
    end_date = datetime.now(tz=UTC).date().isoformat()
    dataset = Dataset(dataset_info)
    dataset.update_from_yaml(path=str(cwd / "../config/hdx_dataset_static.yaml"))
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions"])
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

    Unlike the old pipeline, does not delete `output_dir/pcodes/` afterward —
    hdx_export's fingerprint-based skip (portolan/hdx_export/state.py) needs
    the output to persist on disk so a later unchanged run can detect there's
    nothing to rebuild.
    """
    dataset = _initialize_dataset(output_dir)
    dataset = _add_resources(output_dir, dataset)
    dataset.create_in_hdx(
        remove_additional_resources=True,
        match_resource_order=True,
        updated_by_script=UPDATED_BY_SCRIPT,
        batch=info["batch"],
    )
