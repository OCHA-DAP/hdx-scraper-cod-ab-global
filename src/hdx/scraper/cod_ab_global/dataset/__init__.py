"""HDX dataset creation for COD-AB global boundaries and P-codes."""

from pathlib import Path

from hdx.data.dataset import Dataset

cwd = Path(__file__).parent


def base_dataset(info: dict) -> Dataset:
    """Build a Dataset with the bootstrap shared by boundaries/ and pcodes/."""
    dataset = Dataset(info)
    dataset.update_from_yaml(path=cwd / "../config/hdx_dataset_static.yaml")
    dataset.add_other_location("world")
    dataset.add_tags(["administrative boundaries-divisions"])
    return dataset
