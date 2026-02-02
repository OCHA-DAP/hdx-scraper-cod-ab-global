from pathlib import Path
from shutil import rmtree

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import wheretostart_tempdir_batch

from .config import run_versions
from .dataset.boundaries import create_boundaries_dataset
from .dataset.pcodes import create_pcodes_dataset
from .download.admin0 import download_admin0
from .download.boundaries import download_boundaries
from .download.metadata import download_metadata
from .edge_extender import edge_extender
from .process.boundaries import create_boundaries
from .process.extended_post import postprocess_extended
from .process.extended_pre import preprocess_extended
from .process.matched import create_matched
from .process.pcodes import create_pcodes
from .utils import generate_token

cwd = Path(__file__).parent

_USER_AGENT_LOOKUP = "hdx-scraper-cod-global"
_SAVED_DATA_DIR = (cwd / "../../../../saved_data").resolve()


def main(save: bool = True, use_saved: bool = False) -> None:  # noqa: FBT001, FBT002
    """Generate datasets and create them in HDX."""
    Configuration.read()
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        data_dir = Path(_SAVED_DATA_DIR if save or use_saved else temp_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        for run_version in run_versions:
            token = generate_token()
            download_admin0(data_dir, token)
            download_metadata(data_dir, token)
            download_boundaries(data_dir, token, run_version)
            if run_version == "latest":
                create_pcodes(data_dir)
                create_pcodes_dataset(data_dir, info)
            create_boundaries(data_dir, run_version, "original")
            preprocess_extended(data_dir)
            edge_extender(data_dir)
            postprocess_extended(data_dir)
            create_boundaries(data_dir, run_version, "extended")
            create_matched(data_dir)
            create_boundaries(data_dir, run_version, "matched")
            create_boundaries_dataset(data_dir, run_version, info)
            rmtree(data_dir)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
    )
