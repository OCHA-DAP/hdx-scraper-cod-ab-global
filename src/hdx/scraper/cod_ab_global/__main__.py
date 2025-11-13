import logging
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import wheretostart_tempdir_batch

from .check_lists import check_lists
from .config import run_exclude, run_include
from .dataset.boundaries import create_boundaries_dataset
from .dataset.pcodes import create_pcodes_dataset
from .download.boundaries import main as download_boundaries
from .download.meta import main as download_meta
from .extended.preprocess import preprocess_extended
from .original.boundaries import create_original_boundaries
from .original.pcodes import create_pcodes
from .utils import generate_token

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

_USER_AGENT_LOOKUP = "hdx-scraper-cod-global"
_SAVED_DATA_DIR = (cwd / "../../../../saved_data").resolve()
_UPDATED_BY_SCRIPT = "HDX Scraper: COD-AB Global"


def can_run(run: str) -> bool:
    """Determine whether step can be run."""
    return (not run_include or run in run_include) and (
        not run_exclude or run not in run_exclude
    )


def main(save: bool = True, use_saved: bool = False) -> None:  # noqa: FBT001, FBT002
    """Generate datasets and create them in HDX."""
    Configuration.read()
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        data_dir = Path(_SAVED_DATA_DIR if save or use_saved else temp_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        if can_run("DOWNLOAD"):
            token = generate_token()
            download_meta(data_dir, token)
            download_boundaries(data_dir, token)
        if can_run("CHECK"):
            check_lists(data_dir)
        if can_run("ORIGINAL"):
            create_original_boundaries(data_dir)
            create_pcodes(data_dir)
        if can_run("EXTENDED_PRE"):
            preprocess_extended(data_dir)
        if can_run("DATASETS"):
            create_pcodes_dataset(data_dir, info, _UPDATED_BY_SCRIPT)
            create_boundaries_dataset(data_dir, info, _UPDATED_BY_SCRIPT)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="stage",
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
    )
