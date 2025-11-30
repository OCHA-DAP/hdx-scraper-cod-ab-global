from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import wheretostart_tempdir_batch

from .config import run_exclude, run_include
from .dataset.boundaries import create_boundaries_dataset
from .dataset.pcodes import create_pcodes_dataset
from .download.boundaries import download_boundaries
from .download.check_lists import check_lists
from .download.metadata import download_metadata
from .process.boundaries import create_boundaries
from .process.extended_post import postprocess_extended
from .process.extended_pre import preprocess_extended
from .process.matched import create_matched
from .process.pcodes import create_pcodes
from .utils import generate_token

cwd = Path(__file__).parent

_USER_AGENT_LOOKUP = "hdx-scraper-cod-global"
_SAVED_DATA_DIR = (cwd / "../../../../saved_data").resolve()
_UPDATED_BY_SCRIPT = "HDX Scraper: COD-AB Global"


def can_run(run: str) -> bool:
    """Determine whether step can be run."""
    return (not run_include or run in run_include) and (
        not run_exclude or run not in run_exclude
    )


def main(save: bool = True, use_saved: bool = False) -> None:  # noqa: C901, FBT001, FBT002, PLR0912
    """Generate datasets and create them in HDX."""
    Configuration.read()
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        data_dir = Path(_SAVED_DATA_DIR if save or use_saved else temp_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        if (
            can_run("DOWNLOAD")
            or can_run("DOWNLOAD_METADATA")
            or can_run("DOWNLOAD_BOUNDARIES")
        ):
            token = generate_token()
            if can_run("DOWNLOAD") or can_run("DOWNLOAD_METADATA"):
                download_metadata(data_dir, token)
            if can_run("DOWNLOAD") or can_run("DOWNLOAD_BOUNDARIES"):
                download_boundaries(data_dir, token)
            if can_run("DOWNLOAD") or can_run("DOWNLOAD_CHECK"):
                check_lists(data_dir)
        if can_run("ORIGINAL") or can_run("ORIGINAL_BOUNDARIES"):
            create_boundaries(data_dir, "original")
        if can_run("ORIGINAL") or can_run("ORIGINAL_PCODES"):
            create_pcodes(data_dir)
        if can_run("EXTENDED_PRE") or can_run("EXTENDED_PRE_COUNTRY"):
            preprocess_extended(data_dir)
        if can_run("EXTENDED_POST") or can_run("EXTENDED_POST_COUNTRY"):
            postprocess_extended(data_dir)
        if can_run("EXTENDED_POST") or can_run("EXTENDED_POST_GLOBAL"):
            create_boundaries(data_dir, "extended")
        if can_run("MATCHED") or can_run("MATCHED_COUNTRY"):
            create_matched(data_dir)
        if can_run("MATCHED") or can_run("MATCHED_GLOBAL"):
            create_boundaries(data_dir, "matched")
        if can_run("DATASET") or can_run("DATASET_PCODES"):
            create_pcodes_dataset(data_dir, info, _UPDATED_BY_SCRIPT)
        if can_run("DATASET") or can_run("DATASET_BOUNDARIES"):
            create_boundaries_dataset(data_dir, info, _UPDATED_BY_SCRIPT)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
    )
