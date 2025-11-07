import logging
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import wheretostart_tempdir_batch
from tqdm import tqdm

from .check_lists import check_lists
from .config import RUN_CHECK, RUN_DOWNLOAD, RUN_PCODE
from .create.em import main as create_em
from .dataset import generate_dataset
from .download.boundaries import main as download_boundaries
from .download.meta import main as download_meta
from .utils import generate_token

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent

_USER_AGENT_LOOKUP = "hdx-scraper-cod-em"
_SAVED_DATA_DIR = (cwd / "../../../../saved_data").resolve()
_UPDATED_BY_SCRIPT = "HDX Scraper: COD-Global"


def create_global_data(info: dict) -> None:
    """Create a dataset for the world."""
    dataset = generate_dataset()
    if dataset:
        dataset.update_from_yaml(path=str(cwd / "config/hdx_dataset_static.yaml"))
        if True:
            dataset.create_in_hdx(
                remove_additional_resources=True,
                match_resource_order=False,
                hxl_update=False,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )


def create_cod_em(data_dir: Path, iso3_list: list[str]) -> None:
    """Create a dataset for each country."""
    pbar = tqdm(iso3_list)
    for iso3 in pbar:
        pbar.set_postfix_str(iso3)
        create_em(data_dir, iso3)


def main(save: bool = True, use_saved: bool = False) -> None:  # noqa: FBT001, FBT002
    """Generate datasets and create them in HDX."""
    Configuration.read()
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        data_dir = Path(_SAVED_DATA_DIR if save or use_saved else temp_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        token = generate_token()
        if RUN_DOWNLOAD:
            download_meta(data_dir, token)
            download_boundaries(data_dir, token)
        if RUN_CHECK:
            check_lists(data_dir)
        if RUN_PCODE:
            pass


if __name__ == "__main__":
    facade(
        main,
        hdx_site="stage",
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
    )
