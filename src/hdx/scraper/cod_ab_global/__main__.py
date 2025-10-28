import logging
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.path import wheretostart_tempdir_batch
from tqdm import tqdm

from .config import ARCGIS_METADATA_URL, RUN_STAGE
from .create.em import main as create_em
from .dataset import generate_dataset
from .download.ab import main as download_ab
from .download.em import main as download_em
from .download.meta import cleanup_metadata
from .download.meta import main as download_meta
from .merge.ab import main as merge_ab
from .merge.em import main as merge_em
from .utils import generate_token, get_iso3_list

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


def download_cod_em(data_dir: Path, token: str, iso3_list: list[str]) -> None:
    """Download all COD-AB datasets from ArcGIS Server."""
    pbar = tqdm(iso3_list)
    for iso3 in pbar:
        pbar.set_postfix_str(iso3)
        download_em(data_dir, token, iso3)


def main(save: bool = True, use_saved: bool = False) -> None:  # noqa: FBT001, FBT002
    """Generate datasets and create them in HDX."""
    Configuration.read()
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        data_dir = Path(_SAVED_DATA_DIR if save or use_saved else temp_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        token = generate_token()
        download_meta(data_dir, ARCGIS_METADATA_URL, token)
        return
        iso3_list = get_iso3_list(token)
        if RUN_STAGE == 1:
            download_cod_em(data_dir, token, iso3_list)
        if RUN_STAGE == 2:  # noqa: PLR2004
            create_cod_em(data_dir, iso3_list)
            merge_em(data_dir)
        if RUN_STAGE == 3:  # noqa: PLR2004
            download_ab(data_dir, token)
            merge_ab(data_dir)
        cleanup_metadata(data_dir)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="stage",
        user_agent_config_yaml=str(Path("~").expanduser() / ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
    )
