"""Download and topology-clean the UN BNDA 1:1M international boundary."""

import logging
import tempfile
from pathlib import Path

import geoparquet_io as gpio
from topo_tools import topo_clean

from hdx.scraper.cod_ab_global.config import ARCGIS_SERVICES_URL
from hdx.scraper.cod_ab_global.extended import _write_gpq2
from hdx.scraper.cod_ab_global.utils import generate_token

logger = logging.getLogger(__name__)

_BNDA_URL = f"{ARCGIS_SERVICES_URL}/Global_AB_1M_fs_gray/FeatureServer/5"


def _clean_bnda(raw_path: Path, out_path: Path) -> None:
    """Fix gap/overlap coverage defects in raw BNDA via topo_tools."""
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-clean-") as tmp:
        issues_path = Path(tmp) / "bnda_issues.parquet"
        topo_clean(raw_path, out_path, issues_path, overwrite=True)


def ensure_bnda(catalogs_dir: Path) -> Path:
    """Return the sibling .bnda/bnda_cty.parquet, downloading it if absent."""
    bnda_dir = catalogs_dir.parent / ".bnda"
    bnda_dir.mkdir(exist_ok=True)
    bnda_path = bnda_dir / "bnda_cty.parquet"
    if bnda_path.exists():
        return bnda_path
    logger.info("Downloading UN BNDA boundaries from %s", _BNDA_URL)
    token = generate_token()
    table = gpio.extract_arcgis(_BNDA_URL, token=token, timeout=300)
    with tempfile.TemporaryDirectory(prefix="portolan-bnda-") as tmp:
        raw_path = Path(tmp) / "bnda_raw.parquet"
        clean_path = Path(tmp) / "bnda_clean.parquet"
        table.write(str(raw_path), compression_level=15, geoparquet_version="2.0")
        _clean_bnda(raw_path, clean_path)
        _write_gpq2(clean_path, bnda_path)
    logger.info("Saved BNDA to %s", bnda_path)
    return bnda_path
