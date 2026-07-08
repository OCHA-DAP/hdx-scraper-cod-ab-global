"""Entry point: python -m hdx.scraper.cod_ab_global.portolan.

Requires env vars (via .env or shell):
  ARCGIS_USERNAME, ARCGIS_PASSWORD        OCHA ArcGIS credentials
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  source.coop credentials

Optional:
  PORTOLAN_WORK_DIR       persistent work directory (enables resume on re-run)
  SOURCECOOP_REMOTE       override S3 destination
"""

import functools
import logging
import os
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

os.environ.setdefault("OGR_GEOJSON_MAX_OBJ_SIZE", "0")

import geoparquet_io.core.arcgis as _gpio_arcgis

_orig_request = _gpio_arcgis.make_request_with_retry


@functools.wraps(_orig_request)
def _patched_request(*args: Any, timeout: float = 300.0, **kwargs: Any) -> Any:  # noqa: ANN401
    return _orig_request(*args, timeout=timeout, **kwargs)


_gpio_arcgis.make_request_with_retry = _patched_request

from .config import (  # noqa: E402
    HDX_EXPORT_OUTPUT_DIR,
    HDX_EXPORT_PUSH,
    PORTOLAN_WORK_DIR,
    PORTOLAN_WORKERS,
    SOURCECOOP_REMOTE,
)
from .extended import run as extended_run  # noqa: E402
from .global_ import run as global_run  # noqa: E402
from .hdx_export import run as hdx_export_run  # noqa: E402
from .matched import run as matched_run  # noqa: E402
from .original import _ensure_root_catalog, _portolan, _push_catalog_files  # noqa: E402
from .original import run as original_run  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

work_dir = (
    Path(PORTOLAN_WORK_DIR)
    if PORTOLAN_WORK_DIR
    else Path(mkdtemp(prefix="portolan-cod-ab-"))
)
_ensure_root_catalog(work_dir)
original_run(work_dir)
extended_run(work_dir)
matched_run(work_dir)
global_run(work_dir)

# Single consolidated push after all stages complete — users never see partial state
workers = str(PORTOLAN_WORKERS)
_portolan(["push", SOURCECOOP_REMOTE, "--workers", workers, "--verbose"], cwd=work_dir)
_push_catalog_files(work_dir, SOURCECOOP_REMOTE)
_portolan(["check", "--verbose"], cwd=work_dir)

# HDX export: always builds fresh GDBs/pcodes/metadata from the catalog
# (cheap to skip via hdx_export's own fingerprint check when nothing changed);
# actually pushing to HDX requires the explicit HDX_EXPORT_PUSH opt-in, on top
# of whatever hdx_site is configured in ~/.hdx_configuration.yaml.
hdx_export_output_dir = (
    Path(HDX_EXPORT_OUTPUT_DIR)
    if HDX_EXPORT_OUTPUT_DIR
    else work_dir.parent / "hdx_export_build"
)
if HDX_EXPORT_PUSH:
    from hdx.api.configuration import Configuration

    Configuration.create(
        user_agent_config_yaml=Path("~").expanduser() / ".useragents.yaml",
        user_agent_lookup="hdx-scraper-cod-global",
    )
hdx_export_run(work_dir, hdx_export_output_dir, push_to_hdx=HDX_EXPORT_PUSH)
