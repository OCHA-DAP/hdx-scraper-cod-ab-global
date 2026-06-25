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

from .config import PORTOLAN_WORK_DIR  # noqa: E402
from .extended import run as extended_run  # noqa: E402
from .matched import run as matched_run  # noqa: E402
from .mirror import run as mirror_run  # noqa: E402

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
mirror_run(work_dir)
extended_run(work_dir)
matched_run(work_dir)
