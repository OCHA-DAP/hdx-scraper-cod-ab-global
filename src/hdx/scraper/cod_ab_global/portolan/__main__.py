"""Entry point: python -m hdx.scraper.cod_ab_global.portolan.

Requires env vars (via .env or shell):
  ARCGIS_USERNAME, ARCGIS_PASSWORD        OCHA ArcGIS credentials
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  source.coop credentials

Optional:
  PORTOLAN_WORK_DIR       persistent work directory (enables resume on re-run)
  SOURCECOOP_REMOTE       override S3 destination
"""

import logging
from pathlib import Path
from tempfile import mkdtemp

from .config import PORTOLAN_WORK_DIR
from .mirror import run

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
run(work_dir)
