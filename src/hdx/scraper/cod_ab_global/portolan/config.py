"""Environment-variable configuration for the portolan submodule."""

import os
from os import getenv

from dotenv import load_dotenv

load_dotenv(override=True)

ARCGIS_SERVER = getenv("ARCGIS_SERVER", "https://gis.unocha.org")
ARCGIS_SERVICES_URL = f"{ARCGIS_SERVER}/server/rest/services/Hosted"
ARCGIS_TOKEN_URL = f"{ARCGIS_SERVER}/portal/sharing/rest/generateToken"
ARCGIS_USERNAME = getenv("ARCGIS_USERNAME", "")
ARCGIS_PASSWORD = getenv("ARCGIS_PASSWORD", "")
ARCGIS_EXPIRATION = int(getenv("ARCGIS_EXPIRATION", "1440"))
ARCGIS_TIMEOUT = int(getenv("ARCGIS_TIMEOUT", "60"))

SOURCECOOP_REMOTE = getenv(
    "SOURCECOOP_REMOTE",
    "s3://us-west-2.opendata.source.coop/hdx/cod-ab/",
)
PORTOLAN_WORK_DIR = getenv("PORTOLAN_WORK_DIR", "")
PORTOLAN_WORKERS = int(getenv("PORTOLAN_WORKERS", str(min(os.cpu_count() or 4, 8))))

HDX_EXPORT_OUTPUT_DIR = getenv("HDX_EXPORT_OUTPUT_DIR", "")
# Explicit opt-in, defaulting to off — even once this pipeline is wired up as
# the main entrypoint, actually writing to HDX requires deliberately setting
# this (in addition to whatever hdx_site is configured in
# ~/.hdx_configuration.yaml), so a plain `python -m ...portolan` run never
# pushes to HDX by accident.
HDX_EXPORT_PUSH = getenv("HDX_EXPORT_PUSH", "false").strip().lower() == "true"
