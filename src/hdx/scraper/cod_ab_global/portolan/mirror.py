"""Mirror OCHA COD-AB ArcGIS FeatureServer services to source.coop.

Uses geoparquet_io for authenticated extraction (portolan's extract arcgis
CLI does not expose auth options), then portolan for catalog management
and S3 push.
"""

import logging
import sys
from pathlib import Path
from subprocess import run as _run
from textwrap import dedent

import geoparquet_io as gpio

from .config import (
    ARCGIS_SERVICES_URL,
    PORTOLAN_WORKERS,
    SOURCECOOP_REMOTE,
)
from .utils import fetch_json, generate_token, list_services

logger = logging.getLogger(__name__)


_PORTOLAN = str(Path(sys.executable).parent / "portolan")


def _portolan(args: list[str], cwd: Path) -> None:
    _run([_PORTOLAN, *args], cwd=cwd, check=True)


def _write_catalog_metadata(catalog_dir: Path) -> None:
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    (portolan_dir / "metadata.yaml").write_text(
        dedent(f"""\
            license: CC-BY-3.0-IGO
            keywords:
              - administrative boundaries
              - COD-AB
              - humanitarian
              - OCHA
              - HDX
              - GeoParquet
              - cloud-native
            contact:
              name: HDX Data Systems Team
              email: hdx@un.org
            attribution: UN OCHA Information Systems Section (ISS)
            source_url: {ARCGIS_SERVICES_URL}
        """)
    )


def _extract_service(service_name: str, token: str, catalog_dir: Path) -> None:
    """Extract all layers for one COD-AB service to GeoParquet."""
    service_url = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"
    data = fetch_json(service_url, token)

    service_dir = catalog_dir / service_name.lower()

    for layer in data.get("layers", []):
        layer_id = layer["id"]
        layer_name = layer["name"].lower().replace(" ", "_")
        layer_dir = service_dir / layer_name
        out_path = layer_dir / f"{layer_name}.parquet"

        if out_path.exists():
            logger.info("Skipping %s/%s (already extracted)", service_name, layer_name)
            continue

        layer_dir.mkdir(parents=True, exist_ok=True)
        layer_url = f"{service_url}/{layer_id}"
        logger.info("Extracting %s", layer_url)

        table = gpio.extract_arcgis(layer_url, token=token)
        table = table.sort_hilbert()
        table.write(out_path, compression="ZSTD", compression_level=15)


def run(work_dir: Path) -> None:
    """Mirror OCHA COD-AB ArcGIS services to source.coop.

    Requires: ARCGIS_USERNAME, ARCGIS_PASSWORD, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY. Optional: SOURCECOOP_REMOTE (default points to
    the hdx/cod-ab catalog on source.coop).
    """
    catalog_dir = work_dir / "original"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    (catalog_dir / ".env").write_text(f"PORTOLAN_REMOTE={SOURCECOOP_REMOTE}\n")

    token = generate_token()
    services = list_services(token)
    logger.info("Found %d COD-AB services", len(services))

    for service_name in sorted(services):
        _extract_service(service_name, token, catalog_dir)

    _write_catalog_metadata(catalog_dir)

    workers = str(PORTOLAN_WORKERS)
    if not (catalog_dir / ".portolan" / "config.yaml").exists():
        _portolan(
            ["init", "--title", "COD-AB Original Boundaries", "--auto"],
            cwd=catalog_dir,
        )
    _portolan(["add", ".", "--workers", workers, "--pmtiles"], cwd=catalog_dir)
    _portolan(["check", "--metadata", "--fix"], cwd=catalog_dir)
    _portolan(["readme"], cwd=catalog_dir)
    _portolan(["push", "--workers", workers, "--verbose"], cwd=catalog_dir)
    # TODO(portolan-sdi/portolan-cli#543): restore --strict once fixed
    _portolan(["check", "--verbose"], cwd=catalog_dir)
