"""Mirror OCHA COD-AB ArcGIS FeatureServer services to source.coop.

Uses geoparquet_io for authenticated extraction (portolan's extract arcgis
CLI does not expose auth options), then portolan for catalog management
and S3 push.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CalledProcessError
from subprocess import run as _run
from textwrap import dedent

import geoparquet_io as gpio
import yaml
from hdx.location.country import Country

from .config import (
    ARCGIS_SERVICES_URL,
    PORTOLAN_WORKERS,
    SOURCECOOP_REMOTE,
)
from .utils import fetch_json, fetch_metadata_table, generate_token, list_services

logger = logging.getLogger(__name__)

_CATALOG_TITLE = "COD-AB Administrative Boundaries"


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


# All meaningful fields from COD_Global_Metadata (mirrors refactor.py's column list).
# Written as cod_ab:* custom STAC properties so the table is reconstructable from
# the catalog.
_COD_AB_METADATA_FIELDS = [
    "country_name",
    "country_iso2",
    "country_iso3",
    "version",
    "admin_level_full",
    "admin_level_max",
    "admin_1_name",
    "admin_2_name",
    "admin_3_name",
    "admin_4_name",
    "admin_5_name",
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
    "admin_notes",
    "date_source",
    "date_updated",
    "date_reviewed",
    "date_metadata",
    "date_valid_on",
    "date_valid_to",
    "update_frequency",
    "update_type",
    "source",
    "contributor",
    "methodology_dataset",
    "methodology_pcodes",
    "caveats",
]


def _enrich_service_catalog(service_dir: Path, meta: dict) -> None:
    """Write COD_Global_Metadata fields as cod_ab:* properties in catalog.json."""
    catalog_path = service_dir / "catalog.json"
    if not catalog_path.exists():
        return
    data = json.loads(catalog_path.read_text())
    for field in _COD_AB_METADATA_FIELDS:
        value = meta.get(field)
        if value is not None and str(value).strip():
            data[f"cod_ab:{field}"] = value
    if "cod_ab:country_iso2" not in data:
        iso3 = (meta.get("country_iso3") or "").upper()
        iso2 = Country.get_iso2_from_iso3(iso3) if iso3 else None
        if iso2:
            data["cod_ab:country_iso2"] = iso2
    catalog_path.write_text(json.dumps(data, indent=2))


def _write_service_metadata(
    service_dir: Path, service_name: str, meta: dict | None
) -> None:
    """Write .portolan/metadata.yaml for a service (subcatalog)."""
    if not meta:
        return

    content: dict = {}

    contributor = (meta.get("contributor") or "").strip()
    source = (meta.get("source") or "").strip()
    if contributor and source:
        content["attribution"] = f"{contributor} / {source}"
    elif contributor or source:
        content["attribution"] = contributor or source

    content["source_url"] = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"

    version = (meta.get("version") or "").strip()
    if version:
        content["upstream_version"] = version

    caveats = (meta.get("caveats") or "").strip()
    if caveats:
        content["known_issues"] = caveats

    notes = (meta.get("admin_notes") or "").strip()
    if notes:
        content["processing_notes"] = notes

    date_valid_on = (meta.get("date_valid_on") or "").strip()
    date_valid_to = (meta.get("date_valid_to") or "").strip() or None
    if date_valid_on:
        temporal: dict = {"start": date_valid_on}
        if date_valid_to:
            temporal["end"] = date_valid_to
        content["defaults"] = {"temporal": temporal}

    portolan_dir = service_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            content, default_flow_style=False, allow_unicode=True, sort_keys=False
        )
    )


def _last_edit_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="milliseconds")


def _read_stored_updated(layer_dir: Path) -> str | None:
    collection_path = layer_dir / "collection.json"
    if not collection_path.exists():
        return None
    return json.loads(collection_path.read_text()).get("updated")


def _enrich_layer_collection(layer_dir: Path, updated_iso: str) -> None:
    """Write the STAC updated field into a layer collection.json."""
    collection_path = layer_dir / "collection.json"
    if not collection_path.exists():
        return
    data = json.loads(collection_path.read_text())
    data["updated"] = updated_iso
    collection_path.write_text(json.dumps(data, indent=2))


def _extract_service(
    service_name: str,
    token: str,
    catalog_dir: Path,
    metadata: dict[str, dict],
) -> dict[str, str]:
    """Extract all layers for one COD-AB service to GeoParquet.

    Returns {layer_name: updated_iso} for all layers where lastEditDate is available,
    to be written into each layer's collection.json after portolan add.
    """
    service_url = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"
    data = fetch_json(service_url, token)

    service_dir = catalog_dir / service_name.lower()
    layer_updated: dict[str, str] = {}

    for layer in data.get("layers", []):
        layer_id = layer["id"]
        layer_name = layer["name"].lower().replace(" ", "_")
        layer_dir = service_dir / layer_name
        out_path = layer_dir / f"{layer_name}.parquet"

        layer_dir.mkdir(parents=True, exist_ok=True)

        layer_url = f"{service_url}/{layer_id}"
        layer_meta = fetch_json(layer_url, token)
        last_edit = (layer_meta.get("editingInfo") or {}).get("lastEditDate")
        updated_iso = _last_edit_to_iso(last_edit) if last_edit is not None else None

        if updated_iso is not None:
            layer_updated[layer_name] = updated_iso

        if out_path.exists():
            stored = _read_stored_updated(layer_dir)
            # Skip if: no lastEditDate (can't detect changes), timestamps match,
            # or no stored value yet (bootstrap: trust existing parquet).
            if updated_iso is None or stored is None or updated_iso == stored:
                logger.debug("Skipping unchanged %s", out_path)
                continue
            logger.info(
                "Re-extracting updated layer %s (lastEditDate changed)", layer_name
            )
            out_path.unlink()

        logger.info("Extracting %s", layer_url)

        try:
            table = gpio.extract_arcgis(layer_url, token=token)
        except Exception:
            logger.exception("Failed to extract %s — skipping layer", layer_url)
            continue
        table = table.sort_hilbert()
        table.write(out_path, compression_level=22, geoparquet_version="2.0")

    meta = metadata.get(service_name.lower())
    _write_service_metadata(service_dir, service_name, meta)
    return layer_updated


def _push_catalog_files(work_dir: Path, remote: str) -> None:
    """Upload intermediate catalog.json and README.md files to S3.

    portolan push handles leaf collections only. This syncs catalog.json and
    README.md at the root, variant (original/, extended/, ...), and service
    levels so STAC clients can navigate the full hierarchy.
    """
    _run(
        [
            "aws",
            "s3",
            "sync",
            str(work_dir),
            remote.rstrip("/"),
            "--exclude",
            "*",
            "--include",
            "catalog.json",
            "--include",
            "*/catalog.json",
            "--include",
            "*/*/catalog.json",
            "--include",
            "*/README.md",
            "--include",
            "*/*/README.md",
            "--exclude",
            "*/*/*/*",
        ],
        check=True,
    )


def _ensure_root_catalog(work_dir: Path) -> None:
    """Initialise the single portolan catalog rooted at work_dir if not present."""
    if (work_dir / ".portolan" / "config.yaml").exists() and (
        work_dir / "catalog.json"
    ).exists():
        return
    (work_dir / ".portolan" / "config.yaml").unlink(missing_ok=True)
    _portolan(["init", "--title", _CATALOG_TITLE, "--auto"], cwd=work_dir)


def _remove_stale_services(
    services: list[str],
    variant_dir: Path,
    variant_prefix: str,
    catalog_dir: Path,
) -> None:
    """Remove service directories no longer present in ArcGIS.

    variant_dir is scanned for stale subdirectories; portolan rm is called
    from catalog_dir (the root) using variant_prefix/service_name/ paths.
    """
    current = {s.lower() for s in services}
    for path in sorted(variant_dir.iterdir()):
        if not path.is_dir() or path.name.startswith("."):
            continue
        if path.name not in current:
            logger.info("Removing stale service %s", path.name)
            _portolan(
                ["rm", "--force", f"{variant_prefix}/{path.name}/"], cwd=catalog_dir
            )


def run(work_dir: Path) -> None:
    """Mirror OCHA COD-AB ArcGIS services to source.coop.

    Requires: ARCGIS_USERNAME, ARCGIS_PASSWORD, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY. Optional: SOURCECOOP_REMOTE (default points to
    the hdx/cod-ab catalog on source.coop).
    """
    catalog_dir = work_dir / "original"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    token = generate_token()
    services = list_services(token)
    logger.info("Found %d COD-AB services", len(services))
    metadata = fetch_metadata_table(token)
    logger.info("Fetched metadata for %d services", len(metadata))

    service_layer_updated: dict[str, dict[str, str]] = {}
    for service_name in sorted(services):
        service_layer_updated[service_name] = _extract_service(
            service_name, token, catalog_dir, metadata
        )

    _write_catalog_metadata(work_dir)

    workers = str(PORTOLAN_WORKERS)
    if catalog_dir.exists() and any(
        d.is_dir() and not d.name.startswith(".") for d in catalog_dir.iterdir()
    ):
        _remove_stale_services(services, catalog_dir, "original", work_dir)

    for service_name in sorted(services):
        meta = metadata.get(service_name.lower())
        date_valid_on = (meta.get("date_valid_on") or "").strip() if meta else ""
        args = [
            "add",
            f"original/{service_name.lower()}/",
            "--workers",
            workers,
            "--pmtiles",
        ]
        if date_valid_on:
            args += ["--datetime", date_valid_on]
        try:
            _portolan(args, cwd=work_dir)
        except CalledProcessError:
            logger.exception("portolan add failed for %s — skipping", service_name)
            continue
        if meta:
            _enrich_service_catalog(catalog_dir / service_name.lower(), meta)
        service_dir = catalog_dir / service_name.lower()
        for layer_name, iso in service_layer_updated.get(service_name, {}).items():
            _enrich_layer_collection(service_dir / layer_name, iso)
    try:
        _portolan(["stac-geoparquet"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet: no items in catalog — skipping")
    _portolan(
        ["push", SOURCECOOP_REMOTE, "--workers", workers, "--verbose"], cwd=work_dir
    )
    _push_catalog_files(work_dir, SOURCECOOP_REMOTE)
    # TODO(portolan-sdi/portolan-cli#543): restore --strict once fixed
    _portolan(["check", "--verbose"], cwd=work_dir)
