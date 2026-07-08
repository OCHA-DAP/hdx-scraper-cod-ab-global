"""Mirror OCHA COD-AB ArcGIS FeatureServer services to source.coop.

Uses geoparquet_io for authenticated extraction (portolan's extract arcgis
CLI does not expose auth options), then portolan for catalog management
and S3 push.
"""

import json
import logging
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from shutil import copy, rmtree
from subprocess import CalledProcessError
from subprocess import run as _run
from textwrap import dedent

import geoparquet_io as gpio
import yaml
from hdx.location.country import Country

from hdx.scraper.cod_ab_global.config import date_valid_on_overrides

from .config import (
    ARCGIS_SERVICES_URL,
    PORTOLAN_WORKERS,
)
from .utils import fetch_json, fetch_metadata_table, generate_token, list_services

logger = logging.getLogger(__name__)

_CATALOG_TITLE = "COD-AB Administrative Boundaries"

_PORTOLAN = str(Path(sys.executable).parent / "portolan")


def _portolan(args: list[str], cwd: Path) -> None:
    _run([_PORTOLAN, *args], cwd=cwd, check=True)


def _service_to_path(service_name: str) -> tuple[str, str]:
    """Return (iso3, version) for a COD-AB service name.

    cod_ab_eth_v04 → ("eth", "v04")
    cod_ab_eth     → ("eth", "latest")
    """
    # cod_ab_<iso3>[_<version>] — 3 parts unversioned, 4 parts versioned
    parts = service_name.lower().split("_")
    iso3 = parts[2]
    version = parts[3] if len(parts) > 3 else "latest"  # noqa: PLR2004
    return iso3, version


def _layer_short_name(layer_name: str, iso3: str) -> str:
    """Shorten a layer name by stripping the iso3_admin prefix.

    eth_admin1 → adm1  ·  eth_adminlines → lines  ·  eth_admincapitals → capitals
    """
    stripped = re.sub(rf"^{re.escape(iso3)}_admin", "", layer_name, flags=re.IGNORECASE)
    return f"adm{stripped}" if stripped.isdigit() else stripped


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
    iso3 = (meta.get("country_iso3") or "").upper()
    if "cod_ab:country_iso2" not in data:
        iso2 = Country.get_iso2_from_iso3(iso3) if iso3 else None
        if iso2:
            data["cod_ab:country_iso2"] = iso2
    if "cod_ab:date_valid_on" not in data and iso3 in date_valid_on_overrides:
        data["cod_ab:date_valid_on"] = date_valid_on_overrides[iso3]
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


def read_catalog(version_dir: Path) -> dict:
    """Return parsed catalog.json content, or {} if missing/unreadable.

    Shared by extended.py/matched.py's change-detection and hdx_export's
    fingerprinting/metadata readers so there's one place that knows how to
    open a service's catalog.json.
    """
    catalog_path = version_dir / "catalog.json"
    if not catalog_path.exists():
        return {}
    try:
        return json.loads(catalog_path.read_text())
    except json.JSONDecodeError:
        return {}


def read_json_state(path: Path) -> dict:
    """Return parsed JSON content at `path`, or {} if missing/unreadable.

    Small shared helper for the "skip rebuild if fingerprint unchanged"
    pattern used by both global_.py's `.global_state.json` and
    hdx_export/state.py's `.hdx_export/state.json`.
    """
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def write_json_state(path: Path, data: dict) -> None:
    """Write `data` as indented, sort-keyed JSON to `path`."""
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def _enrich_layer_collection(layer_dir: Path, updated_iso: str) -> None:
    """Write the STAC updated field into a layer collection.json."""
    collection_path = layer_dir / "collection.json"
    if not collection_path.exists():
        return
    data = json.loads(collection_path.read_text())
    data["updated"] = updated_iso
    collection_path.write_text(json.dumps(data, indent=2))


def inject_variant_assets(collection_path: Path, suffix: str) -> None:
    """Add <suffix> data+tiles assets to an existing collection.json.

    Uses portolan's native asset key convention: {suffix} for data,
    {suffix}-tiles for visual tiles.
    """
    if not collection_path.exists():
        return
    data = json.loads(collection_path.read_text())
    assets = data.setdefault("assets", {})
    title = suffix.capitalize()
    assets[suffix] = {
        "href": f"./{suffix}.parquet",
        "type": "application/vnd.apache.parquet",
        "title": title,
        "roles": ["data"],
    }
    assets[f"{suffix}-tiles"] = {
        "href": f"./{suffix}.pmtiles",
        "type": "application/vnd.pmtiles",
        "title": f"{title} (tiles)",
        "roles": ["visual"],
    }
    collection_path.write_text(json.dumps(data, indent=2))


def _generate_variant_pmtiles(
    variant_parquet: Path, layer_dir: Path, workers: str
) -> None:
    """Generate PMTiles for a variant parquet via an isolated temp portolan catalog."""
    stem = variant_parquet.stem  # "extended" or "matched"
    with tempfile.TemporaryDirectory(prefix="portolan-pmtiles-") as tmp:
        tmp_path = Path(tmp)
        tmp_layer = tmp_path / "svc" / stem
        tmp_layer.mkdir(parents=True)
        copy(variant_parquet, tmp_layer / variant_parquet.name)
        _portolan(["init", "--title", "tmp", "--auto"], cwd=tmp_path)
        try:
            _portolan(
                ["add", f"svc/{stem}/", "--workers", workers, "--pmtiles"],
                cwd=tmp_path,
            )
        except CalledProcessError:
            logger.warning("PMTiles generation failed for %s", variant_parquet.name)
            return
        src = tmp_layer / f"{stem}.pmtiles"
        if src.exists():
            copy(src, layer_dir / f"{stem}.pmtiles")


def _hide_variant_files(version_dir: Path) -> list[tuple[Path, Path]]:
    """Temporarily rename variant parquets so portolan only sees original.parquet."""
    hidden: list[tuple[Path, Path]] = []
    for layer_dir in version_dir.iterdir():
        if not layer_dir.is_dir() or layer_dir.name.startswith("."):
            continue
        for suffix in ("extended", "matched"):
            p = layer_dir / f"{suffix}.parquet"
            if p.exists():
                h = layer_dir / f".portolan_bak_{suffix}.parquet"
                p.rename(h)
                hidden.append((h, p))
    return hidden


def _restore_hidden_files(hidden: list[tuple[Path, Path]]) -> None:
    for h, p in hidden:
        if h.exists():
            h.rename(p)


def _extract_service(
    service_name: str,
    token: str,
    work_dir: Path,
    metadata: dict[str, dict],
) -> tuple[dict[str, str], bool]:
    """Extract all layers for one COD-AB service to GeoParquet.

    Returns (layer_updated, any_extracted) where layer_updated is
    {layer_short: updated_iso} for layers with lastEditDate, and any_extracted
    is True if at least one layer was actually downloaded this run.
    """
    service_url = f"{ARCGIS_SERVICES_URL}/{service_name}/FeatureServer"
    data = fetch_json(service_url, token)

    iso3, version = _service_to_path(service_name)
    version_dir = work_dir / iso3 / version
    layer_updated: dict[str, str] = {}
    any_extracted = False

    for layer in data.get("layers", []):
        layer_id = layer["id"]
        layer_name = layer["name"].lower().replace(" ", "_")

        if layer_name.endswith("_em"):
            # ArcGIS pre-matched layers — we generate our own matched variant
            stale = version_dir / _layer_short_name(layer_name, iso3)
            if stale.exists():
                rmtree(stale)
            continue

        layer_short = _layer_short_name(layer_name, iso3)
        layer_dir = version_dir / layer_short
        out_path = layer_dir / "original.parquet"

        layer_dir.mkdir(parents=True, exist_ok=True)

        layer_url = f"{service_url}/{layer_id}"
        layer_meta = fetch_json(layer_url, token)
        last_edit = (layer_meta.get("editingInfo") or {}).get("lastEditDate")
        updated_iso = _last_edit_to_iso(last_edit) if last_edit is not None else None

        if updated_iso is not None:
            layer_updated[layer_short] = updated_iso

        if out_path.exists():
            stored = _read_stored_updated(layer_dir)
            if updated_iso is None or stored is None or updated_iso == stored:
                logger.debug("Skipping unchanged %s", out_path)
                continue
            logger.info(
                "Re-extracting updated layer %s (lastEditDate changed)", layer_short
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
        any_extracted = True

    meta = metadata.get(service_name.lower())
    if version_dir.exists():
        _write_service_metadata(version_dir, service_name, meta)
    return layer_updated, any_extracted


def _push_catalog_files(work_dir: Path, remote: str) -> None:
    """Upload intermediate catalog.json and README.md files to S3.

    portolan push handles leaf collections only. This syncs catalog.json and
    README.md at the root, country, and service levels so STAC clients can
    navigate the full hierarchy.
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


def _remove_stale_services(services: list[str], work_dir: Path) -> None:
    """Remove version directories no longer present in ArcGIS.

    Scans {iso3}/{version}/ directories and calls portolan rm for any that no
    longer correspond to an active ArcGIS service.
    """
    current = {_service_to_path(s) for s in services}
    for country_dir in sorted(work_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        if country_dir.name == "wld":
            continue
        iso3 = country_dir.name
        for version_dir in sorted(country_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            version = version_dir.name
            if (iso3, version) not in current:
                logger.info("Removing stale service %s/%s", iso3, version)
                try:
                    _portolan(["rm", "--force", f"{iso3}/{version}/"], cwd=work_dir)
                except CalledProcessError:
                    logger.warning("portolan rm failed for %s/%s", iso3, version)


def _enrich_original_layers(version_dir: Path, layer_updated: dict[str, str]) -> None:
    """Write updated timestamps and Original titles into layer collection.json files."""
    for layer_short, updated_iso in layer_updated.items():
        _enrich_layer_collection(version_dir / layer_short, updated_iso)
    # Ensure portolan-generated original asset has a human-readable title
    for layer_dir in version_dir.iterdir():
        if not layer_dir.is_dir() or layer_dir.name.startswith("."):
            continue
        collection_path = layer_dir / "collection.json"
        if not collection_path.exists():
            continue
        data = json.loads(collection_path.read_text())
        assets = data.get("assets", {})
        if "original" in assets and "title" not in assets["original"]:
            assets["original"]["title"] = "Original"
            collection_path.write_text(json.dumps(data, indent=2))


def _add_service_to_catalog(  # noqa: PLR0913
    service_name: str,
    version_dir: Path,
    iso3: str,
    version: str,
    meta: dict | None,
    layer_updated: dict[str, str],
    workers: str,
    work_dir: Path,
) -> None:
    """Run portolan add for one service and apply post-add enrichments."""
    date_valid_on = (meta.get("date_valid_on") or "").strip() if meta else ""
    hidden = _hide_variant_files(version_dir)
    args = ["add", f"{iso3}/{version}/", "--workers", workers, "--pmtiles"]
    if date_valid_on:
        args += ["--datetime", date_valid_on]
    try:
        _portolan(args, cwd=work_dir)
    except CalledProcessError:
        logger.exception("portolan add failed for %s — skipping", service_name)
        _restore_hidden_files(hidden)
        return
    _restore_hidden_files(hidden)
    if meta:
        _enrich_service_catalog(version_dir, meta)
    _enrich_original_layers(version_dir, layer_updated)


def run(work_dir: Path) -> None:
    """Mirror OCHA COD-AB ArcGIS services to source.coop.

    Requires: ARCGIS_USERNAME, ARCGIS_PASSWORD.
    Push is handled by __main__.py after all stages complete.
    """
    token = generate_token()
    services = list_services(token)
    logger.info("Found %d COD-AB services", len(services))
    metadata = fetch_metadata_table(token)
    logger.info("Fetched metadata for %d services", len(metadata))

    service_layer_updated: dict[str, dict[str, str]] = {}
    service_extracted: dict[str, bool] = {}

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {
            pool.submit(_extract_service, sn, token, work_dir, metadata): sn
            for sn in sorted(services)
        }
        for future in as_completed(futures):
            sn = futures[future]
            try:
                layer_updated, extracted = future.result()
                service_layer_updated[sn] = layer_updated
                service_extracted[sn] = extracted
            except Exception:
                logger.exception("Extraction failed for %s — skipping", sn)
                service_layer_updated[sn] = {}
                service_extracted[sn] = False

    _write_catalog_metadata(work_dir)
    _remove_stale_services(services, work_dir)

    workers = str(PORTOLAN_WORKERS)
    for service_name in sorted(services):
        iso3, version = _service_to_path(service_name)
        version_dir = work_dir / iso3 / version
        if not version_dir.exists():
            continue
        # Skip portolan add when catalog already exists and nothing was re-extracted —
        # avoids ~268 redundant catalog operations on no-change runs.
        catalog_exists = (version_dir / "catalog.json").exists()
        if catalog_exists and not service_extracted.get(service_name, False):
            continue
        _add_service_to_catalog(
            service_name,
            version_dir,
            iso3,
            version,
            metadata.get(service_name.lower()),
            service_layer_updated.get(service_name, {}),
            workers,
            work_dir,
        )

    try:
        _portolan(["stac-geoparquet"], cwd=work_dir)
    except CalledProcessError:
        logger.warning("portolan stac-geoparquet: no items in catalog — skipping")
