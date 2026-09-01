"""Shared portolan-catalog infrastructure used by original/extended/matched/global/.

Tree-walking, fingerprinting, catalog-metadata, `portolan` subprocess helpers.
"""

import json
import logging
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from subprocess import CalledProcessError
from subprocess import run as _run
from textwrap import dedent

from .config import ARCGIS_SERVICES_URL

logger = logging.getLogger(__name__)

CATALOG_TITLE = "COD-AB Administrative Boundaries"
ADM_SUFFIXES = ("_name", "_name1", "_name2", "_name3", "_pcode")

_TREE_TITLES = {
    "original": "Original",
    "extended": "Extended",
    "matched": "Matched",
    "global": "Global",
}
_PORTOLAN = str(Path(sys.executable).parent / "portolan")


def _portolan(args: list[str], cwd: Path) -> None:
    _run([_PORTOLAN, *args], cwd=cwd, check=True)


def portolan_add(
    cwd: Path, rel_path: str, workers: str, *, datetime_: str | None = None
) -> None:
    """Run `portolan add` for one service dir, logging (not raising) on failure."""
    args = ["add", rel_path, "--workers", workers, "--pmtiles", "--force"]
    if datetime_:
        args += ["--datetime", datetime_]
    try:
        _portolan(args, cwd=cwd)
    except CalledProcessError:
        logger.warning("portolan add failed for %s (continuing)", rel_path)


def admin_layer_pattern(iso3: str) -> re.Pattern[str]:
    """Return the regex matching native admin-polygon layer dirs for one ISO3."""
    return re.compile(rf"^{re.escape(iso3.lower())}_admin(\d+)$")


def iter_version_dirs(root_dir: Path) -> Iterator[tuple[str, str, Path]]:
    """Yield (iso3, version, version_dir) for every non-hidden service dir."""
    for country_dir in sorted(root_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        for version_dir in sorted(country_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            yield country_dir.name, version_dir.name, version_dir


def admin_layers(version_dir: Path, iso3: str) -> list[tuple[int, Path]]:
    """Return [(level, layer_dir), ...] with an existing parquet, sorted by level."""
    if not version_dir.exists():
        return []
    pattern = admin_layer_pattern(iso3)
    found = []
    for d in version_dir.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if m and (d / f"{d.name}.parquet").exists():
            found.append((int(m.group(1)), d))
    return sorted(found)


def file_fingerprint(path: Path) -> list[int] | None:
    """Return [size, mtime_ns] for a file, or None if it doesn't exist."""
    if not path.exists():
        return None
    stat = path.stat()
    return [stat.st_size, stat.st_mtime_ns]


def read_catalog(version_dir: Path) -> dict:
    """Return parsed catalog.json content, or {} if missing/unreadable."""
    catalog_path = version_dir / "catalog.json"
    if not catalog_path.exists():
        return {}
    try:
        return json.loads(catalog_path.read_text())
    except json.JSONDecodeError:
        return {}


def read_json_state(path: Path) -> dict:
    """Return parsed JSON content at `path`, or {} if missing/unreadable."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def write_json_state(path: Path, data: dict) -> None:
    """Write `data` as indented, sort-keyed JSON to `path`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


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


def _ensure_root_catalog(work_dir: Path) -> None:
    """Initialise the portolan catalog rooted at work_dir if not present."""
    work_dir.mkdir(parents=True, exist_ok=True)
    if (work_dir / ".portolan" / "config.yaml").exists() and (
        work_dir / "catalog.json"
    ).exists():
        _write_catalog_metadata(work_dir)
        return
    (work_dir / ".portolan" / "config.yaml").unlink(missing_ok=True)
    _portolan(
        ["init", "--title", CATALOG_TITLE, "--license", "CC-BY-3.0-IGO", "--auto"],
        cwd=work_dir,
    )
    _write_catalog_metadata(work_dir)


def write_top_catalog(work_dir: Path, tree_names: list[str]) -> None:
    """Write work_dir/catalog.json, linking each sibling tree's own catalog.json."""
    data = {
        "type": "Catalog",
        "id": "cod-ab",
        "stac_version": "1.1.0",
        "description": CATALOG_TITLE,
        "links": [
            {
                "rel": "root",
                "href": "./catalog.json",
                "type": "application/json",
                "title": CATALOG_TITLE,
            },
            *(
                {
                    "rel": "child",
                    "href": f"./{name}/catalog.json",
                    "type": "application/json",
                    "title": _TREE_TITLES.get(name, name.title()),
                }
                for name in tree_names
            ),
        ],
    }
    (work_dir / "catalog.json").write_text(json.dumps(data, indent=2))


def push_top_catalog(work_dir: Path, remote: str) -> None:
    """Upload work_dir/catalog.json to the root of remote."""
    _run(
        [
            "aws",
            "s3",
            "cp",
            str(work_dir / "catalog.json"),
            f"{remote.rstrip('/')}/catalog.json",
        ],
        check=True,
    )


def _push_catalog_files(work_dir: Path, remote: str) -> None:
    """Sync intermediate catalog.json/README.md; portolan push skips those."""
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


def remove_stale_versions(valid_pairs: set[tuple[str, str]], work_dir: Path) -> None:
    """Remove {iso3}/{version}/ directories not in valid_pairs (portolan rm)."""
    for iso3, version, _ in iter_version_dirs(work_dir):
        if (iso3, version) not in valid_pairs:
            logger.info("Removing stale service %s/%s", iso3, version)
            try:
                _portolan(["rm", "--force", f"{iso3}/{version}/"], cwd=work_dir)
            except CalledProcessError:
                logger.warning("portolan rm failed for %s/%s", iso3, version)
