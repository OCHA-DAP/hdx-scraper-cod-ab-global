"""Generic JSON file read/write helpers used for catalog and state files."""

import json
from pathlib import Path


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
