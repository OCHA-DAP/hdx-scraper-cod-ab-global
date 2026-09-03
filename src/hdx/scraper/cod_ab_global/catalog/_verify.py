"""Fix mis-rooted asset hrefs and verify/repair remote completeness after push."""

import logging
from pathlib import Path
from subprocess import CalledProcessError

import obstore as obs
from portolan_cli.sync.upload import setup_store

from ._json import read_json_state, write_json_state
from ._subprocess import _portolan

logger = logging.getLogger(__name__)


def _is_leaf_collection(data: dict) -> bool:
    """Return True for a real collection's versions.json, false for an index file."""
    return "versions" in data


def normalize_asset_hrefs(catalog_root: Path) -> int:
    """Rewrite hrefs `portolan add <subpath>` wrote relative to that subpath.

    Restores catalog-root-relative hrefs (portolan-cli issue, workaround). Idempotent.
    """
    fixed = 0
    for versions_path in sorted(catalog_root.rglob("versions.json")):
        collection_dir = versions_path.parent
        data = read_json_state(versions_path)
        if not _is_leaf_collection(data):
            continue
        prefix = collection_dir.parent.relative_to(catalog_root).as_posix()
        changed = False
        for version_entry in data.get("versions", []):
            for asset_data in version_entry.get("assets", {}).values():
                href = asset_data.get("href")
                if not href or (catalog_root / href).exists():
                    continue
                candidate = f"{prefix}/{href}" if prefix else href
                if (catalog_root / candidate).exists():
                    asset_data["href"] = candidate
                    changed = True
        if changed:
            write_json_state(versions_path, data)
            fixed += 1
    return fixed


def _required_remote_keys(catalog_root: Path, prefix: str) -> dict[str, list[str]]:
    """Map each collection id to the remote keys its current version needs."""
    required: dict[str, list[str]] = {}
    for versions_path in sorted(catalog_root.rglob("versions.json")):
        data = read_json_state(versions_path)
        if not _is_leaf_collection(data):
            continue
        collection_id = versions_path.parent.relative_to(catalog_root).as_posix()
        current = data.get("current_version")
        keys = [f"{prefix}/{collection_id}/versions.json".lstrip("/")]
        for version_entry in data.get("versions", []):
            if version_entry.get("version") != current:
                continue
            for asset_data in version_entry.get("assets", {}).values():
                href = asset_data.get("href")
                if href:
                    keys.append(f"{prefix}/{href}".lstrip("/"))
        required[collection_id] = keys
    return required


def find_incomplete_collections(catalog_root: Path, remote: str) -> list[str]:
    """Return collection ids whose current version is missing from remote."""
    store, prefix = setup_store(remote)
    root = prefix.strip("/")
    remote_keys: set[str] = set()
    for batch in obs.list(store, prefix=root or None):
        remote_keys.update(entry["path"] for entry in batch)
    required = _required_remote_keys(catalog_root, root)
    return sorted(
        collection_id
        for collection_id, keys in required.items()
        if any(key not in remote_keys for key in keys)
    )


def repair_collections(catalog_dir: Path, remote: str, collections: list[str]) -> None:
    """Re-push each incomplete collection individually."""
    for collection_id in collections:
        logger.warning("Repairing incomplete collection %s", collection_id)
        try:
            _portolan(
                ["push", remote, "--collection", collection_id, "--force", "--verbose"],
                cwd=catalog_dir,
            )
        except CalledProcessError:
            logger.warning("Repair push failed for %s", collection_id)
