"""Fix local versions.json files that would publish broken or redundant collections."""

from pathlib import Path

from ._json import read_json_state, write_json_state


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


def remove_nested_index_versions(catalog_root: Path) -> int:
    """Delete index versions.json files below the root that push treats as collections.

    Workaround: push re-uploads every file under such a dir on every run. Idempotent.
    """
    removed = 0
    for versions_path in sorted(catalog_root.rglob("versions.json")):
        rel_parts = versions_path.parent.relative_to(catalog_root).parts
        if not rel_parts or any(part.startswith(".") for part in rel_parts):
            continue
        if not _is_leaf_collection(read_json_state(versions_path)):
            versions_path.unlink()
            removed += 1
    return removed
