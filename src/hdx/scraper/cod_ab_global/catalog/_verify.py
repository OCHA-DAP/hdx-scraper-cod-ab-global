"""Fix local versions.json files that would publish broken or redundant collections."""

from pathlib import Path

from ._json import read_json_state


def _is_leaf_collection(data: dict) -> bool:
    """Return True for a real collection's versions.json, false for an index file."""
    return "versions" in data


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
