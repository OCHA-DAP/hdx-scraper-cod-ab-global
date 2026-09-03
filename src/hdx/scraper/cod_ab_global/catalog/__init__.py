"""Shared portolan-catalog infrastructure used by original/extended/matched/global/.

Tree-walking, fingerprinting, catalog-metadata, `portolan` subprocess helpers.
"""

from ._json import read_catalog, read_json_state, write_json_state
from ._service import CATALOG_TITLE, _ensure_root_catalog
from ._subprocess import _portolan, portolan_add
from ._top import (
    _push_catalog_files,
    push_top_catalog,
    sync_tree_deletions,
    write_top_catalog,
)
from ._tree import (
    ADM_SUFFIXES,
    admin_layer_pattern,
    admin_layers,
    file_fingerprint,
    iter_version_dirs,
    remove_stale_versions,
)

__all__ = [
    "ADM_SUFFIXES",
    "CATALOG_TITLE",
    "_ensure_root_catalog",
    "_portolan",
    "_push_catalog_files",
    "admin_layer_pattern",
    "admin_layers",
    "file_fingerprint",
    "iter_version_dirs",
    "portolan_add",
    "push_top_catalog",
    "read_catalog",
    "read_json_state",
    "remove_stale_versions",
    "sync_tree_deletions",
    "write_json_state",
    "write_top_catalog",
]
