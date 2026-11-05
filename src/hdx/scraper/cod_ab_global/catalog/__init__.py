"""Shared portolan-catalog infrastructure used by original/extended/matched/global/.

Tree-walking, fingerprinting, catalog-metadata, `portolan` subprocess helpers.
"""

from ._json import read_catalog, read_json_state, write_json_state
from ._pmtiles import build_pmtiles, tile_parquet
from ._service import CATALOG_TITLE, _ensure_root_catalog
from ._subprocess import _portolan, portolan_add
from ._top import (
    push_top_catalog,
    sync_tree,
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
from ._verify import remove_nested_index_versions

__all__ = [
    "ADM_SUFFIXES",
    "CATALOG_TITLE",
    "_ensure_root_catalog",
    "_portolan",
    "admin_layer_pattern",
    "admin_layers",
    "build_pmtiles",
    "file_fingerprint",
    "iter_version_dirs",
    "portolan_add",
    "push_top_catalog",
    "read_catalog",
    "read_json_state",
    "remove_nested_index_versions",
    "remove_stale_versions",
    "sync_tree",
    "tile_parquet",
    "write_json_state",
    "write_top_catalog",
]
