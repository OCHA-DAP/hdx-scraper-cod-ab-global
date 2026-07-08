"""Fingerprint-based skip logic for HDX resource rebuilds.

Replaces dataset/boundaries_utils.py::compare_gdb's remote-download-and-hash
approach (which paid a network + GDAL-conversion cost every run just to
avoid a spurious HDX "last modified" bump) with a cheap local fingerprint
check, following the same pattern as portolan/global_.py's
`.global_state.json`: skip rebuilding (and re-uploading) a resource
entirely when nothing in its scope has changed since the last successful
build.

State is stored outside the portolan catalog tree — sibling to `.bnda` — so
`portolan push`/`aws s3 sync` never touches it.

Callers pass the upstream `cod_ab:*_updated` field their resource actually
depends on (see portolan/extended.py and portolan/matched.py's own
change-detection for what triggers each stage's reprocessing) rather than a
scope-name that gets looked up internally — this keeps each caller's
dependency explicit instead of encoding it in a second, easy-to-forget
lookup table.
"""

from pathlib import Path

from hdx.scraper.cod_ab_global.config import iso3_exclude, iso3_include
from hdx.scraper.cod_ab_global.portolan.original import (
    read_catalog,
    read_json_state,
    write_json_state,
)

_STATE_FILE = "state.json"


def _state_path(work_dir: Path) -> Path:
    state_dir = work_dir.parent / ".hdx_export"
    state_dir.mkdir(exist_ok=True)
    return state_dir / _STATE_FILE


def build_fingerprint(
    version_dirs: list[tuple[str, Path]], fingerprint_key: str
) -> dict:
    """Build a fingerprint from the exact version_dirs a builder will process.

    fingerprint_key: the catalog.json field whose change is exactly what
    triggers this resource's own upstream reprocessing, e.g.
    "cod_ab:original_updated" or "cod_ab:extended_updated". Pass the same
    version_dirs list the builder itself iterates (e.g.
    `iter_included_version_dirs(work_dir, run_version)`, or a concatenation
    of latest+historic for a resource that spans both) so the fingerprint's
    scope always matches what actually gets built — see metadata.py, whose
    builder combines both run_versions in one pass.
    Includes the resolved ISO3 include/exclude filter state so a
    filter-only change (no underlying data change) still triggers a rebuild.
    """
    services_fp = {
        f"{iso3}/{version_dir.name}": read_catalog(version_dir).get(fingerprint_key)
        for iso3, version_dir in version_dirs
    }
    return {
        "services": services_fp,
        "iso3_include": sorted(iso3_include),
        "iso3_exclude": sorted(iso3_exclude),
    }


def is_stale(
    work_dir: Path, scope: str, label: str, fingerprint: dict, output_path: Path
) -> bool:
    """Return True if `scope`/`label` must be rebuilt.

    True when `fingerprint` differs from the last recorded build, or the
    previous output no longer exists on disk. Callers build the fingerprint
    once (via `build_fingerprint`) and pass it to both this check and the
    later `record` call, rather than it being recomputed twice. `label`
    namespaces the state entry (e.g. a run_version, or "all") — it doesn't
    need to correspond to anything `build_fingerprint` understands.
    """
    if not output_path.exists():
        return True
    stored = read_json_state(_state_path(work_dir))
    return stored.get(f"{scope}:{label}") != fingerprint


def record(work_dir: Path, scope: str, label: str, fingerprint: dict) -> None:
    """Record that `scope`/`label` was just successfully rebuilt."""
    path = _state_path(work_dir)
    stored = read_json_state(path)
    stored[f"{scope}:{label}"] = fingerprint
    write_json_state(path, stored)
