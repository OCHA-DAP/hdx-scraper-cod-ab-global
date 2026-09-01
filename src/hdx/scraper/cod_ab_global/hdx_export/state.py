"""Fingerprint-based skip logic for HDX resource rebuilds.

State is stored sibling to `.bnda`, outside any of the four catalog trees.
"""

from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import (
    admin_layers,
    file_fingerprint,
    read_json_state,
    write_json_state,
)
from hdx.scraper.cod_ab_global.config import iso3_exclude, iso3_include

_STATE_FILE = "state.json"
_PUSH_STATE_FILE = "push_state.json"


def _deepest_layer_fingerprint(version_dir: Path, iso3: str) -> list[int] | None:
    """Return [level, size, mtime_ns] for the deepest existing admin parquet."""
    layers = admin_layers(version_dir, iso3)
    if not layers:
        return None
    level, layer_dir = layers[-1]
    fp = file_fingerprint(layer_dir / f"{layer_dir.name}.parquet")
    return [level, *fp] if fp else None


def _state_path(work_dir: Path) -> Path:
    state_dir = work_dir.parent / ".hdx_export"
    state_dir.mkdir(exist_ok=True)
    return state_dir / _STATE_FILE


def _push_state_path(work_dir: Path) -> Path:
    state_dir = work_dir.parent / ".hdx_export"
    state_dir.mkdir(exist_ok=True)
    return state_dir / _PUSH_STATE_FILE


def build_fingerprint(version_dirs: list[tuple[str, Path]]) -> dict:
    """Build a content fingerprint from the version_dirs a builder will process."""
    services_fp = {}
    for iso3, version_dir in version_dirs:
        fp = _deepest_layer_fingerprint(version_dir, iso3)
        if fp is not None:
            services_fp[f"{iso3}/{version_dir.name}"] = fp
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


def is_push_stale(
    work_dir: Path, site_url: str, scope: str, label: str, fingerprint: dict
) -> bool:
    """Return True if `fingerprint` has never been pushed to `site_url`.

    Tracked in a separate file from `is_stale`'s local-build state, keyed by
    site_url — a scratch-mode build (push_to_hdx=False) only ever updates
    build state, so it can never make this return False for a site that
    hasn't actually received the content.
    """
    stored = read_json_state(_push_state_path(work_dir))
    return stored.get(f"{site_url}|{scope}:{label}") != fingerprint


def record_push(
    work_dir: Path, site_url: str, scope: str, label: str, fingerprint: dict
) -> None:
    """Record that `scope`/`label` was just successfully pushed to `site_url`."""
    path = _push_state_path(work_dir)
    stored = read_json_state(path)
    stored[f"{site_url}|{scope}:{label}"] = fingerprint
    write_json_state(path, stored)
