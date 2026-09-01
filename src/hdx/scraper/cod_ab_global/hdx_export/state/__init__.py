"""Fingerprint-based skip logic for HDX resource rebuilds.

State is stored sibling to `.bnda`, outside any of the four catalog trees.
"""

from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import read_json_state, write_json_state

from ._fingerprint import build_fingerprint

__all__ = [
    "build_fingerprint",
    "is_push_stale",
    "is_stale",
    "record",
    "record_push",
]

_STATE_FILE = "state.json"
_PUSH_STATE_FILE = "push_state.json"


def _state_path(work_dir: Path) -> Path:
    state_dir = work_dir.parent / ".hdx_export"
    state_dir.mkdir(exist_ok=True)
    return state_dir / _STATE_FILE


def _push_state_path(work_dir: Path) -> Path:
    state_dir = work_dir.parent / ".hdx_export"
    state_dir.mkdir(exist_ok=True)
    return state_dir / _PUSH_STATE_FILE


def is_stale(
    work_dir: Path, scope: str, label: str, fingerprint: dict, output_path: Path
) -> bool:
    """Return True if `scope`/`label` needs a rebuild."""
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

    Tracked separately from `is_stale`'s local-build state, keyed by site_url.
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
