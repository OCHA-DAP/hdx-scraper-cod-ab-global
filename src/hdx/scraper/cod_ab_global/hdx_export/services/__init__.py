"""Resolve which portolan catalog services belong to each HDX run_version.

Applies ISO3_INCLUDE/ISO3_EXCLUDE on top of latest-vs-historic partitioning.
"""

from pathlib import Path

from ._filter import _apply_iso3_filter
from ._partition import _partition_by_version


def resolve_services(root_dir: Path, run_version: str) -> dict[str, list[Path]]:
    """Return {iso3: [version_dir, ...]} in scope for one run_version.

    run_version: "latest" (one version_dir per iso3) or "historic" (zero or
    more version_dirs per iso3, every version below the highest).
    """
    latest, historic = _partition_by_version(root_dir)
    filtered_latest, filtered_historic = _apply_iso3_filter(latest, historic)
    if run_version == "latest":
        return {iso3: [d] for iso3, d in filtered_latest.items()}
    return {iso3: dirs for iso3, dirs in filtered_historic.items() if dirs}


def iter_included_version_dirs(
    root_dir: Path, run_version: str
) -> list[tuple[str, Path]]:
    """Return sorted [(iso3, version_dir), ...] in scope for one run_version."""
    grouped = resolve_services(root_dir, run_version)
    return sorted(
        (iso3, version_dir) for iso3, dirs in grouped.items() for version_dir in dirs
    )
