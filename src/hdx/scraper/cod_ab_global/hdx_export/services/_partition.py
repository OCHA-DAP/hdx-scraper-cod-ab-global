"""Partition versioned service directories into latest/historic per iso3."""

import re
from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import (
    iter_version_dirs as _iter_all_version_dirs,
)

_VERSION_RE = re.compile(r"^v(\d+)$")


def _iter_version_dirs(root_dir: Path) -> list[tuple[str, int, Path]]:
    """Return [(iso3, version_num, version_dir), ...] for every versioned service."""
    result = []
    for iso3, version, version_dir in _iter_all_version_dirs(root_dir):
        match = _VERSION_RE.match(version)
        if match:
            result.append((iso3, int(match.group(1)), version_dir))
    return result


def _partition_by_version(
    root_dir: Path,
) -> tuple[dict[str, Path], dict[str, list[Path]]]:
    """Return (latest, historic) service dirs per iso3, before ISO3 filtering.

    latest: {iso3: version_dir} for the highest vNN per country.
    historic: {iso3: [version_dir, ...]} for all lower vNN, ascending order.
    """
    by_iso3: dict[str, list[tuple[int, Path]]] = {}
    for iso3, num, version_dir in _iter_version_dirs(root_dir):
        by_iso3.setdefault(iso3, []).append((num, version_dir))

    latest: dict[str, Path] = {}
    historic: dict[str, list[Path]] = {}
    for iso3, versions in by_iso3.items():
        versions.sort(key=lambda v: v[0])
        historic[iso3] = [v[1] for v in versions[:-1]]
        latest[iso3] = versions[-1][1]
    return latest, historic
