"""Resolve which portolan catalog services belong to each HDX run_version.

Latest/historic is derived purely from directory version numbering: for
each iso3, the highest v{NN} directory is "latest"; all lower v{NN}
directories are "historic". Unversioned "latest/" service directories are
excluded from assembly entirely — verified they always mirror the highest
v{NN} directory's cod_ab:* metadata, so including both would double-count
the same country.

ISO3 include/exclude filtering (ISO3_INCLUDE/ISO3_EXCLUDE) is applied here,
at HDX-assembly time, over the on-disk catalog listing only — it must never
be wired into portolan/utils.py::list_services(), which stays a full,
unconditional mirror of every ArcGIS service to source.coop.
"""

import re
from pathlib import Path

from hdx.scraper.cod_ab_global.config import iso3_exclude, iso3_include

_ISO3_LEN = 3
_VERSION_RE = re.compile(r"^v(\d+)$")


def _iter_version_dirs(work_dir: Path) -> list[tuple[str, int, Path]]:
    """Return [(iso3, version_num, version_dir), ...] for every versioned service."""
    result = []
    for country_dir in sorted(work_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        if country_dir.name == "wld":
            continue
        iso3 = country_dir.name
        for version_dir in sorted(country_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            match = _VERSION_RE.match(version_dir.name)
            if match:
                result.append((iso3, int(match.group(1)), version_dir))
    return result


def _partition_by_version(
    work_dir: Path,
) -> tuple[dict[str, Path], dict[str, list[Path]]]:
    """Return (latest, historic) service dirs per iso3, before ISO3 filtering.

    latest: {iso3: version_dir} for the highest vNN per country.
    historic: {iso3: [version_dir, ...]} for all lower vNN, ascending order.
    """
    by_iso3: dict[str, list[tuple[int, Path]]] = {}
    for iso3, num, version_dir in _iter_version_dirs(work_dir):
        by_iso3.setdefault(iso3, []).append((num, version_dir))

    latest: dict[str, Path] = {}
    historic: dict[str, list[Path]] = {}
    for iso3, versions in by_iso3.items():
        versions.sort(key=lambda v: v[0])
        historic[iso3] = [v[1] for v in versions[:-1]]
        latest[iso3] = versions[-1][1]
    return latest, historic


def _iso3_version_key(iso3_upper: str, version_dir: Path) -> str:
    """Return e.g. 'AFGv01' — matches the old refactor.py exclude-key format."""
    return f"{iso3_upper}{version_dir.name}"


def _keep_version_dir(
    iso3: str,
    version_dir: Path,
    include_all: set[str],
    exclude_all: set[str],
    exclude_version: set[str],
) -> bool:
    """Return True if this (iso3, version_dir) survives ISO3_INCLUDE/EXCLUDE."""
    iso3_upper = iso3.upper()
    if include_all and iso3_upper not in include_all:
        return False
    if iso3_upper in exclude_all:
        return False
    return _iso3_version_key(iso3_upper, version_dir) not in exclude_version


def _apply_iso3_filter(
    latest: dict[str, Path], historic: dict[str, list[Path]]
) -> tuple[dict[str, Path], dict[str, list[Path]]]:
    """Apply ISO3_INCLUDE/ISO3_EXCLUDE, including version-pinned excludes.

    Mirrors download/metadata/refactor.py::_df_filter's semantics exactly
    (e.g. "AFG_v01" excludes only that one version of Afghanistan). Portolan
    directory names are lowercase; ISO3_INCLUDE/ISO3_EXCLUDE env values are
    uppercased by config.py — compare on the uppercased iso3 throughout.
    """
    include_all = {x for x in iso3_include if len(x) == _ISO3_LEN}
    exclude_all = {x for x in iso3_exclude if len(x) == _ISO3_LEN}
    exclude_version = {x.replace("_V", "v") for x in iso3_exclude if "_V" in x}

    filtered_latest = {
        iso3: d
        for iso3, d in latest.items()
        if _keep_version_dir(iso3, d, include_all, exclude_all, exclude_version)
    }
    filtered_historic = {
        iso3: [
            d
            for d in dirs
            if _keep_version_dir(iso3, d, include_all, exclude_all, exclude_version)
        ]
        for iso3, dirs in historic.items()
    }
    return filtered_latest, filtered_historic


def resolve_services(work_dir: Path, run_version: str) -> dict[str, list[Path]]:
    """Return {iso3: [version_dir, ...]} in scope for one run_version.

    run_version: "latest" (one version_dir per iso3) or "historic" (zero or
    more version_dirs per iso3 — every version below the highest).
    """
    latest, historic = _partition_by_version(work_dir)
    filtered_latest, filtered_historic = _apply_iso3_filter(latest, historic)
    if run_version == "latest":
        return {iso3: [d] for iso3, d in filtered_latest.items()}
    return {iso3: dirs for iso3, dirs in filtered_historic.items() if dirs}


def iter_included_version_dirs(
    work_dir: Path, run_version: str
) -> list[tuple[str, Path]]:
    """Return sorted [(iso3, version_dir), ...] in scope for one run_version."""
    grouped = resolve_services(work_dir, run_version)
    return sorted(
        (iso3, version_dir) for iso3, dirs in grouped.items() for version_dir in dirs
    )
