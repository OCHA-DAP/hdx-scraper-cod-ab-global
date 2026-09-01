"""Apply ISO3_INCLUDE/ISO3_EXCLUDE filtering to partitioned service directories."""

from pathlib import Path

from hdx.scraper.cod_ab_global.config import iso3_exclude, iso3_include

_ISO3_LEN = 3


def _iso3_version_key(iso3_upper: str, version_dir: Path) -> str:
    """Return e.g. 'AFGv01', matching the old refactor.py exclude-key format."""
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

    Portolan directory names are lowercase; ISO3_INCLUDE/ISO3_EXCLUDE env values
    are uppercased by config.py, compare on the uppercased iso3 throughout.
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
