"""Walk a portolan catalog tree and fingerprint its admin-layer parquets."""

import logging
import re
from collections.abc import Iterator
from pathlib import Path
from subprocess import CalledProcessError

from ._subprocess import _portolan

logger = logging.getLogger(__name__)

ADM_SUFFIXES = ("_name", "_name1", "_name2", "_name3", "_pcode")


def admin_layer_pattern(iso3: str) -> re.Pattern[str]:
    """Return the regex matching native admin-polygon layer dirs for one ISO3."""
    return re.compile(rf"^{re.escape(iso3.lower())}_admin(\d+)$")


def iter_version_dirs(root_dir: Path) -> Iterator[tuple[str, str, Path]]:
    """Yield (iso3, version, version_dir) for every non-hidden service dir."""
    for country_dir in sorted(root_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        for version_dir in sorted(country_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            yield country_dir.name, version_dir.name, version_dir


def admin_layers(version_dir: Path, iso3: str) -> list[tuple[int, Path]]:
    """Return [(level, layer_dir), ...] with an existing parquet, sorted by level."""
    if not version_dir.exists():
        return []
    pattern = admin_layer_pattern(iso3)
    found = []
    for d in version_dir.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if m and (d / f"{d.name}.parquet").exists():
            found.append((int(m.group(1)), d))
    return sorted(found)


def file_fingerprint(path: Path) -> list[int] | None:
    """Return [size, mtime_ns] for a file, or None if it doesn't exist."""
    if not path.exists():
        return None
    stat = path.stat()
    return [stat.st_size, stat.st_mtime_ns]


def remove_stale_versions(valid_pairs: set[tuple[str, str]], work_dir: Path) -> None:
    """Remove {iso3}/{version}/ directories not in valid_pairs (portolan rm)."""
    for iso3, version, _ in iter_version_dirs(work_dir):
        if (iso3, version) not in valid_pairs:
            logger.info("Removing stale service %s/%s", iso3, version)
            try:
                _portolan(["rm", "--force", f"{iso3}/{version}/"], cwd=work_dir)
            except CalledProcessError:
                logger.warning("portolan rm failed for %s/%s", iso3, version)
