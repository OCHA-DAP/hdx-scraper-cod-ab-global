"""Build each HDX resource (boundaries/pcodes/metadata) if its input changed."""

import logging
from pathlib import Path

from . import state
from .boundaries import build_boundaries_gdb
from .metadata import build_metadata
from .pcodes import build_pcodes
from .services import iter_included_version_dirs

logger = logging.getLogger(__name__)

RUN_VERSIONS = ("latest", "historic")


def build_boundaries(
    original_dir: Path,
    extended_dir: Path,
    matched_dir: Path,
    work_dir: Path,
    output_dir: Path,
) -> dict[tuple[str, str], tuple[bool, dict]]:
    """Build each (stage, run_version) GDB if stale.

    Returns {(stage, run_version): (rebuilt, fingerprint)}.
    """
    roots = {"original": original_dir, "extended": extended_dir, "matched": matched_dir}
    results: dict[tuple[str, str], tuple[bool, dict]] = {}
    for run_version in RUN_VERSIONS:
        for stage, root_dir in roots.items():
            version_dirs = iter_included_version_dirs(root_dir, run_version)
            output_path = (
                output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb.zip"
            )
            fingerprint = state.build_fingerprint(version_dirs)
            if state.is_stale(work_dir, stage, run_version, fingerprint, output_path):
                logger.info("Rebuilding %s/%s", stage, run_version)
                build_boundaries_gdb(version_dirs, run_version, stage, output_dir)
                results[stage, run_version] = (True, fingerprint)
            else:
                logger.info("Skipping unchanged %s/%s", stage, run_version)
                results[stage, run_version] = (False, fingerprint)
    return results


def build_pcodes_resource(
    original_dir: Path, work_dir: Path, output_dir: Path
) -> tuple[bool, dict]:
    """Build pcodes if stale. Returns (rebuilt, fingerprint)."""
    output_path = output_dir / "pcodes" / "global_pcodes.parquet"
    version_dirs = iter_included_version_dirs(original_dir, "latest")
    fingerprint = state.build_fingerprint(version_dirs)
    if not state.is_stale(work_dir, "pcodes", "latest", fingerprint, output_path):
        logger.info("Skipping unchanged pcodes")
        return False, fingerprint
    build_pcodes(original_dir, output_dir)
    return True, fingerprint


def build_metadata_resource(
    original_dir: Path, work_dir: Path, output_dir: Path
) -> tuple[bool, dict]:
    """Build metadata if stale. Returns (rebuilt, fingerprint)."""
    metadata_dir = output_dir / "metadata"
    output_path = metadata_dir / "global_admin_boundaries_metadata_all.parquet"
    version_dirs = [
        *iter_included_version_dirs(original_dir, "latest"),
        *iter_included_version_dirs(original_dir, "historic"),
    ]
    fingerprint = state.build_fingerprint(version_dirs)
    if not state.is_stale(work_dir, "metadata", "all", fingerprint, output_path):
        logger.info("Skipping unchanged metadata")
        return False, fingerprint
    build_metadata(
        original_dir, metadata_dir / "global_admin_boundaries_metadata.parquet"
    )
    return True, fingerprint
