"""Build (and publish) HDX resources from original/, extended/, and matched/.

Build state and push state are tracked independently; see state.py.
"""

import logging
import uuid
from pathlib import Path

from hdx.scraper.cod_ab_global.dataset.boundaries import create_boundaries_dataset
from hdx.scraper.cod_ab_global.dataset.pcodes import create_pcodes_dataset

from . import state
from .boundaries import build_boundaries_gdb
from .metadata import build_metadata
from .pcodes import build_pcodes
from .services import iter_included_version_dirs

logger = logging.getLogger(__name__)

_RUN_VERSIONS = ("latest", "historic")
_STAGES = ("original", "extended", "matched")


def _build_boundaries(
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
    for run_version in _RUN_VERSIONS:
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


def _build_pcodes(
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


def _build_metadata(
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


def _push(  # noqa: PLR0913
    work_dir: Path,
    output_dir: Path,
    site_url: str,
    boundary_results: dict[tuple[str, str], tuple[bool, dict]],
    pcodes_result: tuple[bool, dict],
    metadata_result: tuple[bool, dict],
) -> None:
    """Push whichever HDX datasets haven't reached `site_url` with their fingerprint.

    Driven by state.is_push_stale, not this run's `rebuilt` flags — a
    resource built in an earlier scratch-mode run still needs pushing here
    if this site has never received its current fingerprint. Metadata's CSV
    is a 4th resource bundled into each run_version's boundaries dataset
    (see dataset/boundaries.py), not pushed separately — so a run_version is
    pushed if ANY of its 3 stages OR metadata needs pushing.
    """
    batch = str(uuid.uuid4())
    info = {"batch": batch}
    _, metadata_fingerprint = metadata_result
    metadata_needs_push = state.is_push_stale(
        work_dir, site_url, "metadata", "all", metadata_fingerprint
    )

    for run_version in _RUN_VERSIONS:
        stage_fingerprints = {
            stage: boundary_results[stage, run_version][1] for stage in _STAGES
        }
        stage_needs_push = {
            stage: state.is_push_stale(work_dir, site_url, stage, run_version, fp)
            for stage, fp in stage_fingerprints.items()
        }
        if not (any(stage_needs_push.values()) or metadata_needs_push):
            logger.info("Nothing new to push for %s", run_version)
            continue
        logger.info("Pushing %s boundaries dataset to HDX", run_version)
        create_boundaries_dataset(output_dir, run_version, info)
        for stage, needs_push in stage_needs_push.items():
            if needs_push:
                fingerprint = stage_fingerprints[stage]
                state.record(work_dir, stage, run_version, fingerprint)
                state.record_push(work_dir, site_url, stage, run_version, fingerprint)

    if metadata_needs_push:
        state.record(work_dir, "metadata", "all", metadata_fingerprint)
        state.record_push(work_dir, site_url, "metadata", "all", metadata_fingerprint)

    pcodes_fingerprint = pcodes_result[1]
    if state.is_push_stale(work_dir, site_url, "pcodes", "latest", pcodes_fingerprint):
        logger.info("Pushing pcodes dataset to HDX")
        create_pcodes_dataset(output_dir, info)
        state.record(work_dir, "pcodes", "latest", pcodes_fingerprint)
        state.record_push(work_dir, site_url, "pcodes", "latest", pcodes_fingerprint)


def run(  # noqa: PLR0913
    original_dir: Path,
    extended_dir: Path,
    matched_dir: Path,
    work_dir: Path,
    output_dir: Path,
    *,
    push_to_hdx: bool = False,
) -> None:
    """Build all HDX-ready resources from original/, extended/, and matched/."""
    output_dir.mkdir(parents=True, exist_ok=True)
    boundary_results = _build_boundaries(
        original_dir, extended_dir, matched_dir, work_dir, output_dir
    )
    pcodes_result = _build_pcodes(original_dir, work_dir, output_dir)
    metadata_result = _build_metadata(original_dir, work_dir, output_dir)

    if not push_to_hdx:
        # Scratch mode: a successful build is itself "done" — record now.
        for (stage, run_version), (rebuilt, fingerprint) in boundary_results.items():
            if rebuilt:
                state.record(work_dir, stage, run_version, fingerprint)
        if pcodes_result[0]:
            state.record(work_dir, "pcodes", "latest", pcodes_result[1])
        if metadata_result[0]:
            state.record(work_dir, "metadata", "all", metadata_result[1])
        return

    from hdx.api.configuration import Configuration  # noqa: PLC0415

    site_url = Configuration.read().get_hdx_site_url()
    _push(
        work_dir, output_dir, site_url, boundary_results, pcodes_result, metadata_result
    )
