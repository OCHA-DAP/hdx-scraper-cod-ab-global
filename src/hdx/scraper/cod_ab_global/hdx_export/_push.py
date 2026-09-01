"""Push whichever HDX datasets haven't reached a site with their fingerprint."""

import logging
import uuid
from pathlib import Path

from hdx.scraper.cod_ab_global.dataset.boundaries import create_boundaries_dataset
from hdx.scraper.cod_ab_global.dataset.pcodes import create_pcodes_dataset

from . import state
from ._build import RUN_VERSIONS

logger = logging.getLogger(__name__)

_STAGES = ("original", "extended", "matched")


def push(  # noqa: PLR0913
    work_dir: Path,
    output_dir: Path,
    site_url: str,
    boundary_results: dict[tuple[str, str], tuple[bool, dict]],
    pcodes_result: tuple[bool, dict],
    metadata_result: tuple[bool, dict],
) -> None:
    """Push driven by state.is_push_stale, not this run's `rebuilt` flags."""
    batch = str(uuid.uuid4())
    info = {"batch": batch}
    _, metadata_fingerprint = metadata_result
    metadata_needs_push = state.is_push_stale(
        work_dir, site_url, "metadata", "all", metadata_fingerprint
    )

    for run_version in RUN_VERSIONS:
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
