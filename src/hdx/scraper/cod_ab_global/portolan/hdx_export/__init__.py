"""Build (and publish) HDX resources from the portolan catalog.

Fifth pipeline stage, run after global_.py. Independently re-runnable
without touching extraction/extension/matching — every input is already on
disk in the portolan catalog. See the approved migration plan
(ok-i-m-ready-for-melodic-heron.md) for full context.

State is only recorded (via `state.record`) after a resource's fingerprint
has actually taken effect: immediately after a successful build in scratch
mode (`push_to_hdx=False`), or after a successful HDX push when
`push_to_hdx=True`. This way a failed push doesn't get silently treated as
"already done" on the next run — it retries both the push (and, if the
underlying data changed again, the build) rather than skipping forever.
"""

import logging
import uuid
from pathlib import Path

from hdx.scraper.cod_ab_global.dataset.boundaries import create_boundaries_dataset
from hdx.scraper.cod_ab_global.dataset.pcodes import create_pcodes_dataset

from . import state
from .boundaries import FINGERPRINT_KEYS, build_boundaries_gdb
from .metadata import FINGERPRINT_KEY as METADATA_FINGERPRINT_KEY
from .metadata import build_metadata
from .pcodes import FINGERPRINT_KEY as PCODES_FINGERPRINT_KEY
from .pcodes import build_pcodes
from .services import iter_included_version_dirs

logger = logging.getLogger(__name__)

_RUN_VERSIONS = ("latest", "historic")


def _build_boundaries(
    work_dir: Path, output_dir: Path
) -> dict[tuple[str, str], tuple[bool, dict]]:
    """Build each (stage, run_version) GDB if stale.

    Returns {(stage, run_version): (rebuilt, fingerprint)}.
    """
    results: dict[tuple[str, str], tuple[bool, dict]] = {}
    for run_version in _RUN_VERSIONS:
        version_dirs = iter_included_version_dirs(work_dir, run_version)
        for stage, fingerprint_key in FINGERPRINT_KEYS.items():
            output_path = (
                output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb.zip"
            )
            fingerprint = state.build_fingerprint(version_dirs, fingerprint_key)
            if state.is_stale(work_dir, stage, run_version, fingerprint, output_path):
                logger.info("Rebuilding %s/%s", stage, run_version)
                build_boundaries_gdb(work_dir, run_version, stage, output_dir)
                results[stage, run_version] = (True, fingerprint)
            else:
                logger.info("Skipping unchanged %s/%s", stage, run_version)
                results[stage, run_version] = (False, fingerprint)
    return results


def _build_pcodes(work_dir: Path, output_dir: Path) -> tuple[bool, dict]:
    """Build pcodes if stale. Returns (rebuilt, fingerprint)."""
    output_path = output_dir / "pcodes" / "global_pcodes.parquet"
    version_dirs = iter_included_version_dirs(work_dir, "latest")
    fingerprint = state.build_fingerprint(version_dirs, PCODES_FINGERPRINT_KEY)
    if not state.is_stale(work_dir, "pcodes", "latest", fingerprint, output_path):
        logger.info("Skipping unchanged pcodes")
        return False, fingerprint
    build_pcodes(work_dir, output_dir)
    return True, fingerprint


def _build_metadata(work_dir: Path, output_dir: Path) -> tuple[bool, dict]:
    """Build metadata if stale. Returns (rebuilt, fingerprint)."""
    metadata_dir = output_dir / "metadata"
    output_path = metadata_dir / "global_admin_boundaries_metadata_all.parquet"
    # build_metadata() itself combines latest+historic in one pass (see
    # metadata.py) — the fingerprint must cover the same combined scope, not
    # just one run_version, or a historic-only change would go undetected.
    version_dirs = [
        *iter_included_version_dirs(work_dir, "latest"),
        *iter_included_version_dirs(work_dir, "historic"),
    ]
    fingerprint = state.build_fingerprint(version_dirs, METADATA_FINGERPRINT_KEY)
    if not state.is_stale(work_dir, "metadata", "all", fingerprint, output_path):
        logger.info("Skipping unchanged metadata")
        return False, fingerprint
    build_metadata(work_dir, metadata_dir / "global_admin_boundaries_metadata.parquet")
    return True, fingerprint


def _push(
    work_dir: Path,
    output_dir: Path,
    boundary_results: dict[tuple[str, str], tuple[bool, dict]],
    pcodes_result: tuple[bool, dict],
    metadata_result: tuple[bool, dict],
) -> None:
    """Push whichever HDX datasets have a changed resource, then record state.

    Metadata's CSV is a 4th resource bundled into each run_version's
    boundaries dataset (see dataset/boundaries.py), not pushed separately —
    so a run_version is pushed if ANY of its 3 stages OR metadata changed.
    """
    batch = str(uuid.uuid4())
    info = {"batch": batch}
    metadata_rebuilt, metadata_fingerprint = metadata_result

    for run_version in _RUN_VERSIONS:
        stage_rebuilt = {
            stage: boundary_results[stage, run_version][0] for stage in FINGERPRINT_KEYS
        }
        if not (any(stage_rebuilt.values()) or metadata_rebuilt):
            logger.info("Nothing changed for %s — skipping HDX push", run_version)
            continue
        logger.info("Pushing %s boundaries dataset to HDX", run_version)
        create_boundaries_dataset(output_dir, run_version, info)
        for stage, rebuilt in stage_rebuilt.items():
            if rebuilt:
                _, fingerprint = boundary_results[stage, run_version]
                state.record(work_dir, stage, run_version, fingerprint)

    if metadata_rebuilt:
        state.record(work_dir, "metadata", "all", metadata_fingerprint)

    pcodes_rebuilt, pcodes_fingerprint = pcodes_result
    if pcodes_rebuilt:
        logger.info("Pushing pcodes dataset to HDX")
        create_pcodes_dataset(output_dir, info)
        state.record(work_dir, "pcodes", "latest", pcodes_fingerprint)


def run(work_dir: Path, output_dir: Path, *, push_to_hdx: bool = False) -> None:
    """Build all HDX-ready resources from the portolan catalog.

    push_to_hdx: when True, also upload changed resources to HDX (the
    currently-configured HDX site — set up your `.hdx_configuration.yaml`
    or HDX_SITE/HDX_KEY env vars to point at staging before running this
    against anything other than production).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    boundary_results = _build_boundaries(work_dir, output_dir)
    pcodes_result = _build_pcodes(work_dir, output_dir)
    metadata_result = _build_metadata(work_dir, output_dir)

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

    _push(work_dir, output_dir, boundary_results, pcodes_result, metadata_result)
