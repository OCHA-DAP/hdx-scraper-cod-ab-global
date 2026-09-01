"""Build (and publish) HDX resources from original/, extended/, and matched/.

Build state and push state are tracked independently; see state/.
"""

from pathlib import Path

from . import state
from ._build import build_boundaries, build_metadata_resource, build_pcodes_resource
from ._push import push as _push


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
    boundary_results = build_boundaries(
        original_dir, extended_dir, matched_dir, work_dir, output_dir
    )
    pcodes_result = build_pcodes_resource(original_dir, work_dir, output_dir)
    metadata_result = build_metadata_resource(original_dir, work_dir, output_dir)

    if not push_to_hdx:
        # Scratch mode: a successful build is itself "done", record now.
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
