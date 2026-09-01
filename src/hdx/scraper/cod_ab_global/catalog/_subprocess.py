"""Run the `portolan` CLI as a subprocess against a catalog directory."""

import logging
import sys
from pathlib import Path
from subprocess import CalledProcessError
from subprocess import run as _run

logger = logging.getLogger(__name__)

_PORTOLAN = str(Path(sys.executable).parent / "portolan")


def _portolan(args: list[str], cwd: Path) -> None:
    _run([_PORTOLAN, *args], cwd=cwd, check=True)


def portolan_add(
    cwd: Path, rel_path: str, workers: str, *, datetime_: str | None = None
) -> None:
    """Run `portolan add` for one service dir, logging (not raising) on failure."""
    args = ["add", rel_path, "--workers", workers, "--pmtiles", "--force"]
    if datetime_:
        args += ["--datetime", datetime_]
    try:
        _portolan(args, cwd=cwd)
    except CalledProcessError:
        logger.warning("portolan add failed for %s (continuing)", rel_path)
