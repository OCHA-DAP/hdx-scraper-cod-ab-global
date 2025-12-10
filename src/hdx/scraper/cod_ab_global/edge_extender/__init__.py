from pathlib import Path
from shutil import rmtree
from venv import logger

from . import attempt, cleanup, inputs, lines, merge, outputs
from .config import distance, num_threads, quiet
from .utils import apply_funcs

funcs = [inputs.main, lines.main, attempt.main, merge.main, outputs.main, cleanup.main]


def edge_extender(data_dir: Path) -> None:
    """Run main function."""
    input_dir = data_dir / "country/extended_pre"
    if not quiet:
        logger.info(f"--distance={distance} --num-threads={num_threads}")
    for file in sorted(input_dir.glob("*.parquet")):
        name = file.name.replace(".", "_")
        args = [name, file, file.stem, *funcs]
        apply_funcs(*args)
    if not quiet:
        logger.info("done")
    rmtree(input_dir)
