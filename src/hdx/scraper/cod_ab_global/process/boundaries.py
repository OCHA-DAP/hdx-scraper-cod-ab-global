from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run

from tqdm import tqdm


def create_boundaries(data_dir: Path, run_version: str, stage: str) -> None:
    """Generate global boundaries for datasets."""
    output_dir = data_dir / "global"
    output_path = output_dir / f"global_admin_boundaries_{stage}_{run_version}.gdb"
    rmtree(output_path, ignore_errors=True)
    output_path.mkdir(parents=True, exist_ok=True)
    input_paths = sorted(
        (data_dir / "country" / stage).rglob("*.parquet"),
    )
    for input_path in tqdm(sorted(input_paths)):
        mode = ["--append"] if (output_path / output_path.name).exists() else []
        output_layer = f"admin{input_path.stem[-1]}"
        run(
            [
                *["gdal", "vector", "convert"],
                *[input_path, output_path / output_path.name],
                *mode,
                "--quiet",
                f"--output-layer={output_layer}",
            ],
            check=True,
            capture_output=True,
        )
    make_archive(str(output_path), "zip", output_path)
    rmtree(output_path)
