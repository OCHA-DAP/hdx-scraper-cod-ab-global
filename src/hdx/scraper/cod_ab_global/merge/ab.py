from pathlib import Path
from subprocess import run


def main(data_dir: Path) -> None:
    """Generate edge-matched boundaries for all datasets."""
    layers = ["0", "1", "2", "3", "4"]
    for layer in layers:
        input_path = sorted((data_dir / "versioned").rglob(f"*_admin{layer}.parquet"))
        output_path = data_dir / "versioned" / f"admin{layer}.parquet"
        run(
            [
                *["gdal", "vector", "concat"],
                *[*input_path, output_path],
                "--overwrite",
                "--mode=single",
                "--lco=COMPRESSION=ZSTD",
            ],
            check=False,
        )
