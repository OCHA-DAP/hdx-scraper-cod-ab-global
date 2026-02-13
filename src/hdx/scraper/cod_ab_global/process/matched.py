from pathlib import Path
from shutil import rmtree
from subprocess import run

from ..config import gdal_parquet_options
from ..utils import get_columns


def _gdal_clip(input_path: Path, output_path: Path, clip_path: Path) -> None:
    """Dissolve an admin level down."""
    iso3 = input_path.stem.split("_")[0].upper()
    admin_level = int(input_path.stem[-1])
    columns = ",".join(get_columns(admin_level))
    run(
        [
            *["gdal", "vector", "pipeline"],
            *["read", input_path, "!"],
            *["clip", f"--like={clip_path}", f"--like-where=iso3cd='{iso3}'", "!"],
            *[
                "sql",
                "--dialect=SQLITE",
                (
                    f"--sql=SELECT {columns}, ST_Union(geometry) AS geometry "
                    f"FROM {input_path.stem} "
                    "WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON') "
                    f"GROUP BY {columns}"
                ),
                "!",
            ],
            *["make-valid", "!"],
            *["write", output_path],
            *gdal_parquet_options,
        ],
        check=False,
    )


def create_matched(data_dir: Path) -> None:
    """Generate edge-matched boundaries for all datasets."""
    input_dir = data_dir / "country/extended"
    output_dir = data_dir / "country/matched"
    clip_path = data_dir / "bnda_cty.parquet"
    for input_path in sorted(input_dir.rglob("*.parquet")):
        output_path = output_dir / input_path.parent.name / input_path.name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _gdal_clip(input_path, output_path, clip_path)
        input_path.unlink()
    rmtree(data_dir / "country/extended")
