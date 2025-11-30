from pathlib import Path
from subprocess import run

from ..config import iso3_exclude, iso3_include
from ..utils import get_columns


def get_extra_columns(admin_level: int) -> list[str]:
    """Get a list of column names for the given admin level."""
    return [
        f"adm{admin_level - 1}_name AS adm{admin_level}_name",
        f"adm{admin_level - 1}_name1 AS adm{admin_level}_name1",
        f"adm{admin_level - 1}_name2 AS adm{admin_level}_name2",
        f"adm{admin_level - 1}_name3 AS adm{admin_level}_name3",
        f"adm{admin_level - 1}_pcode AS adm{admin_level}_pcode",
    ]


def adm_copy(input_path: Path, output_path: Path, level: int) -> None:
    """Copy existing admin level."""
    columns = ",".join(get_columns(level)[0:-1])
    run(
        [
            *["gdal", "vector", "sql"],
            *[input_path, output_path],
            (
                f"--sql=SELECT {columns},{level} AS adm_origin,geometry "
                f"FROM {input_path.stem}"
            ),
            "--overwrite",
            "--quiet",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )


def adm_dissolve_down(input_path: Path, output_path: Path, level: int) -> None:
    """Dissolve an admin level down."""
    columns = ",".join(get_columns(level))
    run(
        [
            *["gdal", "vector", "sql"],
            *[input_path, output_path],
            "--dialect=SQLITE",
            (
                f"--sql=SELECT {columns}, ST_Union(geometry) AS geometry "
                f"FROM {input_path.stem} GROUP BY {columns}"
            ),
            "--overwrite",
            "--quiet",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )


def adm_dissolve_up(input_path: Path, output_path: Path, level: int) -> None:
    """Dissolve an admin level down."""
    columns = ",".join(get_columns(level - 1))
    extra_columns = ",".join(get_extra_columns(level))
    run(
        [
            *["gdal", "vector", "sql"],
            *[input_path, output_path],
            f"--sql=SELECT {extra_columns},{columns},geometry FROM {input_path.stem}",
            "--overwrite",
            "--quiet",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )


def postprocess_extended(data_dir: Path) -> None:
    """Postprocess extended boundaries."""
    input_dir = data_dir / "country/extended_tmp/post"
    output_dir = data_dir / "country/extended"
    output_dir.mkdir(parents=True, exist_ok=True)
    for input_path in sorted(input_dir.glob("*.parquet")):
        iso3 = input_path.stem.split("_")[0].upper()
        if (iso3_include and iso3 not in iso3_include) or (
            iso3_exclude and iso3 in iso3_exclude
        ):
            continue
        admin_level = int(input_path.stem[-1])
        version = input_path.stem.split("_")[1]
        output_path = (
            output_dir
            / f"cod_ab_{iso3.lower()}_{version}"
            / f"{iso3.lower()}_admin{admin_level}.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        adm_copy(input_path, output_path, admin_level)
        for level in range(admin_level - 1, -1, -1):
            input_down = output_path.with_stem(f"{output_path.stem[0:-1]}{level + 1}")
            output_down = output_path.with_stem(f"{output_path.stem[0:-1]}{level}")
            adm_dissolve_down(input_down, output_down, level)
        for level in range(admin_level + 1, 5):
            input_up = output_path.with_stem(f"{output_path.stem[0:-1]}{level - 1}")
            output_up = output_path.with_stem(f"{output_path.stem[0:-1]}{level}")
            adm_dissolve_up(input_up, output_up, level)
