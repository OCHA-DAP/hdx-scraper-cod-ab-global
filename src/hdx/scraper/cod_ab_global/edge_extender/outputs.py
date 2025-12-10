from pathlib import Path
from subprocess import run
from typing import LiteralString
from venv import logger

from psycopg import Connection
from psycopg.sql import SQL, Identifier

from .config import dbname, quiet
from .topology import check_gaps, check_overlaps

query_1: LiteralString = """--sql
    DROP VIEW IF EXISTS {table_out};
    CREATE VIEW {table_out} AS
    SELECT
        a.geom,
        b.*
    FROM {table_in1} AS a
    LEFT JOIN {table_in2} AS b
    ON a.fid = b.fid;
"""


def main(conn: Connection, name: str, file: Path, layer: str, *_: list) -> None:
    """Output results to file."""
    check_overlaps(conn, name, f"{name}_05")
    check_gaps(conn, name, f"{name}_05")
    conn.execute(
        SQL(query_1).format(
            table_in1=Identifier(f"{name}_05"),
            table_in2=Identifier(f"{name}_attr"),
            table_out=Identifier(f"{name}_06"),
        ),
    )
    output_path = file.parents[1] / "extended_post" / file.name
    output_path.parent.mkdir(exist_ok=True, parents=True)
    run(
        [
            *["gdal", "vector", "convert"],
            *[f"PG:dbname={dbname}", output_path],
            "--overwrite",
            "--quiet",
            f"--input-layer={name}_06",
            f"--output-layer={layer}",
            "--layer-creation-option=COMPRESSION_LEVEL=15",
            "--layer-creation-option=COMPRESSION=ZSTD",
            "--layer-creation-option=GEOMETRY_NAME=geometry",
        ],
        check=True,
    )
    if not quiet:
        logger.info(f"done: {name}")
    file.unlink()
