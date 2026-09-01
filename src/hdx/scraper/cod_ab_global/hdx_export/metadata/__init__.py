"""Build the HDX metadata CSV/parquet resources from the portolan catalog.

Concatenates each service's cod_ab:* catalog.json fields (written by
original/_metadata.py::enrich_service_catalog) via utils._metadata_io.save_metadata().
"""

from pathlib import Path

from pandas import DataFrame, to_datetime

from hdx.scraper.cod_ab_global.catalog import read_catalog
from hdx.scraper.cod_ab_global.hdx_export.services import iter_included_version_dirs
from hdx.scraper.cod_ab_global.utils import save_metadata

from ._columns import _COLUMNS, _COUNT_COLUMNS, _DATE_COLUMNS


def _read_service_row(version_dir: Path) -> dict | None:
    """Return a metadata row dict from one service's catalog.json, or None."""
    data = read_catalog(version_dir)
    if not data:
        return None
    return {col: data.get(f"cod_ab:{col}") for col in _COLUMNS}


def build_metadata(original_dir: Path, output_file: Path) -> None:
    """Build and save the global metadata parquet/CSV (_all/_latest/_historic)."""
    version_dirs = [
        *iter_included_version_dirs(original_dir, "latest"),
        *iter_included_version_dirs(original_dir, "historic"),
    ]
    rows = []
    for _iso3, version_dir in version_dirs:
        row = _read_service_row(version_dir)
        if row is not None:
            rows.append(row)

    df = DataFrame(rows, columns=_COLUMNS)
    df["country_name"] = df["country_name"].str.replace("\u2019", "'", regex=False)
    df["admin_level_full"] = df["admin_level_full"].astype("Int32")
    df[_COUNT_COLUMNS] = df[_COUNT_COLUMNS].astype("Int32")
    for col in _DATE_COLUMNS:
        df[col] = to_datetime(df[col], errors="coerce")
    df = df.sort_values(by=["country_iso3", "version"])

    output_file.parent.mkdir(parents=True, exist_ok=True)
    save_metadata(output_file, df)
