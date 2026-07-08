"""Build the HDX metadata CSV/parquet resources from the portolan catalog.

Replaces download/metadata/refactor.py's output role. Every field here is
already written into each service's catalog.json by
portolan/original.py::_enrich_service_catalog (admin_level_full_overrides
and date_valid_on_overrides are both applied there — see that module, not
here) — this module only concatenates those cod_ab:* fields into the
dataframe shape the old metadata CSV/parquet resources expect, then reuses
the existing utils.py::save_metadata() unchanged.
"""

from pathlib import Path

from pandas import DataFrame, to_datetime

from hdx.scraper.cod_ab_global.portolan.original import read_catalog
from hdx.scraper.cod_ab_global.utils import save_metadata

from .services import iter_included_version_dirs

# Reads catalog.json fields written at the "original" stage — see
# boundaries.py's FINGERPRINT_KEYS for why this lives next to each module's
# own logic rather than in a generic cross-cutting lookup.
FINGERPRINT_KEY = "cod_ab:original_updated"

_COUNT_COLUMNS = [
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
]

# catalog.json stores these as plain ISO date strings (raw JSON, no schema).
# The old pipeline got real datetime64 columns for free via GDAL's
# ESRIJSON->parquet conversion; here they must be cast explicitly or
# consumers expecting Timestamps (e.g. dataset/boundaries.py's
# set_time_period, which calls .isoformat() on the min/max) break.
_DATE_COLUMNS = [
    "date_source",
    "date_updated",
    "date_reviewed",
    "date_metadata",
    "date_valid_on",
    "date_valid_to",
]

_COLUMNS = [
    "country_name",
    "country_iso2",
    "country_iso3",
    "version",
    "admin_level_full",
    "admin_level_max",
    "admin_1_name",
    "admin_2_name",
    "admin_3_name",
    "admin_4_name",
    "admin_5_name",
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
    "admin_notes",
    "date_source",
    "date_updated",
    "date_reviewed",
    "date_metadata",
    "date_valid_on",
    "date_valid_to",
    "update_frequency",
    "update_type",
    "source",
    "contributor",
    "methodology_dataset",
    "methodology_pcodes",
    "caveats",
]


def _read_service_row(version_dir: Path) -> dict | None:
    """Return a metadata row dict from one service's catalog.json, or None."""
    data = read_catalog(version_dir)
    if not data:
        return None
    return {col: data.get(f"cod_ab:{col}") for col in _COLUMNS}


def build_metadata(work_dir: Path, output_file: Path) -> None:
    """Build and save the global metadata parquet/CSV (_all/_latest/_historic)."""
    version_dirs = [
        *iter_included_version_dirs(work_dir, "latest"),
        *iter_included_version_dirs(work_dir, "historic"),
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
