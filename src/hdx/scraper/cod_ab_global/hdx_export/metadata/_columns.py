"""Column schema for the global metadata CSV/parquet resources."""

_COUNT_COLUMNS = [
    "admin_1_count",
    "admin_2_count",
    "admin_3_count",
    "admin_4_count",
    "admin_5_count",
]

# catalog.json stores these as plain date strings; cast explicitly or
# consumers expecting Timestamps (e.g. dataset/boundaries.py) break.
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
