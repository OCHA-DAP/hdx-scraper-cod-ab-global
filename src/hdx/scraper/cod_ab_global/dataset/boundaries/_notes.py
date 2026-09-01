"""Build the dataset notes markdown for the boundaries dataset."""

from ._dataset_info import _BREAK


def get_notes(admin_count: int, run_version: str) -> str:
    """Get notes for a dataset."""
    other_link = (
        "[historic boundaries](https://data.humdata.org/dataset/cod-ab-global-historic)"
        if run_version == "latest"
        else "[latest boundaries](https://data.humdata.org/dataset/cod-ab-global)"
    )
    paragraphs = [
        (
            f"Global administrative level 1-4 boundaries (COD-AB) dataset for "
            f"{admin_count} countries / territories, {run_version} versions."
        ),
        (
            "This is an aggregation of "
            "[subnational administrative boundaries](https://data.humdata.org/dataset/?cod_level=cod-enhanced&res_format=GeoJSON)"
            " available in 3 variations:"
        ),
        (
            "**Edge-Matched**: Subnational boundaries are aligned to fit international"
            " boundaries. This process results in simplification at the international"
            " border, but gaps and overlaps are eliminated. The internal resolution of"
            " each subnational boundary layer is not modified by this process."
            " Recommended to use for most use cases."
        ),
        (
            "**Original**: Subnational boundaries are unmodified from their original "
            "source. There will be gaps and overlaps at the international border. "
            "Recommended if maintaining the integrity of the initial source is"
            " important."
        ),
        (
            "**Extended**: This is an intermediate step in the process before"
            " edge-matching. Recommended for organizations with specific international"
            " boundary needs."
        ),
        "Metadata about sources used is also available as a table.",
        f"A version of this dataset is also available with {other_link}.",
    ]
    if run_version == "latest":
        paragraphs.append(
            "For a list of P-codes for use in data collection such as ODK / Kobo, see "
            "[Global P-code List](https://data.humdata.org/dataset/global-pcodes)"
        )
    return _BREAK.join(paragraphs)
