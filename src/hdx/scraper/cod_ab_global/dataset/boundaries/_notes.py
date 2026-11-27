"""Build the dataset notes markdown for the boundaries dataset."""

from ._dataset_info import _BREAK

_HDX = "https://data.humdata.org/dataset"
_HUB = f"[Global - Subnational Administrative Boundaries]({_HDX}/cod-ab-global)"
_HISTORIC = (
    "[Global - Subnational Administrative Boundaries (Historic)]"
    f"({_HDX}/cod-ab-global-historic)"
)
_COUNTRY = f"[Country COD-AB datasets]({_HDX}/?cod_level=cod-ab)"
_PCODES = f"[Global P-code List]({_HDX}/global-pcodes)"


def get_notes(admin_count: int, run_version: str) -> str:
    """Get notes for a dataset."""
    if run_version == "latest":
        intro = (
            f"Administrative level 1-4 boundaries for {admin_count} "
            "countries / territories, using the latest version for each."
        )
        related = [
            f"{_COUNTRY}: for boundaries of a single country or territory.",
            (
                f"{_PCODES}: P-codes and names as CSV, for data collection tools "
                "such as ODK / Kobo."
            ),
            f"{_HISTORIC}: past versions of boundaries for each country or territory.",
        ]
    else:
        intro = (
            f"Past versions of administrative level 1-4 boundaries for "
            f"{admin_count} countries / territories. For current boundaries, "
            f"use {_HUB}."
        )
        related = [
            f"{_COUNTRY}: for boundaries of a single country or territory.",
            f"{_PCODES}: P-codes and names as CSV.",
        ]
    paragraphs = [
        intro,
        (
            "This dataset combines the "
            f"[country COD-AB datasets]({_HDX}/?cod_level=cod-ab) and contains:"
        ),
        (
            "- **Edge-Matched**: Subnational boundaries are aligned to fit"
            " international boundaries. This process results in simplification at"
            " the international border, but gaps and overlaps are eliminated. The"
            " internal resolution of each subnational boundary layer is not modified"
            " by this process. Recommended to use for most use cases.\n"
            "- **Original**: Subnational boundaries are unmodified from their original "
            "source. There will be gaps and overlaps at the international border. "
            "Recommended if maintaining the integrity of the initial source is"
            " important.\n"
            "- **Metadata**: A table of the sources used."
        ),
        "**Related datasets**",
        "\n".join(f"- {item}" for item in related),
    ]
    return _BREAK.join(paragraphs)
