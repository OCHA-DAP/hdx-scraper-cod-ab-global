"""Dataset-level metadata and per-stage resource info for the boundaries dataset."""

_BREAK = "  \n  \n"


def get_dataset_info(run_version: str) -> dict:
    """Get dataset info for a dataset."""
    name_extra = ""
    title_extra = ""
    if run_version != "latest":
        name_extra = f"-{run_version}"
        title_extra = f" ({run_version.title()})"
    methodology = (
        "Data taken from administrative boundary layers available on HDX."
        " Edge-extending of original geometries is automatically applied using"
        " an [algorithm](https://github.com/fieldmaps/edge-extender)."
        " Extended boundaries are then clipped against international boundaries to"
        " achieve edge-matching."
    )
    caveats = _BREAK.join(
        [
            (
                "There may be a delay of a few days between when new country"
                " boundaries are added to HDX and when they are aggregated"
                " into this global dataset."
            ),
            (
                "In the extended and edge-matched resources, lower levels are"
                " filled in with higher ones if they don't exist. Example:"
                " Admin 2 is used to fill in Admin 3 and 4 if they don't"
                " exist. Also, only layers with full coverage are used for"
                " these two resources. Example: if Admin 3 only covers part"
                " of a location, Admin 2 is used instead."
            ),
        ]
    )
    return {
        "name": f"cod-ab-global{name_extra}",
        "title": f"Global - Subnational Administrative Boundaries{title_extra}",
        "methodology_other": methodology,
        "caveats": caveats,
    }


def get_resource(run_version: str, stage: str) -> dict:
    """Get resource info for a dataset."""
    resources = {
        "matched": {
            "name": f"global_admin_boundaries_matched_{run_version}.gdb.zip",
            "description": (
                "Edge-matched geometry (no gaps or overlaps), "
                f"{run_version} versions only."
            ),
        },
        "original": {
            "name": f"global_admin_boundaries_original_{run_version}.gdb.zip",
            "description": (
                "Original geometry (with gaps and overlaps), "
                f"{run_version} versions only."
            ),
        },
        "extended": {
            "name": f"global_admin_boundaries_extended_{run_version}.gdb.zip",
            "description": (
                f"Extended geometry (pre-edge-matching), {run_version} versions only."
            ),
        },
    }
    return resources[stage]
