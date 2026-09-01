"""Dataset info and resource list for the global P-codes dataset."""

dataset_info = {
    "name": "global-pcodes",
    "title": "Global P-code List",
    "notes": (
        "CSV containing subnational p-codes, their corresponding administrative names, "
        "parent p-codes, and reference dates for the world (where available). "
        "Latin names are used where available."
        "\n  \n  "
        "For actual boundaries: [Global Subnational Administrative Boundaries](https://data.humdata.org/dataset/cod-ab-global)"
    ),
    "methodology_other": (
        "P-codes taken from the latest administrative boundary layers available on the "
        "OCHA ArcGIS server (gis.unocha.org)."
    ),
    "caveats": (
        "There may be a delay of a few days between when new country boundaries "
        "are added to HDX and when they are aggregated into this global dataset."
    ),
}

resources = [
    {
        "name": "global_pcodes.csv",
        "description": (
            "Table contains the 3-digit ISO code, admin level, p-code, "
            "administrative name, parent p-code, and date."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcodes_adm_1_2.csv",
        "description": (
            "Data for admin levels 1 and 2. Table contains the 3-digit ISO "
            "code, admin level, p-code, administrative name, parent p-code, and date."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcode_lengths.csv",
        "description": "P-code lengths for all countries at all levels.",
    },
    {
        "name": "global_pcodes_hxl.csv",
        "description": (
            "Table contains the 3-digit ISO code, admin level, p-code, "
            "administrative name, parent p-code, and date. Includes HXL hashtags."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcodes_adm_1_2_hxl.csv",
        "description": (
            "Data for admin levels 1 and 2. Table contains the 3-digit ISO code, "
            "admin level, p-code, administrative name, parent p-code, and date. "
            "Includes HXL hashtags."
        ),
        "p_coded": "True",
    },
    {
        "name": "global_pcode_lengths_hxl.csv",
        "description": (
            "P-code lengths for all countries at all levels. Includes HXL hashtags."
        ),
    },
]
