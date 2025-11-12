def get_columns(admin_level: int, *, is_point: bool = False) -> list[str]:
    """Get a list of column names for the given admin level."""
    columns = []
    end_level = admin_level - 1 if is_point else -1
    for level in range(admin_level, end_level, -1):
        columns += [f"adm{level}_name"]
        columns += [f"adm{level}_name1", f"adm{level}_name2", f"adm{level}_name3"]
        columns += [f"adm{level}_pcode"]
    columns += ["lang", "lang1", "lang2", "lang3"]
    columns += ["iso3", "version", "valid_on", "valid_to"]
    columns += ["area_sqkm", "geometry"]
    return columns
