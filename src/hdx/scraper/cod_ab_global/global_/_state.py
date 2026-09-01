"""Fingerprint the matched/ inputs feeding global/, and check output presence."""

from pathlib import Path

from hdx.scraper.cod_ab_global.catalog import file_fingerprint

_MAX_ADMIN = 4


def combined_fingerprint(services_meta: list[dict]) -> dict[str, list[int]]:
    """Return {service_name: [size, mtime_ns]} across all contributing parquets."""
    result = {}
    for meta in services_meta:
        fp = file_fingerprint(meta["parquet_path"])
        if fp is not None:
            result[meta["service_name"]] = fp
    return result


def parquets_exist(global_dir: Path) -> bool:
    """Return True if all four output parquets are present."""
    return all(
        (global_dir / f"admin{level}" / f"admin{level}.parquet").exists()
        for level in range(1, _MAX_ADMIN + 1)
    )


def pmtiles_exist(global_dir: Path) -> bool:
    """Return True if all four PMTiles files are present."""
    return all(
        (global_dir / f"admin{level}" / f"admin{level}.pmtiles").exists()
        for level in range(1, _MAX_ADMIN + 1)
    )
