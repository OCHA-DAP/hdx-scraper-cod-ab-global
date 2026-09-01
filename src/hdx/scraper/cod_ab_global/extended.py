"""Edge-extend COD-AB boundaries from original/ into extended/.

Layer naming mirrors original/ exactly; only the root segment differs.
"""

import logging
import tempfile
from pathlib import Path
from shutil import copy

import duckdb
import geoparquet_io as gpio
from hdx.location.country import Country
from topo_tools import dissolve
from topo_tools import edge_extend as extend

from hdx.scraper.cod_ab_global.config import where_filter as _where_filter

from .catalog import (
    ADM_SUFFIXES,
    admin_layers,
    file_fingerprint,
    iter_version_dirs,
    portolan_add,
    read_catalog,
    read_json_state,
    remove_stale_versions,
    write_json_state,
)
from .config import ADMIN_SCHEMA_PATH, PORTOLAN_WORKERS

logger = logging.getLogger(__name__)


def _get_admin_level_full(original_version_dir: Path, iso3: str) -> int | None:
    """Return admin_level_full from original/'s catalog.json, verified on disk.

    Falls back to the highest native admin dir with an existing parquet.
    """
    catalog = read_catalog(original_version_dir)
    val = catalog.get("cod_ab:admin_level_full")
    if val is not None:
        try:
            level = int(val)
        except (TypeError, ValueError):
            level = None
        if level is not None:
            layer_name = f"{iso3}_admin{level}"
            seed = original_version_dir / layer_name / f"{layer_name}.parquet"
            if seed.exists():
                return level
    layers = admin_layers(original_version_dir, iso3)
    return layers[-1][0] if layers else None


def _admin_group_cols(all_cols: list[str], level: int) -> list[str]:
    """Return columns to SELECT/GROUP BY when dissolving to admin level.

    Empty list means no pcode column exists for this level.
    """
    keep = {f"adm{L}{s}" for L in range(level + 1) for s in ADM_SUFFIXES} | {
        "lang",
        "lang1",
        "lang2",
        "lang3",
        "version",
        "valid_on",
        "valid_to",
    }
    cols = [c for c in all_cols if c in keep]
    pcodes = {f"adm{L}_pcode" for L in range(level + 1)}
    if not any(c in pcodes for c in cols):
        return []
    return cols


def _write_gpq2(src: Path, dest: Path) -> None:
    """Read a GeoParquet file, Hilbert-sort it, and write as GeoParquet 2.0."""
    gpio.read(str(src)).sort_hilbert().write(
        str(dest), compression_level=22, geoparquet_version="2.0"
    )


def _dissolve_all_levels(
    seed_path: Path,
    iso3: str,
    admin_level_full: int,
    extended_version_dir: Path,
) -> None:
    """Write edge-extended + dissolved lower-level parquets into extended/."""
    iso3_upper = iso3.upper()
    iso2 = Country.get_iso2_from_iso3(iso3_upper) or ""
    iso_suffix = f"'{iso2}' AS iso2, '{iso3_upper}' AS iso3"

    with tempfile.TemporaryDirectory(prefix="portolan-dissolve-") as tmp:
        tmp_path = Path(tmp)
        current_path = tmp_path / f"{iso3}_admin{admin_level_full}.parquet"

        con = duckdb.connect()
        try:
            con.load_extension("spatial")
            con.execute(
                f"CREATE TABLE seed AS SELECT * FROM read_parquet('{seed_path}')"
            )
            all_cols = [row[0] for row in con.execute("DESCRIBE seed").fetchall()]
            keep_cols = _admin_group_cols(all_cols, admin_level_full)
            if not keep_cols:
                logger.warning(
                    "No pcode column found for %s up to level %d, nothing written",
                    iso3_upper,
                    admin_level_full,
                )
                return
            cols_str = ", ".join(keep_cols)
            con.execute(
                f"COPY (SELECT {cols_str}, {iso_suffix}, geometry FROM seed)"
                f" TO '{current_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            layer_name = f"{iso3}_admin{admin_level_full}"
            out_dir = extended_version_dir / layer_name
            out_dir.mkdir(parents=True, exist_ok=True)
            _write_gpq2(current_path, out_dir / f"{layer_name}.parquet")

            for level in range(admin_level_full - 1, -1, -1):
                pcode_col = f"adm{level}_pcode"
                if pcode_col not in keep_cols:
                    continue
                layer_name = f"{iso3}_admin{level}"
                out_dir = extended_version_dir / layer_name
                out_dir.mkdir(parents=True, exist_ok=True)
                dissolved_path = tmp_path / f"{layer_name}_raw.parquet"
                issues_path = tmp_path / f"{layer_name}_issues.parquet"
                dissolve(
                    current_path,
                    dissolved_path,
                    issues_path,
                    group_by=[pcode_col],
                    target_schema_path=ADMIN_SCHEMA_PATH,
                    overwrite=True,
                )
                if issues_path.exists():
                    logger.warning(
                        "Dissolve to level %d for %s reported topology issues: %s",
                        level,
                        iso3_upper,
                        issues_path,
                    )
                _write_gpq2(dissolved_path, out_dir / f"{layer_name}.parquet")
                current_path = dissolved_path
        finally:
            con.close()


def _apply_where_filter(path: Path, iso3_upper: str) -> None:
    """Apply config.where_filter to a parquet in-place before edge extension."""
    raw = _where_filter.get(iso3_upper)
    if not raw:
        return
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
        available = {r[0] for r in described}
        conditions = [
            c.strip()
            for c in raw.split(" and ")
            if c.strip().split()[0].lower() in available
        ]
        if not conditions:
            return
        where = " and ".join(conditions)
        tmp = path.with_suffix(".tmp.parquet")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{path}') WHERE {where})"
            f" TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        path.unlink()
        tmp.rename(path)
        logger.debug("Applied where filter for %s: %s", iso3_upper, where)
    finally:
        con.close()


def _process_service(
    iso3: str,
    version: str,
    original_version_dir: Path,
    extended_version_dir: Path,
    admin_level_full: int,
) -> bool:
    """Run edge extension for one service in an isolated temp dir."""
    layer_name = f"{iso3}_admin{admin_level_full}"
    seed_src = original_version_dir / layer_name / f"{layer_name}.parquet"
    if not seed_src.exists():
        logger.warning("Seed parquet not found: %s", seed_src)
        return False

    with tempfile.TemporaryDirectory(prefix="portolan-extended-") as tmp:
        temp_path = Path(tmp)
        pre_path = temp_path / "pre.parquet"
        copy(seed_src, pre_path)
        _apply_where_filter(pre_path, iso3.upper())

        post_path = temp_path / "post.parquet"
        try:
            extend(pre_path, post_path, overwrite=True)
        except Exception:
            logger.exception("Edge extension failed for %s/%s", iso3, version)
            return False

        for level in range(admin_level_full + 2):
            stale_dir = extended_version_dir / f"{iso3}_admin{level}"
            if stale_dir.exists():
                (stale_dir / f"{iso3}_admin{level}.parquet").unlink(missing_ok=True)

        try:
            _dissolve_all_levels(
                post_path, iso3, admin_level_full, extended_version_dir
            )
        except Exception:
            logger.exception("Postprocessing failed for %s/%s", iso3, version)
            return False

    logger.info("Extended %s/%s successfully", iso3, version)
    return True


def enumerate_services(root_dir: Path) -> list[tuple[str, str]]:
    """Return [(iso3, version), ...] for all service dirs in root_dir."""
    return [
        (iso3, version)
        for iso3, version, _ in iter_version_dirs(root_dir)
        if (version.startswith("v") and version[1:].isdigit()) or version == "latest"
    ]


def run(original_dir: Path, extended_dir: Path) -> None:
    """Edge-extend admin-polygon layers from original/ into extended/."""
    services = enumerate_services(original_dir)
    if not services:
        logger.warning("No services found in %s, run original first", original_dir)
        return
    logger.info("Found %d services to process for extended", len(services))

    remove_stale_versions(set(services), extended_dir)

    fingerprints_path = extended_dir / ".state" / "fingerprints.json"
    stored_fingerprints = read_json_state(fingerprints_path)
    new_fingerprints: dict[str, list[int]] = {}
    workers = str(PORTOLAN_WORKERS)

    for iso3, version in services:
        key = f"{iso3}/{version}"
        original_version_dir = original_dir / iso3 / version
        extended_version_dir = extended_dir / iso3 / version

        admin_level_full = _get_admin_level_full(original_version_dir, iso3)
        if admin_level_full is None:
            logger.warning("Cannot determine admin_level_full for %s/%s", iso3, version)
            continue
        layer_name = f"{iso3}_admin{admin_level_full}"
        seed_src = original_version_dir / layer_name / f"{layer_name}.parquet"
        fingerprint = file_fingerprint(seed_src)
        if fingerprint is None:
            continue
        new_fingerprints[key] = fingerprint

        if (
            fingerprint == stored_fingerprints.get(key)
            and extended_version_dir.exists()
        ):
            logger.debug("Skipping unchanged extended for %s/%s", iso3, version)
            continue

        logger.info("Processing extended for %s/%s", iso3, version)
        if not _process_service(
            iso3, version, original_version_dir, extended_version_dir, admin_level_full
        ):
            logger.warning(
                "Extended processing failed for %s/%s, will retry next run",
                iso3,
                version,
            )
            continue

        portolan_add(extended_dir, f"{iso3}/{version}/", workers)

    write_json_state(fingerprints_path, new_fingerprints)
