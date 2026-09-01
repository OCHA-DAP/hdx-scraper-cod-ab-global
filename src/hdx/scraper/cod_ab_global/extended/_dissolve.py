"""Dissolve an edge-extended seed layer down to every shallower admin level."""

import logging
import tempfile
from pathlib import Path

import duckdb
from hdx.location.country import Country
from topo_tools import dissolve

from hdx.scraper.cod_ab_global.catalog import ADM_SUFFIXES
from hdx.scraper.cod_ab_global.config import ADMIN_SCHEMA_PATH

from ._write import write_gpq2

logger = logging.getLogger(__name__)


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


def dissolve_all_levels(
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
            write_gpq2(current_path, out_dir / f"{layer_name}.parquet")

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
                write_gpq2(dissolved_path, out_dir / f"{layer_name}.parquet")
                current_path = dissolved_path
        finally:
            con.close()
