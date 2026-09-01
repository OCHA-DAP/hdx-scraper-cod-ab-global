"""Clip one service's edge-extended admin1+ layers to the UN BNDA boundary."""

import logging
import tempfile
from pathlib import Path

import duckdb
from topo_tools import edge_clip as topo_clip

from hdx.scraper.cod_ab_global.catalog import admin_layer_pattern, admin_layers
from hdx.scraper.cod_ab_global.extended import _write_gpq2

logger = logging.getLogger(__name__)


def _clip_to_bnda(
    input_path: Path, output_path: Path, bnda_path: Path, iso3: str
) -> None:
    """Clip one extended admin layer to the UN international boundary."""
    iso3_upper = iso3.upper()
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
        with tempfile.TemporaryDirectory(prefix="portolan-matched-clip-") as tmp:
            tmp_path = Path(tmp)
            parent_path = tmp_path / "parent.parquet"
            con.execute(f"""
                COPY (
                    SELECT geometry FROM read_parquet('{bnda_path}')
                    WHERE iso3cd = '{iso3_upper}'
                ) TO '{parent_path}' (FORMAT PARQUET)
            """)

            tmp_out = tmp_path / "clipped.parquet"
            topo_clip(input_path, parent_path, tmp_out, overwrite=True)
            _write_gpq2(tmp_out, output_path)
    finally:
        con.close()


def process_service(
    iso3: str,
    version: str,
    extended_version_dir: Path,
    matched_version_dir: Path,
    bnda_path: Path,
) -> bool:
    """Clip all admin1+ layers for one service to UN boundaries."""
    layers = [
        (level, d) for level, d in admin_layers(extended_version_dir, iso3) if level > 0
    ]
    if not layers:
        logger.warning(
            "No admin1+ extended layers found for %s/%s, skipping", iso3, version
        )
        return False

    pattern = admin_layer_pattern(iso3)
    current_levels = {level for level, _ in layers}
    if matched_version_dir.exists():
        for d in matched_version_dir.iterdir():
            m = d.is_dir() and pattern.match(d.name)
            if m and int(m.group(1)) not in current_levels:
                (d / f"{d.name}.parquet").unlink(missing_ok=True)

    try:
        for level, layer_dir in layers:
            layer_name = f"{iso3}_admin{level}"
            input_path = layer_dir / f"{layer_name}.parquet"
            out_dir = matched_version_dir / layer_name
            out_dir.mkdir(parents=True, exist_ok=True)
            output_path = out_dir / f"{layer_name}.parquet"
            _clip_to_bnda(input_path, output_path, bnda_path, iso3)
    except Exception:
        logger.exception("Matched clipping failed for %s/%s", iso3, version)
        return False

    logger.info("Matched %s/%s successfully", iso3, version)
    return True
