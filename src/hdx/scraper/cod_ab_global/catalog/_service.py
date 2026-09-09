"""Bootstrap a single portolan catalog tree (original/extended/matched/global/)."""

from pathlib import Path
from textwrap import dedent

from hdx.scraper.cod_ab_global.config import ARCGIS_SERVICES_URL

from ._subprocess import _portolan

CATALOG_TITLE = "COD-AB Administrative Boundaries"


def _write_catalog_metadata(catalog_dir: Path) -> None:
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    (portolan_dir / "metadata.yaml").write_text(
        dedent(f"""\
            license: CC-BY-3.0-IGO
            keywords:
              - administrative boundaries
              - COD-AB
              - humanitarian
              - OCHA
              - HDX
              - GeoParquet
              - cloud-native
            contact:
              name: HDX Data Systems Team
              email: hdx@un.org
            attribution: UN OCHA Information Systems Section (ISS)
            source_url: {ARCGIS_SERVICES_URL}
        """)
    )


def _write_catalog_config(catalog_dir: Path) -> None:
    """Disable portolan's covering-column rewrite (see CLAUDE.md's upstream issues)."""
    (catalog_dir / ".portolan" / "config.yaml").write_text(
        dedent("""\
            conversion:
              vector:
                add_bbox: false
        """)
    )


def _ensure_root_catalog(work_dir: Path) -> None:
    """Initialise the portolan catalog rooted at work_dir if not present."""
    work_dir.mkdir(parents=True, exist_ok=True)
    if (work_dir / ".portolan" / "config.yaml").exists() and (
        work_dir / "catalog.json"
    ).exists():
        _write_catalog_metadata(work_dir)
        _write_catalog_config(work_dir)
        return
    (work_dir / ".portolan" / "config.yaml").unlink(missing_ok=True)
    _portolan(
        ["init", "--title", CATALOG_TITLE, "--license", "CC-BY-3.0-IGO", "--auto"],
        cwd=work_dir,
    )
    _write_catalog_metadata(work_dir)
    _write_catalog_config(work_dir)
