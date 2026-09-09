"""Monkeypatch workarounds for upstream portolan-cli/geoparquet-io bugs.

See CLAUDE.md's "Known upstream issues" section for the tracked issue numbers.
"""

import geoparquet_io.core.arcgis as _gpio_arcgis
import portolan_cli.extract.arcgis.discovery as _arcgis_discovery

# discover_layers() has no token param, unlike discover_services() (portolan-cli#855).
_orig_fetch_json = _arcgis_discovery._fetch_json  # noqa: SLF001
_ARCGIS_TOKEN: str | None = None


def set_token(token: str) -> None:
    """Set the token used by the patched discovery _fetch_json fallback."""
    global _ARCGIS_TOKEN  # noqa: PLW0603
    _ARCGIS_TOKEN = token


def _patched_fetch_json(
    url: str, timeout: float = 60.0, token: str | None = None
) -> dict:
    return _orig_fetch_json(url, timeout=timeout, token=token or _ARCGIS_TOKEN)


_arcgis_discovery._fetch_json = _patched_fetch_json  # noqa: SLF001

# GDAL misdetects ESRIJSON as GeoJSON when features[] precedes the ID keys
# (geoparquet-io#501).
_orig_esrijson_page_to_table = _gpio_arcgis._esrijson_page_to_table  # noqa: SLF001
_ESRIJSON_HEADER_KEYS = (
    "geometryType",
    "spatialReference",
    "fields",
    "objectIdFieldName",
)


def _patched_esrijson_page_to_table(page: dict, con: object = None) -> object:
    if not page.get("features"):
        return None
    reordered = {k: page[k] for k in _ESRIJSON_HEADER_KEYS if k in page}
    reordered.update(page)
    return _gpio_arcgis._json_doc_to_table(  # noqa: SLF001
        reordered, exclude="geom", suffix=".json", con=con
    )


_gpio_arcgis._esrijson_page_to_table = _patched_esrijson_page_to_table  # noqa: SLF001
