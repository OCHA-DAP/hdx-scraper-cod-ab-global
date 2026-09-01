"""Entry point: python -m hdx.scraper.cod_ab_global."""

import os

os.environ.setdefault("OGR_GEOJSON_MAX_OBJ_SIZE", "0")

from ._pipeline import main

if __name__ == "__main__":
    main()
