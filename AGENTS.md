# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Python scraper that downloads Common Operational Datasets - Administrative Boundaries (COD-AB) from OCHA's ArcGIS Enterprise Server (gis.unocha.org) and generates global administrative boundary datasets published to the Humanitarian Data Exchange (HDX).

The pipeline processes administrative boundaries for 100+ countries/territories, performing edge-extension and edge-matching operations to produce three variations:

- **Original**: Unmodified boundaries (may have gaps/overlaps at international borders)
- **Extended**: Edge-extended boundaries using a custom algorithm (no external dependencies)
- **Matched**: Edge-matched to UN Geodata 1:1M international boundaries (no gaps/overlaps)

## Development Commands

### Setup

```shell
uv sync
source .venv/bin/activate
pre-commit install
```

### Running the Pipeline

```shell
# Run the full pipeline
python run.py

# Or use the module directly
python -m hdx.scraper.cod_ab_global

# Using taskipy
uv run task app
```

### Code Quality

```shell
# Format and lint code (runs ruff format -> ruff check -> ruff format)
uv run task ruff

# Pre-commit will run automatically on commit, or manually:
pre-commit run --all-files
```

### Exporting Requirements

```shell
# Export both production and dev requirements
uv run task export

# This generates requirements.txt and requirements-dev.txt
```

### Docker

```shell
# Build the image
docker build -t hdx-scraper-cod-ab-global .

# The container runs PostgreSQL with PostGIS extension internally
```

## Configuration Requirements

### Required Files in Home Directory

1. `.hdx_configuration.yaml` - HDX API credentials:

```yaml
hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
hdx_read_only: false
hdx_site: prod
```

1. `.useragents.yaml` - User agent configuration (must contain `hdx-scraper-cod-ab-global` key)

### Environment Variables

**Required for ArcGIS Access:**

- `ARCGIS_USERNAME` - Username for gis.unocha.org authentication
- `ARCGIS_PASSWORD` - Password for gis.unocha.org authentication

**Optional Location Filtering:**

- `ISO3_INCLUDE` - Comma-separated ISO-3 codes to include (e.g., "AFG,BFA,CAF")
- `ISO3_EXCLUDE` - Comma-separated ISO-3 codes to exclude
- Both accept versioned values (e.g., "AFG_v01" to pin to a specific version)

**Version Control:**

- `RUN_VERSION` - Controls output: "LATEST" (default), "HISTORIC", or "LATEST,HISTORIC"
  - LATEST: Only the most recent version per country
  - HISTORIC: All versions except the latest
  - LATEST,HISTORIC: Both outputs

**Other Configuration:**

- `ARCGIS_SERVER` - Default: "<https://gis.unocha.org>"
- `ARCGIS_FOLDER` - Default: "Hosted"
- `ARCGIS_LAYER_REGEX` - Default: `r"^[a-z]{3}_admin\d$"`
- `ATTEMPT` - Retry attempts for downloads (default: 5)
- `WAIT` - Wait seconds between retries (default: 10)
- `TIMEOUT` - HTTP timeout in seconds (default: 60)
- `TIMEOUT_DOWNLOAD` - Download timeout (default: 600)
- `EXPIRATION` - Token expiration in minutes (default: 1440)

## Architecture

### High-Level Pipeline Flow

The pipeline is orchestrated in `__main__.py` and executes these stages sequentially:

1. **Authentication** (`utils.generate_token()`): Generates ArcGIS Enterprise token
2. **Download Phase**:
   - `download_admin0()`: Downloads admin0 boundaries from FIS server
   - `download_metadata()`: Fetches metadata table from ArcGIS
   - `download_boundaries()`: Downloads all country layers as ESRIJSON → GeoParquet
3. **Process Phase**:
   - `create_pcodes()`: Generates P-codes dataset (latest version only)
   - `create_boundaries()`: Consolidates country layers into global File Geodatabase
   - `preprocess_extended()`: Prepares geometries for edge extension
   - `edge_extender()`: Custom edge-extension algorithm (see below)
   - `postprocess_extended()`: Post-processes extended geometries
   - `create_matched()`: Edge-matches to UN Geodata 1:1M boundaries
4. **Dataset Creation** (`dataset/`): Uploads resources to HDX with metadata

### Module Structure

```
src/hdx/scraper/cod_ab_global/
├── __main__.py           # Pipeline orchestration
├── config.py             # Environment variables, global config
├── utils.py              # HTTP client, token generation, utilities
├── download/             # Data fetching from ArcGIS
│   ├── admin0.py         # Admin0 from FIS server
│   ├── metadata/         # Metadata table download
│   └── boundaries/       # Country layer downloads
│       ├── __init__.py   # Service/layer discovery
│       ├── feature.py    # ESRIJSON → GeoParquet conversion
│       └── refactor.py   # Column name standardization
├── process/              # Data transformation
│   ├── boundaries.py     # Global File Geodatabase creation
│   ├── pcodes.py         # P-codes dataset generation
│   ├── extended_pre.py   # Pre-processing for edge extension
│   ├── extended_post.py  # Post-processing after edge extension
│   └── matched.py        # Edge-matching to UN boundaries
├── edge_extender/        # Self-contained edge extension module
│   ├── __init__.py       # Main orchestration
│   ├── inputs.py         # Read/prepare input geometries
│   ├── lines.py          # Extract boundary lines
│   ├── attempt.py        # Extend edges iteratively
│   ├── merge.py          # Merge extended edges back
│   ├── outputs.py        # Write output geometries
│   ├── cleanup.py        # Remove temporary files
│   ├── points.py         # Point geometry operations
│   ├── topology.py       # Topology validation
│   ├── voronoi.py        # Voronoi diagram generation
│   └── utils.py          # Shared utilities
└── dataset/              # HDX dataset creation
    ├── boundaries.py     # Main boundaries dataset
    └── pcodes.py         # P-codes dataset
```

### Edge Extender Module

The `edge_extender` module is designed to run **without external dependencies** (no PostGIS required). It processes each country's parquet file through a functional pipeline:

1. **inputs.main**: Load parquet, prepare geometries
2. **lines.main**: Extract boundary segments
3. **attempt.main**: Iteratively extend edges using configurable distance parameter
4. **merge.main**: Merge extended segments back into original geometries
5. **outputs.main**: Write processed geometries
6. **cleanup.main**: Remove intermediate files

Configuration via environment:

- `EDGE_EXTENDER_DISTANCE` - Extension distance (default varies)
- `EDGE_EXTENDER_NUM_THREADS` - Parallel processing threads
- `EDGE_EXTENDER_QUIET` - Suppress logging

The module uses a functional composition pattern (`utils.apply_funcs`) to chain operations.

### Data Flow

```
ArcGIS Server (ESRIJSON)
  → download/ → GeoParquet (country/original/)
  → process/boundaries.py → File Geodatabase (global/original)
  → process/extended_pre.py → Parquet (country/extended_pre/)
  → edge_extender/ → Parquet (country/extended/)
  → process/extended_post.py → Parquet (country/extended/)
  → process/boundaries.py → File Geodatabase (global/extended)
  → process/matched.py → Parquet (country/matched/)
  → process/boundaries.py → File Geodatabase (global/matched)
  → dataset/boundaries.py → HDX Resource Upload
```

### Key Technical Details

- **GDAL Integration**: Uses `gdal vector` CLI commands for format conversions, field type manipulation, and File Geodatabase creation
- **Retry Logic**: `tenacity` library with configurable attempts/waits for ArcGIS API calls
- **Parquet Everywhere**: Internal format is GeoParquet (ZSTD compression, level 15)
- **HTTP/2**: Uses `httpx[http2]` for faster parallel downloads
- **Version Handling**: Metadata table drives which country versions to process
- **Where Filters**: Country-specific SQL filters in `config.where_filter` to exclude problematic features

### Important Constants

- `UPDATED_BY_SCRIPT = "HDX Scraper: COD-AB Global"` - Used for HDX dataset provenance
- `OBJECTID = "esriFieldTypeOID"` - ESRI object ID field type identifier
- P-codes are marked with `"p_coded": "True"` in HDX resources

### Testing & CI/CD

- No automated tests currently (no `tests/` directory)
- Pre-commit runs uv sync/export and ruff formatting/linting
- CI/CD publishes Docker images to AWS ECR on releases (`publish.yaml`)

### Where Filters

Some countries require SQL filters to exclude invalid/conflict geometries (see `config.where_filter`):

- No automated tests currently (no `tests/` directory)
- Pre-commit runs uv sync/export and ruff formatting/linting
- CI/CD publishes Docker images to AWS ECR on releases (`publish.yaml`)

### Debugging Notes

The `__main__.py` file contains `if False:` blocks that can be toggled to skip/include certain pipeline stages during development. The default state runs only the edge extension and matching phases (lines 36-44 and 52-53 are disabled).

### Where Filters

Some countries require SQL filters to exclude invalid/conflict geometries (see `config.where_filter`):

- Lebanon: Excludes conflict zones
- Pakistan: Excludes specific admin1 codes (PK1, PK3)
- Sudan: Excludes SD19
- South Sudan: Excludes SS00 and SS0807

These filters are applied during the download phase in `download/boundaries/feature.py`.
