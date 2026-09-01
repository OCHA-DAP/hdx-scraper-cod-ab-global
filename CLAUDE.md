# AGENTS.md

This file provides guidance to code agents when working with code in this repository.

## ABSOLUTE CONSTRAINTS — Never Violate

**You are NEVER allowed to degrade the quality of the source geometry or coordinate data in any way. This includes but is not limited to:**

- No coordinate precision reduction (e.g. `--precision`, `ST_SnapToGrid`, rounding)
- No geometry simplification (e.g. `ST_Simplify`, `ST_SimplifyPreserveTopology`, tippecanoe `-S`)
- No vertex reduction or tolerance-based generalization of any kind
- No lossy geometry transformations at any stage of the pipeline

This rule applies to all stages: download, processing, edge extension, clipping, PMTiles generation, and any format conversion. The source data must pass through at full fidelity. If a tool or operation requires precision reduction to succeed, find a different approach — do not apply the reduction.

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
│   └── matched.py        # Edge-matching to UN boundaries
└── dataset/              # HDX dataset creation
    ├── boundaries.py     # Main boundaries dataset
    └── pcodes.py         # P-codes dataset
```

### Data Flow

```
ArcGIS Server (ESRIJSON)
  → download/ → GeoParquet (country/original/)
  → process/boundaries.py → File Geodatabase (global/original)
  → topo_tools.extend() → Parquet (country/extended/)
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

## Portolan Mirror

Mirrors COD-AB ArcGIS services to source.coop as four independent sibling portolan
catalogs under one work directory. Run with:

```shell
uv run python -m hdx.scraper.cod_ab_global
```

Set `PORTOLAN_WORK_DIR=./portolan` in `.env` to use a persistent local work directory
(gitignored). Without it, a temp dir is used and everything is rebuilt from scratch.

### Four sibling trees

```
work_dir/
  original/   native ArcGIS mirror, via portolan_cli's extract_arcgis_catalog()
    <iso3>/<version>/<iso3>_admin{N}/<iso3>_admin{N}.parquet
  extended/   this repo's edge-extension (topo_tools.edge_extend/dissolve)
    <iso3>/<version>/<iso3>_admin{N}/<iso3>_admin{N}.parquet   (same layout as original/)
  matched/    edge-matched to UN Geodata 1:1M via topo_tools.edge_clip
    <iso3>/<version>/<iso3>_admin{N}/<iso3>_admin{N}.parquet   (admin0 excluded)
  global/     cross-country composite, no iso3 anywhere in its path
    admin{1..4}/admin{N}.parquet
  .bnda/        UN BNDA download, outside all four catalogs
```

`original/`, `extended/`, and `matched/` share the exact same relative path shape and
layer naming (`^{iso3}_admin(\d+)$`, `original.py::admin_layer_pattern()`), so only the
leading root segment changes when following one layer across variants. Each of the four
trees is its own independent portolan catalog (own `.portolan/config.yaml`,
`catalog.json`), initialised via `original.py::_ensure_root_catalog()`. Push happens once
per tree, to its own `{SOURCECOOP_REMOTE}/{original,extended,matched,global}/` subpath,
after all four stages complete (`__main__.py`).

### Change detection

Each tree fingerprints only its own immediate upstream input, independently:

- **`original/`**: `original.py::_extract_service()` fetches `editingInfo.lastEditDate`
  per ArcGIS layer and compares it to the value stored in `original/.state/fingerprints.json`.
  A service is skipped (no ArcGIS re-extraction) only if every layer's timestamp is
  unchanged, any change re-extracts the whole service via `extract_arcgis_catalog()`.
- **`extended/`, `matched/`**: each fingerprints its deepest upstream seed parquet as
  `[size, mtime_ns]` (`extended.py::file_fingerprint()`), stored in
  `<tree>/.state/fingerprints.json`. A service is reprocessed only if that fingerprint
  changed since the last run.
- **`global/`**: fingerprints the combined set of contributing `matched/` parquets,
  stored in `global/.state/fingerprint.json` (`global_.py::_combined_fingerprint()`). A
  rebuild is also forced if the expected output parquets or PMTiles are missing.

This means a no-op re-run against an unchanged ArcGIS source skips ArcGIS re-extraction
and all downstream `topo_tools` recomputation for every stage.

### STAC catalog structure

- `<tree>/<iso3>/<version>/catalog.json`, service-level STAC Catalog; `original/`'s is
  enriched with `cod_ab:*` fields from `COD_Global_Metadata`
  (`original.py::_enrich_service_catalog()`)
- `<tree>/<iso3>/<version>/<iso3>_admin{N}/collection.json`, layer-level STAC Collection
- `global/admin{N}/collection.json`, one collection per admin level. `global/` layers must
  live in a subdirectory (portolan requires data assets to be inside a collection, not at
  the catalog root), hence `admin{N}/admin{N}.parquet` rather than a flat `admin{N}.parquet`
- `portolan add` regenerates `catalog.json`/`collection.json` on every run for the
  services it touches, custom fields are re-applied afterwards
- `original.py::_push_catalog_files()` syncs intermediate `catalog.json`/`README.md` at
  the root, country, and service levels to S3, since `portolan push` only handles leaf
  collections (portolan-cli#552, tracked upstream, not yet fixed)

### Known upstream issues (workarounds applied locally)

- **[portolan-sdi/portolan-cli#855](https://github.com/portolan-sdi/portolan-cli/issues/855)**
  (open): `discover_layers()` has no `token` param, unlike `discover_services()`, breaking
  discovery against token-gated ArcGIS servers. Workaround: `original.py` monkeypatches
  `portolan_cli.extract.arcgis.discovery._fetch_json` to inject the stored token.
- **[portolan-sdi/portolan-cli#546](https://github.com/portolan-sdi/portolan-cli/issues/546)**
  (open): no native freshness-aware resume in `extract_arcgis_catalog()`. Workaround:
  the `lastEditDate` fingerprinting described above, done entirely in this repo's code.
- **[portolan-sdi/portolan-cli#552](https://github.com/portolan-sdi/portolan-cli/issues/552)**
  (open): `portolan push` skips intermediate `catalog.json` for nested collections.
  Workaround: `_push_catalog_files()`.
- **[geoparquet/geoparquet-io#501](https://github.com/geoparquet/geoparquet-io/issues/501)**
  (open): GDAL misdetects ESRIJSON as GeoJSON when a response's `features[]` array
  precedes the ESRIJSON-identifying keys, common on ArcGIS Hosted FeatureServer
  responses. Workaround: `original.py` monkeypatches
  `geoparquet_io.core.arcgis._esrijson_page_to_table` to reorder those keys to the front
  before GDAL sees the payload.
- **[portolan-sdi/portolan-cli#545](https://github.com/portolan-sdi/portolan-cli/issues/545)**
  (closed): native auth support (`ExtractionOptions.token`) landed, no workaround needed.
- **[geoparquet/geoparquet-io#786](https://github.com/geoparquet/geoparquet-io/pull/786)**
  (open PR): `esriFieldTypeBigInteger` (and `DateOnly`/`TimeOnly`) are missing from
  `TYPE_MAPPING` in `_build_schema_from_layer_info`, so fields of that type are silently
  coerced to string instead of int64. Workaround: `original.py` monkeypatches
  `geoparquet_io.core.arcgis._build_schema_from_layer_info` to force `int64` on any field
  whose declared type is `esriFieldTypeBigInteger`; safe to leave in place once the PR
  merges, since re-applying the same mapping is a no-op.

### HTTP timeout for large layers

Large polygon layers (e.g. Philippines admin1 regions) can exceed the default 60s HTTP
timeout. `original.py` passes `timeout=300` in `ExtractionOptions`, `matched.py`'s
one-off UN BNDA download passes `timeout=300` directly to `gpio.extract_arcgis()`.
