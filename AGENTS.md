# AGENTS.md

This file provides guidance to code agents when working with code in this repository.

## ABSOLUTE CONSTRAINTS — Never Violate

**You are NEVER allowed to degrade the quality of the source geometry or coordinate data in any way. This includes but is not limited to:**

- No coordinate precision reduction (e.g. `--precision`, `ST_SnapToGrid`, rounding)
- No geometry simplification (e.g. `ST_Simplify`, `ST_SimplifyPreserveTopology`, tippecanoe `-S`)
- No vertex reduction or tolerance-based generalization of any kind
- No lossy geometry transformations at any stage of the pipeline

This rule applies to all stages: download, processing, edge extension, clipping, PMTiles generation, and any format conversion. The source data must pass through at full fidelity. PMTiles are the one exception: overview levels generalize at coarser zooms, but the finest zoom must stay as close to the source geometry as tile coordinate precision allows. If a tool or operation requires precision reduction to succeed, find a different approach — do not apply the reduction.

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
- **Parquet Everywhere**: Internal format is GeoParquet 2.0, labelled `OGC:CRS84`, rows sorted by P-code then name (`extended/_write.py::write_gpq2()`, ZSTD level 22)
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
    admin{1..4}/admin{N}.parquet, admin{N}.pmtiles, admin{N}_points.pmtiles
.bnda/        UN BNDA download, sibling of work_dir, never inside any pushed catalog tree
```

`original/`, `extended/`, and `matched/` share the exact same relative path shape and
layer naming (`^{iso3}_admin(\d+)$`, `catalog/_tree.py::admin_layer_pattern()`), so only
the leading root segment changes when following one layer across variants. Each of the four
trees is its own independent portolan catalog (own `.portolan/config.yaml`,
`catalog.json`), initialised via `catalog/_service.py::_ensure_root_catalog()`. Push happens once
per tree, to its own `{SOURCECOOP_REMOTE}/{original,extended,matched,global}/` subpath,
after all four stages complete (`_pipeline.py`), with `aws s3 sync` alone
(`catalog/_top.py::sync_tree()`, `config/aws.config` for per-part multipart retries):
one pass for everything but `versions.json`, then a `--delete` pass that adds
`versions.json`, so remote orphans left by `portolan rm` are removed.

PMTiles are built by tylertoo (`catalog/_pmtiles.py::tile_parquet()`, z0-z11, density drop,
thinning and visibility filters off, no tile size limit, `bounded` memory profile) beside
each parquet right before every `portolan add --force`, which registers the sibling archive
as it is (never `--pmtiles`, which re-tiles any archive older than its parquet with tippecanoe). `global_/_points.py::build_points()` also writes
`admin{N}_points.pmtiles` (maximum inscribed circle centers, all attribute columns),
registered as asset `admin{N}_points`.

### Change detection

Each tree fingerprints only its own immediate upstream input, independently:

- **`original/`**: `original/_extract.py::extract_service()` fetches `editingInfo.lastEditDate`
  per ArcGIS layer and compares it to the value stored in `original/.state/fingerprints.json`.
  A service is skipped (no ArcGIS re-extraction) only if every layer's timestamp is
  unchanged, any change re-extracts the whole service via `extract_arcgis_catalog()`.
- **`extended/`, `matched/`**: each fingerprints its deepest upstream seed parquet as
  `[size, mtime_ns]` (`catalog/_tree.py::file_fingerprint()`), stored in
  `<tree>/.state/fingerprints.json`. A service is reprocessed only if that fingerprint
  changed since the last run.
- **`global/`**: fingerprints the combined set of contributing `matched/` parquets,
  stored in `global/.state/fingerprint.json` (`global_/_state.py::combined_fingerprint()`).
  A rebuild is also forced if the expected output parquets or PMTiles (including
  `admin{N}_points.pmtiles`) are missing.

This means a no-op re-run against an unchanged ArcGIS source skips ArcGIS re-extraction
and all downstream `topo_tools` recomputation for every stage.

### STAC catalog structure

- `work_dir/catalog.json`, root STAC Catalog linking the four sibling trees via `child`
  links (`catalog/_top.py::write_top_catalog()`), rewritten every run from `_pipeline.py`
  after all four stages complete; pushed to `{SOURCECOOP_REMOTE}/catalog.json` via
  `catalog/_top.py::push_top_catalog()`
- `<tree>/<iso3>/<version>/catalog.json`, service-level STAC Catalog; `original/`'s is
  enriched with `cod_ab:*` fields from `COD_Global_Metadata`
  (`original/_metadata.py::enrich_service_catalog()`)
- `<tree>/<iso3>/<version>/<iso3>_admin{N}/collection.json`, layer-level STAC Collection
- `global/admin{N}/collection.json`, one collection per admin level. `global/` layers must
  live in a subdirectory (portolan requires data assets to be inside a collection, not at
  the catalog root), hence `admin{N}/admin{N}.parquet` rather than a flat `admin{N}.parquet`
- `portolan add` regenerates `catalog.json`/`collection.json` on every run for the
  services it touches, custom fields are re-applied afterwards

### Known upstream issues (workarounds applied locally)

- **[portolan-sdi/portolan-cli#855](https://github.com/portolan-sdi/portolan-cli/issues/855)**
  (closed, 0.9.0): `discover_layers()` takes the token through `ArcGISClient`, no workaround
  needed.
- **[portolan-sdi/portolan-cli#546](https://github.com/portolan-sdi/portolan-cli/issues/546)**
  (open): no native freshness-aware resume in `extract_arcgis_catalog()`. Workaround:
  the `lastEditDate` fingerprinting described above, done entirely in this repo's code.
- **[geoparquet/geoparquet-io#501](https://github.com/geoparquet/geoparquet-io/issues/501)**
  (closed, 1.7.0): ESRIJSON pages are reordered before GDAL reads them, no workaround needed.
- **[portolan-sdi/portolan-cli#545](https://github.com/portolan-sdi/portolan-cli/issues/545)**
  (closed): native auth support (`ExtractionOptions.token`) landed, no workaround needed.
- **[geoparquet/geoparquet-io#786](https://github.com/geoparquet/geoparquet-io/pull/786)**
  (merged in 1.5.0): `esriFieldTypeBigInteger`/`DateOnly`/`TimeOnly` now map correctly in
  `TYPE_MAPPING`, no workaround needed.
- **portolan-cli 0.9.0** `add` step (not yet filed upstream): `_needs_spatial_rewrite` in
  `preparation.py` only recognizes the GeoParquet 1.x `covering` struct column, not
  GeoParquet 2.0's native bbox support. Every `portolan add` call silently reconverts a 2.0
  file lacking `covering` back to 1.1 via `gpio.convert(...).write(...)` with no version
  argument (gpio's own default), so `extended/_write.py::write_gpq2()`'s 2.0 output never
  actually persists as 2.0. Workaround: `add_bbox: false` in each tree's
  `.portolan/config.yaml` (`catalog/_service.py::_write_catalog_config()`), which stops the
  rewrite but also disables portolan's own PTL-DAT-006/007 row-order checks for this
  catalog.
- **portolan-cli 0.9.0** (not yet filed upstream): `extract_arcgis_catalog()` writes an
  index-format `versions.json` in each `original/<iso3>/<version>/`, and `push`'s
  `discover_collections()` treats it as a collection whose metadata-only sync re-uploads
  every file below it on every push. Workaround:
  `catalog/_verify.py::remove_nested_index_versions()`, run before every push in `_push.py`.
- **portolan-cli 0.9.0** (not yet filed upstream): service `catalog.json` files come out
  self-rooted with no `parent` link (PTL-LNK-001/006). Workaround: `_push.py` runs
  `portolan check --fix --no-data` on each tree before syncing it.
- **portolan-cli 0.9.0** (not yet filed upstream): `add` fills the source `url` and zoom range
  of extract-written styles (`styles/default.json`) only for PMTiles it tiled itself, so
  styles beside prebuilt archives get no `url`. It also fills `minzoom`/`maxzoom` only when
  absent, never correcting stale values. Workaround: `catalog/_pmtiles.py::tile_parquet()`
  calls portolan's own `complete_style_sources()` with the archive's header zoom range.

### HTTP timeout for large layers

Large polygon layers (e.g. Philippines admin1 regions) can exceed the default 60s HTTP
timeout. `original/_extract.py` passes `timeout=300` in `ExtractionOptions`,
`matched/_bnda.py`'s one-off UN BNDA download passes `timeout=300` directly to
`gpio.extract_arcgis()`.
