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

This Python pipeline mirrors Common Operational Datasets - Administrative Boundaries (COD-AB)
from OCHA's ArcGIS Enterprise Server (gis.unocha.org) and publishes them to source.coop and
the Humanitarian Data Exchange (HDX). It builds three variations for 100+ countries and
territories:

- **Original**: unmodified boundaries, which may have gaps or overlaps at international borders.
- **Extended**: edge-extended boundaries (`topo_tools.edge_extend`). Input to Matched. Published to source.coop only.
- **Matched**: Extended clipped to UN Geodata 1:1M international boundaries, with no gaps or overlaps.

## Development Commands

```shell
uv sync
source .venv/bin/activate
pre-commit install
```

```shell
uv run task app                      # runs python run.py
python run.py                        # re-launches under uv outside a venv
python -m hdx.scraper.cod_ab_global  # same pipeline
uv run task ruff                     # ruff check && ruff format
docker build -t hdx-scraper-cod-ab-global .
```

Every run command calls `_pipeline.py::main()`, which takes no arguments or flags. A run is
the update: it re-checks ArcGIS and recomputes only what changed (see Change detection).
Stages, in order:

1. `original.run()`: ArcGIS token, service discovery, extraction of changed services.
2. `extended.run()`: `config.where_filter`, then `edge_extend` and `package_polygons`.
3. `matched.run()`: `edge_mosaic` into UN BNDA, with the original layer for detached pieces.
4. `global_.run()`: union of matched layers, `topo_detect` (then `topo_clean` on defects),
   `schema_fill` and `package_polygons`, then `admin{1..4}` parquet and PMTiles.
5. `write_top_catalog()`: root `catalog.json`.
6. `push_all()`: `aws s3 sync` of all four trees to `SOURCECOOP_REMOTE`, on every run.
   Any AWS credential source publishes. A failed sync, including missing credentials, stops
   the run before the HDX export. `aws s3 sync` skips files whose size and mtime are
   unchanged. The root `catalog.json` is uploaded every run.
7. `hdx_export.run()`: builds GDBs and CSVs for `original` and `matched` into
   `HDX_EXPORT_OUTPUT_DIR`. With `HDX_EXPORT_PUSH=true` it pushes each dataset whose data
   fingerprint differs from `../.hdx_export/push_state.json` for the configured site.
   Changes to dataset metadata code (notes, tags, `cod_level`) alone do not trigger a push.

Set `PORTOLAN_WORK_DIR=./portolan` in `.env` for a persistent, gitignored work directory.
Without it a temp dir is used and everything is rebuilt. `.bnda/`, `.hdx_export/` and
`hdx_export_build/` sit beside the work directory.

## Configuration

`config.py` loads `.env` with `load_dotenv(override=True)`, so `.env` values win over the shell.

HDX push needs `~/.hdx_configuration.yaml`:

```yaml
hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
hdx_read_only: false
hdx_site: prod
```

and `~/.useragents.yaml` with a `hdx-scraper-cod-global` key.

| Variable | Default | Used by |
| --- | --- | --- |
| `ARCGIS_USERNAME` / `ARCGIS_PASSWORD` | (none) | Token for `cod_ab_*` services, which reject anonymous requests |
| `ARCGIS_SERVER` | `https://gis.unocha.org` | Services are read from `/server/rest/services/Hosted` |
| `ARCGIS_TIMEOUT` | `60` | HTTP timeout (s) for discovery and metadata calls. Token generation uses the httpx default and extraction uses 300 |
| `ARCGIS_EXPIRATION` | `1440` | Token lifetime (min) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | (none) | `aws s3 sync` to source.coop |
| `SOURCECOOP_REMOTE` | `s3://us-west-2.opendata.source.coop/hdx/cod-ab/` | Push target |
| `PORTOLAN_WORK_DIR` | (temp dir) | Root of the four trees |
| `PORTOLAN_WORKERS` | `min(cpu_count, 8)` | Parallel `portolan` workers |
| `ISO3_INCLUDE` / `ISO3_EXCLUDE` | (none) | HDX export only. Comma-separated ISO-3 codes. `ISO3_EXCLUDE` also takes version-pinned values. Extraction and source.coop cover every service |
| `HDX_EXPORT_PUSH` | `false` | `true` pushes to the `hdx_site` in `~/.hdx_configuration.yaml` |
| `HDX_EXPORT_OUTPUT_DIR` | `<work dir parent>/hdx_export_build` | HDX build output |

## Where Filters

`config.where_filter` drops features in `extended/_process.py` before edge extension, so it
affects Extended, Matched and Global but not Original:

- Lebanon: `adm1_pcode <> 'Conflict'`
- Pakistan: `adm1_pcode not in ('PK1', 'PK3')`
- Sudan: `adm1_pcode <> 'SD19'`
- South Sudan: `adm1_pcode <> 'SS00' and adm2_pcode <> 'SS0807'`

## Testing and CI/CD

- No `tests/` directory. `uv run task test` exists but has nothing to run.
- Pre-commit runs `uv-sync`, `ruff-check` and `ruff-format`.
- `publish.yaml` publishes the Docker image to AWS ECR on releases.

## Portolan Mirror

The source.coop side is four independent sibling portolan catalogs under one work directory.
Every stage writes parquet through `extended/_write.py::write_gpq2()`: GeoParquet 2.0,
`OGC:CRS84`, rows sorted by P-code then name, ZSTD level 22.

### Four sibling trees

```
work_dir/
  original/   native ArcGIS mirror, via portolan_cli's extract_arcgis_catalog()
    <iso3>/<version>/<iso3>_admin{N}/<iso3>_admin{N}.parquet
  extended/   topo_tools.edge_extend, then package_polygons
    <iso3>/<version>/<iso3>_admin{N}/<iso3>_admin{N}.parquet   (same layout as original/)
  matched/    edge-matched to UN Geodata 1:1M via topo_tools.edge_mosaic
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

A re-run against an unchanged ArcGIS source skips ArcGIS re-extraction
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
