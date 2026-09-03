# Collector for COD-AB Datasets

[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-cod-ab/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-cod-ab/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-cod-ab/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-cod-ab?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline mirrors Common Operational Datasets - Administrative Boundaries (COD-AB) from
[gis.unocha.org](https://gis.unocha.org/server/rest/services/Hosted), processes them into
edge-extended and edge-matched global variants, and publishes the results to two destinations:

- **HDX**: [cod-ab-global](https://data.humdata.org/dataset/cod-ab-global),
  [cod-ab-global-historic](https://data.humdata.org/dataset/cod-ab-global-historic), and
  [global-pcodes](https://data.humdata.org/dataset/global-pcodes)
- **source.coop**: `s3://us-west-2.opendata.source.coop/hdx/cod-ab/`, as four sibling
  [STAC](https://stacspec.org/) catalogs (`original/`, `extended/`, `matched/`, `global/`), one
  per processing stage. See [CLAUDE.md](CLAUDE.md) for the catalog layout and design notes.

## Development

### Environment

[uv](https://github.com/astral-sh/uv) is used for package management with development done using Python >=3.13. Pre-commit formatting follows [ruff](https://docs.astral.sh/ruff/) guidelines. To get set up:

```shell
    uv sync
    source .venv/bin/activate
    pre-commit install
```

### Configuration

The pipeline needs three sets of credentials:

1. **ArcGIS** (required): `ARCGIS_USERNAME` and `ARCGIS_PASSWORD` for
   [gis.unocha.org](https://gis.unocha.org/server/rest/services/Hosted). The `Hosted` folder is
   the default export location for the ArcGIS Enterprise Server; COD-AB layers (prefixed
   `cod_ab_`) require authentication.
2. **AWS** (required to push to source.coop): `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   with write access to the source.coop S3 bucket.
3. **HDX** (required to push to HDX): a `.hdx_configuration.yaml` file in your home directory:

   ```yaml
   hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
   hdx_read_only: false
   hdx_site: prod
   ```

   Plus the universal `.useragents.yaml` file in your home directory, containing the key
   `hdx-scraper-cod-global`.

### Running Pipeline

```shell
    python run.py
```

or equivalently `python -m hdx.scraper.cod_ab_global`.

Pushing to HDX is opt-in: set `HDX_EXPORT_PUSH=true` in addition to the HDX credentials above,
otherwise the HDX export stage builds locally without writing anything. Pushing to source.coop
happens whenever AWS credentials are present.

### Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `ARCGIS_USERNAME` / `ARCGIS_PASSWORD` | (none) | ArcGIS Enterprise credentials |
| `ARCGIS_SERVER` | `https://gis.unocha.org` | ArcGIS Enterprise host |
| `ISO3_INCLUDE` / `ISO3_EXCLUDE` | (none) | Comma-separated ISO-3 codes to include/exclude, e.g. `AFG,BFA,CAF`; accepts versioned values like `AFG_v01` to pin a specific version |
| `SOURCECOOP_REMOTE` | `s3://us-west-2.opendata.source.coop/hdx/cod-ab/` | source.coop push target |
| `PORTOLAN_WORK_DIR` | (temp dir) | Local work directory for the four sibling catalogs; set to a persistent path for incremental local runs |
| `PORTOLAN_WORKERS` | up to 8 | Parallel workers for portolan operations |
| `HDX_EXPORT_PUSH` | `false` | Set `true` to actually push the HDX export (requires `.hdx_configuration.yaml`) |
| `HDX_EXPORT_OUTPUT_DIR` | (temp dir under work dir's parent) | Local output directory for the HDX export build |

### Docker

```shell
docker build -t hdx-scraper-cod-ab-global .
```

The image's `ENTRYPOINT` runs `python -m hdx.scraper.cod_ab_global` directly.
