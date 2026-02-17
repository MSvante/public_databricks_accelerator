# Claude Instructions

This repository contains a YAML-driven transformation engine for Databricks using Lakeflow (DLT) pipelines.

## Project Overview

- **Framework**: Databricks Asset Bundles with Lakeflow (DLT)
- **Pattern**: Medallion architecture (bronze/silver/gold)
- **Configuration**: YAML-driven table definitions with auto-discovery
- **Environments**: dev, test, prod

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       ├── transformation-ci.yml    # PR validation (lint, test, bundle validate)
│       └── transformation-cd.yml    # Deployment (dev/test/prod)
│
├── docs/                            # Documentation
│   ├── data_ingestion_strategy.md   # Ingestion architecture
│   └── completed_plans/             # Implementation plans
│
└── transformation/                  # Main project
    ├── databricks.yml               # Asset Bundle config with targets
    ├── resources/
    │   └── transformation.yml       # DLT pipeline definition
    ├── config/                      # YAML table definitions
    │   ├── navision/
    │   │   ├── bronze.yml
    │   │   └── silver.yml
    │   └── entsog/
    │       ├── bronze.yml
    │       └── silver.yml
    └── src/
        ├── main.py                  # DLT entry point
        ├── engine/                  # Core engine
        │   ├── config_loader.py     # YAML parser
        │   ├── bronze.py            # Bronze layer utilities
        │   ├── silver.py            # Silver layer utilities
        │   └── expectations.py      # Data quality
        └── transforms/              # Source-specific transforms
            ├── navision/
            │   ├── customers.py
            │   ├── orders.py
            │   ├── order_lines.py
            │   ├── products.py
            │   └── vendors.py
            └── entsog/
                ├── allocations.py
                ├── connection_points.py
                ├── nominations.py
                └── physical_flows.py
```

## Key Concepts

### Catalog Structure

Layer-based catalogs with source system schemas:
- `raw.<source_system>.<table>` - Bronze (raw ingestion)
- `cleansed.<source_system>.<table>` - Silver (transformed)
- `curated.<source_system>.<table>` - Gold (business metrics, future)

### Adding a Source System

1. Create `config/<system>/bronze.yml` and `silver.yml`
2. Create `src/transforms/<system>/*.py`
3. Deploy: `databricks bundle deploy -t dev`

Source systems are auto-discovered from `config/`.

### Writing Transforms

Transform functions take DataFrames and return DataFrames:

```python
def transform(df: DataFrame) -> DataFrame:
    return df.filter(F.col("id").isNotNull())
```

### Deployment

```bash
cd transformation
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## CI/CD

- **CI** (transformation-ci.yml): Lint, type-check, security scan, bundle validate
- **CD** (transformation-cd.yml): Deploy to dev (auto), test, prod (with approvals)

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST_DEV` | Dev workspace URL |
| `DATABRICKS_TOKEN_DEV` | Dev workspace token |
| `DATABRICKS_HOST_TEST` | Test workspace URL |
| `DATABRICKS_TOKEN_TEST` | Test workspace token |
| `DATABRICKS_HOST_PROD` | Prod workspace URL |
| `DATABRICKS_TOKEN_PROD` | Prod workspace token |

## When Making Changes

1. Configuration changes go in `config/<source_system>/`
2. Transform logic goes in `src/transforms/<source_system>/`
3. Engine changes go in `src/engine/`
4. Pipeline definition changes go in `resources/transformation.yml`
5. Test in dev first, then promote to test and prod
