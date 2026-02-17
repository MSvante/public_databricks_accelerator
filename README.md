# Databricks Accelerator

A production-ready accelerator for building data transformation pipelines on Databricks using Lakeflow (DLT) with a YAML-driven configuration approach.

## Overview

This accelerator provides a framework for processing data through medallion architecture layers:

- **Bronze (Raw)** - Ingest from landing zone with Auto Loader, schema evolution, and housekeeping columns
- **Silver (Cleansed)** - Apply transformations, data quality expectations, and deduplication
- **Gold (Curated)** - Business-level aggregations and metrics (future)

## Features

- **YAML-Driven Configuration** - Define tables, sources, and expectations in YAML files
- **Auto-Discovery** - Source systems are automatically discovered from the `config/` directory
- **Data Quality** - Built-in expectation framework with drop, warn, and fail actions
- **Databricks Asset Bundles** - Deployment via `databricks bundle deploy`
- **Multi-Environment** - Separate dev, test, and prod targets
- **CI/CD Ready** - GitHub Actions workflows for validation and deployment

## Architecture

```
Landing Zone (Parquet)
        │
        ▼
┌───────────────────┐
│   Bronze Layer    │  raw.<source_system>.<table>
│   (raw catalog)   │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Silver Layer    │  cleansed.<source_system>.<table>
│(cleansed catalog) │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Gold Layer      │  curated.<source_system>.<table>
│(curated catalog)  │  (future)
└───────────────────┘
```

### Catalog Structure

```
raw catalog (bronze)
├── navision schema
│   ├── customers
│   ├── orders
│   └── products
└── entsog schema
    ├── physical_flows
    └── nominations

cleansed catalog (silver)
├── navision schema
│   ├── customers_clean
│   └── orders_enriched
└── entsog schema
    ├── physical_flows_clean
    └── nominations_clean
```

## Repository Structure

```
.
├── transformation/
│   ├── databricks.yml              # Asset Bundle configuration
│   ├── resources/
│   │   └── transformation.yml      # DLT pipeline definition
│   ├── src/
│   │   ├── main.py                 # DLT entry point
│   │   ├── engine/                 # Core engine modules
│   │   │   ├── config_loader.py    # YAML parser
│   │   │   ├── bronze.py           # Bronze layer utilities
│   │   │   ├── silver.py           # Silver layer utilities
│   │   │   └── expectations.py     # Data quality
│   │   └── transforms/             # Source-specific transforms
│   │       ├── navision/
│   │       └── entsog/
│   └── config/                     # YAML table definitions
│       ├── navision/
│       │   ├── bronze.yml
│       │   └── silver.yml
│       └── entsog/
│           ├── bronze.yml
│           └── silver.yml
├── docs/                           # Documentation
└── .github/workflows/              # CI/CD pipelines
    ├── transformation-ci.yml       # PR validation
    └── transformation-cd.yml       # Deployment
```

## Quick Start

### Prerequisites

1. Databricks CLI installed and configured
2. Access to a Databricks workspace with Unity Catalog
3. Landing zone storage containers with source data

### Deploy

```bash
cd transformation

# Set workspace URLs
export DATABRICKS_HOST_DEV="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_HOST_TEST="https://adb-yyy.azuredatabricks.net"
export DATABRICKS_HOST_PROD="https://adb-zzz.azuredatabricks.net"

# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to prod
databricks bundle deploy -t prod
```

## Adding a New Source System

1. **Create configuration files:**
   ```
   config/new_source/bronze.yml
   config/new_source/silver.yml
   ```

2. **Create transform modules:**
   ```
   src/transforms/new_source/__init__.py
   src/transforms/new_source/table1.py
   ```

3. **Deploy:**
   ```bash
   databricks bundle deploy -t dev
   ```

No additional pipeline configuration needed - source systems are auto-discovered!

## Writing Transformations

Transform functions receive DataFrames and return a transformed DataFrame:

```python
# transforms/navision/customers.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform(customers: DataFrame) -> DataFrame:
    """Clean and standardize customer data."""
    return (
        customers
        .filter(F.col("customer_id").isNotNull())
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .dropDuplicates(["customer_id"])
    )
```

## Data Quality Expectations

| Action | Behavior |
|--------|----------|
| `drop` | Invalid records are dropped |
| `warn` | Invalid records kept, warning logged |
| `fail` | Pipeline fails on invalid records |

## CI/CD

### GitHub Actions Workflows

- **transformation-ci.yml** - Runs on PRs: lint, type-check, security scan, bundle validation
- **transformation-cd.yml** - Deploys to environments: dev (auto), test, prod (with approvals)

### Required Secrets

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST_DEV` | Dev workspace URL |
| `DATABRICKS_TOKEN_DEV` | Dev workspace token |
| `DATABRICKS_HOST_TEST` | Test workspace URL |
| `DATABRICKS_TOKEN_TEST` | Test workspace token |
| `DATABRICKS_HOST_PROD` | Prod workspace URL |
| `DATABRICKS_TOKEN_PROD` | Prod workspace token |

## Development

```bash
cd transformation

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/

# Format
black src/ tests/
isort src/ tests/
```

## References

- [Lakeflow Documentation](https://docs.databricks.com/en/dlt/)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/)
