# Transformation Engine

A YAML-driven transformation engine for Databricks using Lakeflow (DLT) pipelines.

## Overview

This transformation engine processes data through bronze (raw) and silver (cleansed) layers using:

- **YAML configuration** for table definitions
- **Python transformations** for data cleaning logic
- **Data quality expectations** for validation
- **Databricks Asset Bundles** for deployment

**Note**: The transformation engine deploys to dev, test, and prod environments.

## Architecture

### Catalog Structure

Layer-based catalogs with source system schemas:

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

curated catalog (gold - future)
├── navision schema
└── entsog schema
```

### Data Flow

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
```

## Quick Start

### Prerequisites

1. Databricks CLI installed and configured
2. Access to a Databricks workspace
3. Landing zone containers with source data

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

All source systems are auto-discovered from `config/` and processed together.

## Directory Structure

```
transformation/
├── databricks.yml                    # Asset Bundle configuration
├── resources/
│   └── transformation.yml            # Single pipeline definition
├── src/
│   ├── main.py                       # DLT entry point (discovers all systems)
│   ├── engine/
│   │   ├── config_loader.py          # YAML parser
│   │   ├── bronze.py                 # Bronze layer utilities
│   │   ├── silver.py                 # Silver layer utilities
│   │   └── expectations.py           # Data quality
│   └── transforms/
│       ├── navision/                 # Navision transforms
│       │   ├── customers.py
│       │   └── orders.py
│       └── entsog/                   # ENTSOG transforms
│           └── physical_flows.py
├── config/
│   ├── navision/
│   │   ├── bronze.yml
│   │   └── silver.yml
│   └── entsog/
│       ├── bronze.yml
│       └── silver.yml
└── tests/
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

## Configuration

### Bronze Layer (config/{source}/bronze.yml)

Defines tables to ingest from the landing zone:

```yaml
source_system: navision
layer: bronze

defaults:
  source_format: parquet
  schema_evolution:
    add_new_columns: true

tables:
  - name: customers
    source_path: ${var.landing_zone_uri}/navision/customers/

  - name: orders
    source_path: ${var.landing_zone_uri}/navision/orders/
```

### Silver Layer (config/{source}/silver.yml)

Defines transformed tables with expectations:

```yaml
source_system: navision
layer: silver

tables:
  - name: customers_clean
    raw_tables: customers
    transform: transforms/navision/customers.py
    expectations:
      - name: valid_customer_id
        expression: "customer_id IS NOT NULL"
        action: drop

  - name: orders_enriched
    raw_tables: orders, customers_clean
    transform: transforms/navision/orders.py
```

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

For joins, specify multiple tables in `raw_tables`:

```python
def transform(orders: DataFrame, customers_clean: DataFrame) -> DataFrame:
    """Enrich orders with customer data."""
    return orders.join(
        customers_clean.select("customer_id", "name"),
        on="customer_id",
        how="left"
    )
```

## Data Quality Expectations

| Action | Behavior |
|--------|----------|
| `drop` | Invalid records are dropped |
| `warn` | Invalid records kept, warning logged |
| `fail` | Pipeline fails on invalid records |

## Housekeeping Columns

Bronze tables automatically include:

| Column | Description |
|--------|-------------|
| `_ingested_at` | Ingestion timestamp |
| `_source_file` | Source file path |
| `_modified_at` | File modification time |
| `_source_system` | Source system name |

## Development

### Local Testing

```bash
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

### CI/CD

- **CI**: Runs on PRs - lint, test, security scan, bundle validation
- **CD**: Single deploy job - deploys to selected environment

## Environment Variables

Set these secrets in GitHub Actions:

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST_DEV` | Dev workspace URL |
| `DATABRICKS_TOKEN_DEV` | Dev workspace token |
| `DATABRICKS_HOST_TEST` | Test workspace URL |
| `DATABRICKS_TOKEN_TEST` | Test workspace token |
| `DATABRICKS_HOST_PROD` | Prod workspace URL |
| `DATABRICKS_TOKEN_PROD` | Prod workspace token |

## References

- [Lakeflow Documentation](https://docs.databricks.com/en/dlt/)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/)
