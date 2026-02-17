# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation Engine - Main Entry Point
# MAGIC
# MAGIC This notebook is the entry point for the DLT pipeline. It discovers all source
# MAGIC systems from the config/ directory and creates bronze and silver tables.
# MAGIC
# MAGIC ## Catalog Structure
# MAGIC
# MAGIC Layer-based catalogs with source system schemas:
# MAGIC - `raw.<source_system>.<table>` - Bronze tables
# MAGIC - `cleansed.<source_system>.<table>` - Silver tables
# MAGIC - `curated.<source_system>.<table>` - Gold tables (future)
# MAGIC
# MAGIC ## Pipeline Variables
# MAGIC
# MAGIC - `landing_zone_uri`: Base URI for landing zone storage
# MAGIC - `config_path`: Path to configuration files in workspace
# MAGIC - `raw_catalog`: Catalog for bronze tables (default: 'raw')
# MAGIC - `cleansed_catalog`: Catalog for silver tables (default: 'cleansed')

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from engine.config_loader import ConfigLoader
import os

# COMMAND ----------

# =============================================================================
# Configuration Loading
# =============================================================================

# Get configuration from pipeline settings
landing_zone_uri = spark.conf.get("landing_zone_uri")
config_path = spark.conf.get("config_path")
raw_catalog = spark.conf.get("raw_catalog", "raw")
cleansed_catalog = spark.conf.get("cleansed_catalog", "cleansed")

# Initialize config loader
config_loader = ConfigLoader(spark)

# COMMAND ----------

# =============================================================================
# Discover Source Systems
# =============================================================================
# Find all source systems by looking for directories in config/


def discover_source_systems(config_base_path: str) -> list:
    """Discover all source systems from config directory."""
    try:
        # List directories in the config path
        entries = dbutils.fs.ls(config_base_path)
        source_systems = [
            entry.name.rstrip("/")
            for entry in entries
            if entry.isDir() and not entry.name.startswith(".")
        ]
        return source_systems
    except Exception as e:
        print(f"Error discovering source systems: {e}")
        return []


source_systems = discover_source_systems(config_path)
print(f"Discovered source systems: {source_systems}")

# COMMAND ----------

# =============================================================================
# Bronze Layer - Raw Data Ingestion
# =============================================================================
# Creates streaming tables from landing zone using Auto Loader.
# Tables are created in: raw.<source_system>.<table>
#
# Each table gets housekeeping columns:
#   - _ingested_at: When the record was ingested
#   - _source_file: Path to the source file
#   - _modified_at: File modification timestamp
#   - _source_system: Source system identifier

# COMMAND ----------

for source_system in source_systems:
    # Load bronze configuration for this source system
    bronze_config_path = f"{config_path}/{source_system}/bronze.yml"

    try:
        bronze_config = config_loader.load_bronze_config(bronze_config_path)
    except Exception as e:
        print(f"Skipping {source_system} bronze - no config found: {e}")
        continue

    # Create bronze tables for this source system
    for table_config in bronze_config.tables:
        table_name = table_config.name

        # Resolve source path with landing_zone_uri
        source_path = table_config.source_path.replace(
            "${var.landing_zone_uri}", landing_zone_uri
        )

        # Get Auto Loader options from config
        options = {
            "cloudFiles.format": table_config.source_format,
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns"
            if bronze_config.defaults.schema_evolution.add_new_columns
            else "none",
        }

        if bronze_config.defaults.schema_evolution.merge_schema:
            options["mergeSchema"] = "true"

        # Create closure to capture table-specific values
        def make_bronze_table(
            name: str,
            path: str,
            opts: dict,
            src_system: str,
            catalog: str,
        ):
            # Fully qualified table name: raw.<source_system>.<table>
            fq_name = f"{catalog}.{src_system}.{name}"

            @dlt.table(
                name=fq_name,
                comment=f"Bronze table for {name} from {src_system}",
            )
            def bronze_table():
                return (
                    spark.readStream.format("cloudFiles")
                    .options(**opts)
                    .load(path)
                    .withColumn("_ingested_at", F.current_timestamp())
                    .withColumn("_source_file", F.input_file_name())
                    .withColumn(
                        "_modified_at", F.col("_metadata.file_modification_time")
                    )
                    .withColumn("_source_system", F.lit(src_system))
                )

            return bronze_table

        # Register the table
        make_bronze_table(
            table_name, source_path, options, source_system, raw_catalog
        )

# COMMAND ----------

# =============================================================================
# Silver Layer - Transformed Data
# =============================================================================
# Creates materialized views with:
#   - Custom Python transformations
#   - Data quality expectations
#   - Joins between bronze and silver tables
#
# Tables are created in: cleansed.<source_system>.<table>

# COMMAND ----------

import importlib.util
import inspect
import sys
from pathlib import Path


def load_transform_function(transform_path: str, workspace_root: str):
    """Load a transform function from a Python file."""
    full_path = f"{workspace_root}/{transform_path}"

    module_name = Path(transform_path).stem
    spec = importlib.util.spec_from_file_location(module_name, full_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from {full_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    if not hasattr(module, "transform"):
        raise ValueError(
            f"Module {transform_path} does not have a 'transform' function"
        )

    return getattr(module, "transform")


# COMMAND ----------

for source_system in source_systems:
    # Load silver configuration for this source system
    silver_config_path = f"{config_path}/{source_system}/silver.yml"

    try:
        silver_config = config_loader.load_silver_config(silver_config_path)
    except Exception as e:
        print(f"Skipping {source_system} silver - no config found: {e}")
        continue

    workspace_root = str(Path(config_path).parent)

    # Create silver tables for this source system
    for table_config in silver_config.tables:
        table_name = table_config.name
        raw_tables = table_config.get_raw_table_list()
        transform_path = table_config.transform
        expectations = table_config.expectations

        # Create closure to capture table-specific values
        def make_silver_table(
            name: str,
            raw_table_names: list,
            transform_module_path: str,
            table_expectations: list,
            src_system: str,
            raw_cat: str,
            cleansed_cat: str,
            ws_root: str,
        ):
            # Fully qualified table name: cleansed.<source_system>.<table>
            fq_name = f"{cleansed_cat}.{src_system}.{name}"

            # Build expectation decorators list
            decorators = []
            for exp in table_expectations:
                if exp.action == "drop":
                    decorators.append(("expect_or_drop", exp.name, exp.expression))
                elif exp.action == "fail":
                    decorators.append(("expect_or_fail", exp.name, exp.expression))
                else:
                    decorators.append(("expect", exp.name, exp.expression))

            @dlt.table(
                name=fq_name,
                comment=f"Silver table for {name} from {src_system}",
            )
            def silver_table():
                # Load input DataFrames
                input_dfs = {}
                for raw_name in raw_table_names:
                    # Determine if this is a bronze table or another silver table
                    if raw_name.endswith("_clean") or raw_name.endswith("_enriched"):
                        # Silver table reference
                        fq_raw = f"{cleansed_cat}.{src_system}.{raw_name}"
                    else:
                        # Bronze table reference
                        fq_raw = f"{raw_cat}.{src_system}.{raw_name}"

                    # Try streaming read first, fall back to batch
                    try:
                        input_dfs[raw_name] = dlt.read_stream(fq_raw)
                    except Exception:
                        input_dfs[raw_name] = dlt.read(fq_raw)

                # Apply transform if specified
                if transform_module_path:
                    transform_fn = load_transform_function(
                        transform_module_path, ws_root
                    )
                    sig = inspect.signature(transform_fn)
                    param_names = list(sig.parameters.keys())
                    args = [input_dfs[n] for n in param_names]
                    result_df = transform_fn(*args)
                elif raw_table_names:
                    result_df = input_dfs[raw_table_names[0]]
                else:
                    raise ValueError(f"Table {name} has no raw_tables or transform")

                return result_df

            # Apply expectations
            for decorator_type, exp_name, exp_expr in decorators:
                if decorator_type == "expect_or_drop":
                    silver_table = dlt.expect_or_drop(exp_name, exp_expr)(silver_table)
                elif decorator_type == "expect_or_fail":
                    silver_table = dlt.expect_or_fail(exp_name, exp_expr)(silver_table)
                else:
                    silver_table = dlt.expect(exp_name, exp_expr)(silver_table)

            return silver_table

        # Register the table
        make_silver_table(
            table_name,
            raw_tables,
            transform_path,
            expectations,
            source_system,
            raw_catalog,
            cleansed_catalog,
            workspace_root,
        )
