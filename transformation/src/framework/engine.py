"""The generic pipeline engine: YAML in, pipeline graph out.

``build_pipeline`` registers every table and flow with the Lakeflow Declarative
Pipelines API. ``dp`` and ``spark`` are injected, so tests can pass a recording
fake and assert on the graph without a Spark cluster.

Raw layer, per source table::

    landing files --Auto Loader--> raw_log_<name> --AUTO CDC (SCD1)--> <name> (CDF on)

    with PII (shared targets only):
    landing --> pii_raw_<d>.raw_log_<name> --> pii_raw_<d>.<name>          vaulted
                         |
                         +--sha2--> raw_<d>.raw_log_<name> --> raw_<d>.<name>   public

Trusted layer, per data product and per driver::

    <name>__<driver>  (temporary view running transform() for that driver)
          --AUTO CDC (SCD1), one flow per driver--> trusted_<d>.<name>
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from framework import cdc
from framework.config import DataProduct, FrameworkConfig, SourceTable
from framework.naming import Naming

log = logging.getLogger(__name__)

RAW_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",  # trusted flows stream from this
    "delta.feature.timestampNtz": "supported",
}
TRUSTED_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",  # so downstream consumers can go incremental too
    "delta.feature.timestampNtz": "supported",
}
TRUSTED_SEQUENCE_COLUMN = "source_extract_datetime"
TRUSTED_DELETE_CONDITION = "is_deleted = true"


@dataclass(frozen=True)
class Registration:
    """What was registered; returned so callers and tests can inspect it."""

    kind: str  # "table" | "streaming_table" | "view" | "cdc_flow"
    name: str
    source: str | None = None
    flow_name: str | None = None


def _view_decorator(dp):
    # OSS Spark calls it temporary_view; older Databricks code uses view.
    return getattr(dp, "temporary_view", None) or dp.view


class PipelineBuilder:
    def __init__(
        self,
        dp,
        spark,
        config: FrameworkConfig,
        naming: Naming,
        import_module: Callable[[str], ModuleType] = importlib.import_module,
    ) -> None:
        self.dp = dp
        self.spark = spark
        self.config = config
        self.naming = naming
        self.import_module = import_module
        self.registered: list[Registration] = []

    # ------------------------------------------------------------------ raw
    def _register_raw_log_from_landing(self, src: SourceTable, schema: str, mask: bool) -> str:
        name = f"{self.naming.catalog}.{schema}.{src.raw_log_name}"
        spark = self.spark

        def raw_log():
            from pyspark.sql import functions as F

            from framework.pii import mask_columns

            df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", src.format)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(src.landing_path)
                .withColumn("source_file_path", F.col("_metadata.file_path"))
                .withColumn("source_file_modified_at", F.col("_metadata.file_modification_time"))
                .withColumn(src.sequence_column, F.col(src.timestamp_column).cast("timestamp"))
            )
            if src.timestamp_column != src.sequence_column:
                df = df.drop(src.timestamp_column)
            return mask_columns(df, src.pii_columns) if mask else df

        self.dp.table(
            name=name,
            comment=f"Append-only log of every landed change for {src.name}",
            table_properties={"delta.feature.timestampNtz": "supported"},
        )(raw_log)
        self.registered.append(Registration("table", name, source=src.landing_path))
        return name

    def _register_public_log_from_vault(self, src: SourceTable, vault_log: str, schema: str) -> str:
        name = f"{self.naming.catalog}.{schema}.{src.raw_log_name}"
        spark = self.spark

        def public_log():
            from framework.pii import mask_columns

            return mask_columns(spark.readStream.table(vault_log), src.pii_columns)

        self.dp.table(
            name=name,
            comment=f"Public log for {src.name}; PII columns SHA-256 hashed",
            table_properties={"delta.feature.timestampNtz": "supported"},
        )(public_log)
        self.registered.append(Registration("table", name, source=vault_log))
        return name

    def _register_raw_scd1(self, src: SourceTable, schema: str, log_table: str) -> str:
        name = f"{self.naming.catalog}.{schema}.{src.name}"
        self.dp.create_streaming_table(
            name=name,
            comment=f"Current state of {src.name} (SCD Type 1)",
            table_properties=RAW_TABLE_PROPERTIES,
            cluster_by=list(src.cluster_by) or None,
        )
        self.dp.create_auto_cdc_flow(
            target=name,
            source=log_table,
            keys=list(src.keys),
            sequence_by=src.sequence_column,
            apply_as_deletes=src.delete_condition,
            except_column_list=[src.operation_column],
            stored_as_scd_type=1,
        )
        self.registered.append(Registration("streaming_table", name))
        self.registered.append(Registration("cdc_flow", name, source=log_table))
        return name

    def build_raw(self) -> None:
        for src in self.config.sources:
            public_schema = self.naming.schema("raw", src.domain)
            if src.has_pii and self.naming.builds_vault:
                vault_schema = self.naming.schema("raw", src.domain, vaulted=True)
                vault_log = self._register_raw_log_from_landing(src, vault_schema, mask=False)
                self._register_raw_scd1(src, vault_schema, vault_log)
                public_log = self._register_public_log_from_vault(src, vault_log, public_schema)
            else:
                # Sandbox: no vault, mask straight from landing.
                public_log = self._register_raw_log_from_landing(src, public_schema, mask=src.has_pii)
            self._register_raw_scd1(src, public_schema, public_log)

    # -------------------------------------------------------------- trusted
    def input_schemas(self, product_def: dict[str, Any], vaulted: bool) -> dict[str, str]:
        """catalog.schema for every raw table a product reads.

        The vaulted variant reads PII sources from the vault and everything
        else from the public raw schemas.
        """
        out = {}
        for table in cdc.input_tables(product_def):
            if table not in self.config.domain_registry:
                raise ValueError(f"Data product reads unknown source table '{table}'")
            domain = self.config.domain_registry[table]
            from_vault = vaulted and table in self.config.pii_sources
            out[table] = f"{self.naming.catalog}.{self.naming.schema('raw', domain, vaulted=from_vault)}"
        return out

    def _register_trusted(self, product: DataProduct, module: ModuleType, vaulted: bool) -> None:
        product_def = module.DATA_PRODUCT
        schema = self.naming.schema("trusted", product.domain, vaulted=vaulted)
        target = f"{self.naming.catalog}.{schema}.{product.name}"
        suffix = "_vaulted" if vaulted else ""
        schemas = self.input_schemas(product_def, vaulted)
        key_registry = self.config.key_registry
        spark = self.spark

        self.dp.create_streaming_table(
            name=target,
            comment=f"{'Vaulted' if vaulted else 'Public'} trusted table {product.name}",
            table_properties=TRUSTED_TABLE_PROPERTIES,
        )
        self.registered.append(Registration("streaming_table", target))

        for driver in cdc.driver_names(product_def):
            view_name = f"{product.name}{suffix}__{driver}"

            def staging(driver=driver):
                with cdc.driver_scope(driver):
                    return module.transform(spark, schemas, key_registry)

            _view_decorator(self.dp)(name=view_name, comment=f"{product.name} changes driven by {driver}")(staging)
            self.dp.create_auto_cdc_flow(
                target=target,
                source=view_name,
                keys=list(product.keys),
                sequence_by=TRUSTED_SEQUENCE_COLUMN,
                apply_as_deletes=TRUSTED_DELETE_CONDITION if product.allow_hard_deletes else None,
                stored_as_scd_type=1,
                name=view_name,  # explicit, unique flow name per driver
            )
            self.registered.append(Registration("view", view_name))
            self.registered.append(Registration("cdc_flow", target, source=view_name, flow_name=view_name))

    def build_trusted(self) -> None:
        for product in self.config.data_products:
            module = self.import_module(product.module)
            if not cdc.driver_names(getattr(module, "DATA_PRODUCT", {})):
                raise ValueError(f"{product.module} must define DATA_PRODUCT with at least one driver")
            if product.has_pii and self.naming.builds_vault:
                self._register_trusted(product, module, vaulted=True)
            self._register_trusted(product, module, vaulted=False)

    def build(self) -> list[Registration]:
        self.build_raw()
        self.build_trusted()
        log.info("Registered %d pipeline objects", len(self.registered))
        return self.registered


def build_pipeline(dp, spark, config: FrameworkConfig, naming: Naming, **kwargs) -> list[Registration]:
    return PipelineBuilder(dp, spark, config, naming, **kwargs).build()
