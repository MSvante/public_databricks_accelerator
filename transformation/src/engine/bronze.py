"""
Bronze Layer Engine for Transformation Pipeline.

This module handles the bronze (raw) layer of the transformation pipeline,
using Auto Loader to ingest data from the landing zone with housekeeping columns.
"""

from typing import Any, Callable
import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config_loader import BronzeConfig, TableConfig


class BronzeLayer:
    """
    Bronze layer engine for ingesting raw data from the landing zone.

    Uses Auto Loader (cloudFiles) to incrementally read new files and adds
    standard housekeeping columns to all tables.
    """

    def __init__(self, spark: Any, config: BronzeConfig) -> None:
        """
        Initialize the bronze layer engine.

        Args:
            spark: SparkSession instance.
            config: Bronze layer configuration.
        """
        self.spark = spark
        self.config = config

    def _add_housekeeping_columns(self, df: DataFrame) -> DataFrame:
        """
        Add standard housekeeping columns to a DataFrame.

        Columns added:
            - _ingested_at: Timestamp when the record was ingested
            - _source_file: Full path of the source file
            - _modified_at: Last modification time of the source file
            - _source_system: Source system identifier

        Args:
            df: Input DataFrame from Auto Loader.

        Returns:
            DataFrame with housekeeping columns added.
        """
        return (
            df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn(
                "_modified_at",
                F.col("_metadata.file_modification_time")
            )
            .withColumn("_source_system", F.lit(self.config.source_system))
        )

    def _get_auto_loader_options(self, table_config: TableConfig) -> dict[str, str]:
        """
        Get Auto Loader options for a table.

        Args:
            table_config: Table configuration.

        Returns:
            Dictionary of Auto Loader options.
        """
        options = {
            "cloudFiles.format": table_config.source_format,
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns"
            if self.config.defaults.schema_evolution.add_new_columns
            else "none",
        }

        if self.config.defaults.schema_evolution.merge_schema:
            options["mergeSchema"] = "true"

        return options

    def create_table(self, table_config: TableConfig) -> Callable[[], DataFrame]:
        """
        Create a DLT table definition for a bronze table.

        Args:
            table_config: Configuration for the table.

        Returns:
            A function that returns the DataFrame for the table.
        """
        source_path = table_config.source_path
        options = self._get_auto_loader_options(table_config)
        source_system = self.config.source_system

        # Create the table function
        def create_df() -> DataFrame:
            df = (
                self.spark.readStream
                .format("cloudFiles")
                .options(**options)
                .load(source_path)
            )
            return self._add_housekeeping_columns(df)

        return create_df

    def register_tables(self) -> None:
        """
        Register all bronze tables with DLT.

        This method creates streaming tables for each table defined in the
        bronze configuration using Auto Loader.
        """
        for table_config in self.config.tables:
            table_name = table_config.name
            source_path = table_config.source_path
            options = self._get_auto_loader_options(table_config)

            # Create a closure to capture the table-specific values
            def make_table_fn(
                path: str, opts: dict[str, str], src_system: str
            ) -> Callable[[], DataFrame]:
                def table_fn() -> DataFrame:
                    df = (
                        self.spark.readStream
                        .format("cloudFiles")
                        .options(**opts)
                        .load(path)
                    )
                    return (
                        df
                        .withColumn("_ingested_at", F.current_timestamp())
                        .withColumn("_source_file", F.input_file_name())
                        .withColumn(
                            "_modified_at",
                            F.col("_metadata.file_modification_time")
                        )
                        .withColumn("_source_system", F.lit(src_system))
                    )
                return table_fn

            # Register the streaming table with DLT
            table_fn = make_table_fn(
                source_path, options, self.config.source_system
            )

            dlt.table(
                name=table_name,
                comment=f"Bronze table for {table_name} from {self.config.source_system}",
            )(table_fn)


def create_bronze_tables(spark: Any, config: BronzeConfig) -> None:
    """
    Convenience function to create all bronze tables from configuration.

    Args:
        spark: SparkSession instance.
        config: Bronze layer configuration.
    """
    layer = BronzeLayer(spark, config)
    layer.register_tables()
