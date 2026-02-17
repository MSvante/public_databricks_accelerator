"""
Silver Layer Engine for Transformation Pipeline.

This module handles the silver (cleansed) layer of the transformation pipeline,
dynamically loading Python transform functions and applying data quality expectations.
"""

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Any, Callable, Optional

import dlt
from pyspark.sql import DataFrame

from .config_loader import SilverConfig, TableConfig
from .expectations import ExpectationsEngine


class SilverLayer:
    """
    Silver layer engine for applying transformations and data quality rules.

    Dynamically loads Python transform modules and applies expectations to create
    clean, validated data in the silver layer.
    """

    def __init__(
        self,
        spark: Any,
        config: SilverConfig,
        catalog: str,
        raw_schema: str = "raw",
        cleansed_schema: str = "cleansed",
    ) -> None:
        """
        Initialize the silver layer engine.

        Args:
            spark: SparkSession instance.
            config: Silver layer configuration.
            catalog: Unity Catalog name.
            raw_schema: Schema name for bronze tables.
            cleansed_schema: Schema name for silver tables.
        """
        self.spark = spark
        self.config = config
        self.catalog = catalog
        self.raw_schema = raw_schema
        self.cleansed_schema = cleansed_schema
        self.expectations_engine = ExpectationsEngine()
        self._transform_cache: dict[str, Callable] = {}

    def _load_transform_function(self, transform_path: str) -> Callable:
        """
        Dynamically load a transform function from a Python file.

        The transform function must be named 'transform' and accept DataFrames
        as arguments (one per raw table specified in raw_tables).

        Args:
            transform_path: Path to the transform module (relative to workspace).

        Returns:
            The transform function.

        Raises:
            ValueError: If the module doesn't have a 'transform' function.
        """
        if transform_path in self._transform_cache:
            return self._transform_cache[transform_path]

        # Convert workspace path to file path
        # In DLT, files are synced to /Workspace/<root_path>/<path>
        config_path = self.spark.conf.get("config_path")
        workspace_root = str(Path(config_path).parent)
        full_path = f"{workspace_root}/{transform_path}"

        # Load the module
        module_name = Path(transform_path).stem
        spec = importlib.util.spec_from_file_location(module_name, full_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load module from {full_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # Get the transform function
        if not hasattr(module, "transform"):
            raise ValueError(
                f"Module {transform_path} does not have a 'transform' function"
            )

        transform_fn = getattr(module, "transform")
        self._transform_cache[transform_path] = transform_fn
        return transform_fn

    def _get_table_reference(self, table_name: str) -> DataFrame:
        """
        Get a reference to a table (bronze or silver).

        Args:
            table_name: Name of the table.

        Returns:
            DataFrame reference using dlt.read or dlt.read_stream.
        """
        # Try to read as a streaming table first (for bronze tables)
        try:
            return dlt.read_stream(table_name)
        except Exception:
            # Fall back to batch read (for silver tables)
            return dlt.read(table_name)

    def _apply_transform(
        self, table_config: TableConfig
    ) -> Callable[[], DataFrame]:
        """
        Create a function that applies the transformation for a silver table.

        Args:
            table_config: Configuration for the table.

        Returns:
            A function that returns the transformed DataFrame.
        """
        raw_tables = table_config.get_raw_table_list()
        transform_path = table_config.transform
        expectations = table_config.expectations

        def transform_fn() -> DataFrame:
            # Load input DataFrames
            input_dfs = {
                table_name: self._get_table_reference(table_name)
                for table_name in raw_tables
            }

            # Load and call the transform function
            if transform_path:
                user_transform = self._load_transform_function(transform_path)

                # Get function signature to determine argument order
                sig = inspect.signature(user_transform)
                param_names = list(sig.parameters.keys())

                # Build arguments in the correct order
                args = [input_dfs[name] for name in param_names]
                result_df = user_transform(*args)
            else:
                # No transform specified, use first input table as-is
                if raw_tables:
                    result_df = input_dfs[raw_tables[0]]
                else:
                    raise ValueError(
                        f"Table {table_config.name} has no raw_tables or transform"
                    )

            return result_df

        return transform_fn

    def register_tables(self) -> None:
        """
        Register all silver tables with DLT.

        This method creates materialized views for each table defined in the
        silver configuration, applying transformations and expectations.
        """
        for table_config in self.config.tables:
            table_name = table_config.name
            expectations = table_config.expectations

            # Create the transform function
            transform_fn = self._apply_transform(table_config)

            # Build DLT expectations
            dlt_expectations = self.expectations_engine.build_expectations(
                expectations
            )

            # Register the table with DLT
            # Apply expectations based on their actions
            expect_decorators = []
            for exp in expectations:
                if exp.action == "drop":
                    expect_decorators.append(
                        dlt.expect_or_drop(exp.name, exp.expression)
                    )
                elif exp.action == "fail":
                    expect_decorators.append(
                        dlt.expect_or_fail(exp.name, exp.expression)
                    )
                else:  # warn
                    expect_decorators.append(
                        dlt.expect(exp.name, exp.expression)
                    )

            # Create table function with all decorators applied
            def make_decorated_fn(
                fn: Callable[[], DataFrame],
                decorators: list,
                name: str,
                src_system: str,
            ) -> None:
                @dlt.table(
                    name=name,
                    comment=f"Silver table for {name} from {src_system}",
                    schema=self.cleansed_schema,
                )
                def table_fn() -> DataFrame:
                    return fn()

                # Apply expectation decorators
                for decorator in decorators:
                    table_fn = decorator(table_fn)

            make_decorated_fn(
                transform_fn,
                expect_decorators,
                table_name,
                self.config.source_system,
            )


def create_silver_tables(
    spark: Any,
    config: SilverConfig,
    catalog: str,
    raw_schema: str = "raw",
    cleansed_schema: str = "cleansed",
) -> None:
    """
    Convenience function to create all silver tables from configuration.

    Args:
        spark: SparkSession instance.
        config: Silver layer configuration.
        catalog: Unity Catalog name.
        raw_schema: Schema name for bronze tables.
        cleansed_schema: Schema name for silver tables.
    """
    layer = SilverLayer(spark, config, catalog, raw_schema, cleansed_schema)
    layer.register_tables()
