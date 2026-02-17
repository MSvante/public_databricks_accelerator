"""
Configuration Loader for Transformation Engine.

This module handles loading and parsing YAML configuration files for bronze and silver
layer definitions. It resolves variable substitutions and validates configuration schema.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import re
import yaml


@dataclass
class Expectation:
    """Data quality expectation definition."""

    name: str
    expression: str
    action: str = "warn"  # drop | warn | fail

    def __post_init__(self) -> None:
        """Validate expectation configuration."""
        valid_actions = {"drop", "warn", "fail"}
        if self.action not in valid_actions:
            raise ValueError(
                f"Invalid action '{self.action}' for expectation '{self.name}'. "
                f"Must be one of: {valid_actions}"
            )


@dataclass
class TableConfig:
    """Configuration for a single table."""

    name: str
    source_path: Optional[str] = None  # For bronze tables
    source_format: str = "parquet"
    raw_tables: Optional[str] = None  # For silver tables (comma-separated)
    transform: Optional[str] = None  # Path to transform module
    expectations: list[Expectation] = field(default_factory=list)

    def get_raw_table_list(self) -> list[str]:
        """Parse raw_tables string into a list of table names."""
        if not self.raw_tables:
            return []
        return [t.strip() for t in self.raw_tables.split(",")]


@dataclass
class SchemaEvolutionConfig:
    """Schema evolution settings."""

    add_new_columns: bool = True
    merge_schema: bool = True


@dataclass
class DefaultsConfig:
    """Default configuration for tables."""

    source_format: str = "parquet"
    schema_evolution: SchemaEvolutionConfig = field(
        default_factory=SchemaEvolutionConfig
    )


@dataclass
class BronzeConfig:
    """Bronze layer configuration."""

    source_system: str
    layer: str
    tables: list[TableConfig]
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.layer != "bronze":
            raise ValueError(f"Expected layer 'bronze', got '{self.layer}'")


@dataclass
class SilverConfig:
    """Silver layer configuration."""

    source_system: str
    layer: str
    tables: list[TableConfig]

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.layer != "silver":
            raise ValueError(f"Expected layer 'silver', got '{self.layer}'")


class ConfigLoader:
    """
    Loads and parses YAML configuration files for the transformation engine.

    Handles variable substitution using Databricks Asset Bundle variables
    accessible via spark.conf.
    """

    # Pattern for variable substitution: ${var.name} or ${var.name}
    VAR_PATTERN = re.compile(r"\$\{var\.(\w+)\}")

    def __init__(self, spark: Any) -> None:
        """
        Initialize the config loader.

        Args:
            spark: SparkSession for accessing configuration values.
        """
        self.spark = spark
        self._var_cache: dict[str, str] = {}

    def _get_var(self, name: str) -> str:
        """
        Get a variable value from Spark configuration.

        Variables are set via pipeline configuration and accessible via spark.conf.

        Args:
            name: Variable name (without 'var.' prefix).

        Returns:
            Variable value.

        Raises:
            ValueError: If variable is not found.
        """
        if name in self._var_cache:
            return self._var_cache[name]

        try:
            value = self.spark.conf.get(name)
            self._var_cache[name] = value
            return value
        except Exception:
            raise ValueError(
                f"Variable '{name}' not found in pipeline configuration. "
                f"Ensure it's defined in databricks.yml and passed to the pipeline."
            )

    def _resolve_variables(self, value: Any) -> Any:
        """
        Recursively resolve variable substitutions in a value.

        Args:
            value: Value that may contain variable references.

        Returns:
            Value with variables resolved.
        """
        if isinstance(value, str):
            def replace_var(match: re.Match) -> str:
                var_name = match.group(1)
                return self._get_var(var_name)

            return self.VAR_PATTERN.sub(replace_var, value)

        elif isinstance(value, dict):
            return {k: self._resolve_variables(v) for k, v in value.items()}

        elif isinstance(value, list):
            return [self._resolve_variables(item) for item in value]

        return value

    def _load_yaml(self, path: str) -> dict[str, Any]:
        """
        Load and parse a YAML file with variable resolution.

        Args:
            path: Path to the YAML file.

        Returns:
            Parsed YAML content with variables resolved.
        """
        with open(path, "r") as f:
            content = yaml.safe_load(f)

        return self._resolve_variables(content)

    def _parse_expectations(
        self, expectations_data: list[dict[str, Any]]
    ) -> list[Expectation]:
        """Parse expectation configurations."""
        return [
            Expectation(
                name=exp["name"],
                expression=exp["expression"],
                action=exp.get("action", "warn"),
            )
            for exp in expectations_data
        ]

    def _parse_table(
        self, table_data: dict[str, Any], defaults: Optional[DefaultsConfig] = None
    ) -> TableConfig:
        """Parse a table configuration."""
        expectations = []
        if "expectations" in table_data:
            expectations = self._parse_expectations(table_data["expectations"])

        source_format = table_data.get("source_format")
        if source_format is None and defaults:
            source_format = defaults.source_format

        return TableConfig(
            name=table_data["name"],
            source_path=table_data.get("source_path"),
            source_format=source_format or "parquet",
            raw_tables=table_data.get("raw_tables"),
            transform=table_data.get("transform"),
            expectations=expectations,
        )

    def load_bronze_config(self, path: str) -> BronzeConfig:
        """
        Load bronze layer configuration from a YAML file.

        Args:
            path: Path to the bronze.yml file.

        Returns:
            BronzeConfig object with parsed configuration.
        """
        data = self._load_yaml(path)

        # Parse defaults
        defaults_data = data.get("defaults", {})
        schema_evolution_data = defaults_data.get("schema_evolution", {})
        schema_evolution = SchemaEvolutionConfig(
            add_new_columns=schema_evolution_data.get("add_new_columns", True),
            merge_schema=schema_evolution_data.get("merge_schema", True),
        )
        defaults = DefaultsConfig(
            source_format=defaults_data.get("source_format", "parquet"),
            schema_evolution=schema_evolution,
        )

        # Parse tables
        tables = [
            self._parse_table(table_data, defaults)
            for table_data in data.get("tables", [])
        ]

        return BronzeConfig(
            source_system=data["source_system"],
            layer=data["layer"],
            tables=tables,
            defaults=defaults,
        )

    def load_silver_config(self, path: str) -> SilverConfig:
        """
        Load silver layer configuration from a YAML file.

        Args:
            path: Path to the silver.yml file.

        Returns:
            SilverConfig object with parsed configuration.
        """
        data = self._load_yaml(path)

        # Parse tables
        tables = [
            self._parse_table(table_data)
            for table_data in data.get("tables", [])
        ]

        return SilverConfig(
            source_system=data["source_system"],
            layer=data["layer"],
            tables=tables,
        )

    def get_config_path(self, source_system: str, layer: str) -> str:
        """
        Construct the path to a configuration file.

        Args:
            source_system: Source system name (e.g., 'navision').
            layer: Layer name ('bronze' or 'silver').

        Returns:
            Full path to the configuration file.
        """
        config_base = self._get_var("config_path")
        return f"{config_base}/{source_system}/{layer}.yml"
