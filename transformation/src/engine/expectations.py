"""
Data Quality Expectations Engine for Transformation Pipeline.

This module handles the conversion of YAML expectation definitions to DLT
expectation decorators and provides utilities for data quality validation.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional

import dlt
from pyspark.sql import DataFrame

from .config_loader import Expectation


@dataclass
class DLTExpectation:
    """Represents a DLT expectation ready to be applied."""

    name: str
    expression: str
    action: str
    decorator: Optional[Callable] = None


class ExpectationsEngine:
    """
    Engine for managing data quality expectations in DLT pipelines.

    Converts YAML-defined expectations into DLT expectation decorators
    and provides utilities for applying them to DataFrames.
    """

    def __init__(self) -> None:
        """Initialize the expectations engine."""
        pass

    def build_expectation(self, expectation: Expectation) -> DLTExpectation:
        """
        Convert a YAML expectation to a DLT expectation.

        Args:
            expectation: Expectation configuration from YAML.

        Returns:
            DLTExpectation with the appropriate decorator.
        """
        decorator: Optional[Callable] = None

        if expectation.action == "drop":
            # Records that fail are dropped
            def drop_decorator(func: Callable) -> Callable:
                return dlt.expect_or_drop(
                    expectation.name, expectation.expression
                )(func)
            decorator = drop_decorator

        elif expectation.action == "fail":
            # Pipeline fails if any records fail
            def fail_decorator(func: Callable) -> Callable:
                return dlt.expect_or_fail(
                    expectation.name, expectation.expression
                )(func)
            decorator = fail_decorator

        else:  # warn (default)
            # Records are kept, but metrics are logged
            def warn_decorator(func: Callable) -> Callable:
                return dlt.expect(
                    expectation.name, expectation.expression
                )(func)
            decorator = warn_decorator

        return DLTExpectation(
            name=expectation.name,
            expression=expectation.expression,
            action=expectation.action,
            decorator=decorator,
        )

    def build_expectations(
        self, expectations: list[Expectation]
    ) -> list[DLTExpectation]:
        """
        Convert a list of YAML expectations to DLT expectations.

        Args:
            expectations: List of expectation configurations from YAML.

        Returns:
            List of DLTExpectation objects.
        """
        return [self.build_expectation(exp) for exp in expectations]

    def apply_expectations(
        self,
        df: DataFrame,
        expectations: list[Expectation],
        spark: Any,
    ) -> DataFrame:
        """
        Apply expectations to a DataFrame using SQL expressions.

        This method is for use outside of DLT decorators, applying
        expectations as DataFrame operations.

        Args:
            df: Input DataFrame.
            expectations: List of expectation configurations.
            spark: SparkSession for SQL operations.

        Returns:
            DataFrame with expectations applied (for drop actions).
        """
        result_df = df

        for exp in expectations:
            if exp.action == "drop":
                # Filter out rows that don't meet the expectation
                result_df = result_df.filter(exp.expression)
            elif exp.action == "fail":
                # Check if any rows fail the expectation
                failing_count = result_df.filter(f"NOT ({exp.expression})").count()
                if failing_count > 0:
                    raise ValueError(
                        f"Expectation '{exp.name}' failed: {failing_count} records "
                        f"do not satisfy '{exp.expression}'"
                    )
            # For 'warn', we just log (DLT handles this automatically)

        return result_df

    def get_expectations_dict(
        self, expectations: list[Expectation]
    ) -> dict[str, str]:
        """
        Convert expectations to a dictionary format for DLT.

        This format can be used with @dlt.expect_all or similar decorators.

        Args:
            expectations: List of expectation configurations.

        Returns:
            Dictionary mapping expectation names to expressions.
        """
        return {exp.name: exp.expression for exp in expectations}


def validate_expectation_expression(expression: str, spark: Any) -> bool:
    """
    Validate that an expectation expression is valid SQL.

    Args:
        expression: SQL boolean expression to validate.
        spark: SparkSession for parsing.

    Returns:
        True if the expression is valid, False otherwise.
    """
    try:
        # Try to parse the expression
        spark.sql(f"SELECT * FROM range(1) WHERE {expression}")
        return True
    except Exception:
        return False
