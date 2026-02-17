"""
Allocations Transformation for ENTSOG.

This module transforms raw gas allocation data from the bronze layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(allocations: DataFrame) -> DataFrame:
    """
    Clean and standardize allocation data.

    Args:
        allocations: Raw allocations DataFrame from bronze layer.

    Returns:
        Cleaned allocations DataFrame for silver layer.
    """
    return (
        allocations
        .filter(F.col("allocation_id").isNotNull())
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("period_from", F.to_timestamp(F.col("periodFrom")))
        .withColumn("period_to", F.to_timestamp(F.col("periodTo")))
        .withColumn("_processed_at", F.current_timestamp())
        .dropDuplicates(["allocation_id", "period_from"])
    )
