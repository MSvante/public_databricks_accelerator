"""
Physical Flows Transformation for ENTSOG.

This module transforms raw physical gas flow data from the bronze layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(physical_flows: DataFrame) -> DataFrame:
    """
    Clean and standardize physical flow data.

    Args:
        physical_flows: Raw physical flows DataFrame from bronze layer.

    Returns:
        Cleaned physical flows DataFrame for silver layer.
    """
    return (
        physical_flows
        # Filter invalid records
        .filter(F.col("point_key").isNotNull())

        # Standardize column names
        .withColumnRenamed("pointKey", "point_key")

        # Parse and validate numeric values
        .withColumn(
            "value",
            F.col("value").cast("double")
        )

        # Standardize direction indicator
        .withColumn(
            "direction",
            F.upper(F.trim(F.col("direction")))
        )

        # Parse period timestamps
        .withColumn(
            "period_from",
            F.to_timestamp(F.col("periodFrom"))
        )
        .withColumn(
            "period_to",
            F.to_timestamp(F.col("periodTo"))
        )

        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate
        .dropDuplicates(["point_key", "period_from", "direction"])
    )
