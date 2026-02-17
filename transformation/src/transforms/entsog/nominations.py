"""
Nominations Transformation for ENTSOG.

This module transforms raw gas nomination data from the bronze layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(nominations: DataFrame) -> DataFrame:
    """
    Clean and standardize nomination data.

    Args:
        nominations: Raw nominations DataFrame from bronze layer.

    Returns:
        Cleaned nominations DataFrame for silver layer.
    """
    return (
        nominations
        .filter(F.col("nomination_id").isNotNull())
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("period_from", F.to_timestamp(F.col("periodFrom")))
        .withColumn("period_to", F.to_timestamp(F.col("periodTo")))
        .withColumn("_processed_at", F.current_timestamp())
        .dropDuplicates(["nomination_id", "period_from"])
    )
