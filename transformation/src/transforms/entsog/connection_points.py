"""
Connection Points Transformation for ENTSOG.

This module transforms raw connection point reference data from the bronze layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(connection_points: DataFrame) -> DataFrame:
    """
    Clean and standardize connection point data.

    Args:
        connection_points: Raw connection points DataFrame from bronze layer.

    Returns:
        Cleaned connection points DataFrame for silver layer.
    """
    return (
        connection_points
        .filter(F.col("point_key").isNotNull())
        .withColumn("point_label", F.trim(F.col("pointLabel")))
        .withColumn("operator_key", F.trim(F.col("operatorKey")))
        .withColumn("operator_label", F.trim(F.col("operatorLabel")))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("_processed_at", F.current_timestamp())
        .dropDuplicates(["point_key"])
    )
