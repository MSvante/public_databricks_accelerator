"""
Customer Transformation for Navision.

This module transforms raw customer data from the bronze layer,
cleaning and standardizing fields for the silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(customers: DataFrame) -> DataFrame:
    """
    Clean and standardize customer data.

    Transformations applied:
        - Filter out records with null customer_id
        - Standardize email to lowercase
        - Standardize name with proper casing
        - Trim whitespace from text fields
        - Deduplicate by customer_id (keep latest by _ingested_at)

    Args:
        customers: Raw customer DataFrame from bronze layer.

    Returns:
        Cleaned customer DataFrame for silver layer.
    """
    return (
        customers
        # Filter invalid records
        .filter(F.col("customer_id").isNotNull())

        # Standardize email
        .withColumn(
            "email",
            F.when(
                F.col("email").isNotNull(),
                F.lower(F.trim(F.col("email")))
            ).otherwise(None)
        )

        # Standardize name
        .withColumn(
            "name",
            F.when(
                F.col("name").isNotNull(),
                F.initcap(F.trim(F.col("name")))
            ).otherwise(None)
        )

        # Standardize phone (remove non-digits, format)
        .withColumn(
            "phone",
            F.when(
                F.col("phone").isNotNull(),
                F.regexp_replace(F.col("phone"), r"[^\d+]", "")
            ).otherwise(None)
        )

        # Standardize address fields
        .withColumn(
            "address",
            F.when(
                F.col("address").isNotNull(),
                F.trim(F.col("address"))
            ).otherwise(None)
        )
        .withColumn(
            "city",
            F.when(
                F.col("city").isNotNull(),
                F.initcap(F.trim(F.col("city")))
            ).otherwise(None)
        )
        .withColumn(
            "country",
            F.when(
                F.col("country").isNotNull(),
                F.upper(F.trim(F.col("country")))
            ).otherwise(None)
        )

        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate by customer_id, keeping the most recent record
        .withColumn(
            "_row_num",
            F.row_number().over(
                F.Window.partitionBy("customer_id")
                .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
