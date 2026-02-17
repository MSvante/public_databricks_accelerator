"""
Vendors Transformation for Navision.

This module transforms raw vendor data from the bronze layer,
cleaning and standardizing fields for the silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(vendors: DataFrame) -> DataFrame:
    """
    Clean and standardize vendor data.

    Transformations applied:
        - Filter out records with null vendor_id
        - Standardize vendor names
        - Standardize contact information
        - Deduplicate by vendor_id

    Args:
        vendors: Raw vendors DataFrame from bronze layer.

    Returns:
        Cleaned vendors DataFrame for silver layer.
    """
    return (
        vendors
        # Filter invalid records
        .filter(F.col("vendor_id").isNotNull())

        # Standardize vendor name
        .withColumn(
            "vendor_name",
            F.when(
                F.col("vendor_name").isNotNull(),
                F.trim(F.col("vendor_name"))
            ).otherwise(
                F.coalesce(F.col("name"), F.col("company_name"))
            )
        )

        # Standardize email
        .withColumn(
            "email",
            F.when(
                F.col("email").isNotNull(),
                F.lower(F.trim(F.col("email")))
            ).otherwise(None)
        )

        # Standardize phone
        .withColumn(
            "phone",
            F.when(
                F.col("phone").isNotNull(),
                F.regexp_replace(F.col("phone"), r"[^\d+]", "")
            ).otherwise(None)
        )

        # Standardize address
        .withColumn(
            "address",
            F.when(
                F.col("address").isNotNull(),
                F.trim(F.col("address"))
            ).otherwise(None)
        )

        # Standardize country (uppercase ISO code)
        .withColumn(
            "country",
            F.when(
                F.col("country").isNotNull(),
                F.upper(F.trim(F.col("country")))
            ).otherwise(None)
        )

        # Add active flag
        .withColumn(
            "is_active",
            F.when(
                F.upper(F.col("status")).isin("ACTIVE", "APPROVED"),
                F.lit(True)
            ).when(
                F.upper(F.col("status")).isin("INACTIVE", "BLOCKED", "SUSPENDED"),
                F.lit(False)
            ).otherwise(F.lit(True))
        )

        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate by vendor_id, keeping the most recent record
        .withColumn(
            "_row_num",
            F.row_number().over(
                F.Window.partitionBy("vendor_id")
                .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
