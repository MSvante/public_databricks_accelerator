"""
Products Transformation for Navision.

This module transforms raw product data from the bronze layer,
cleaning and standardizing fields for the silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(products: DataFrame) -> DataFrame:
    """
    Clean and standardize product data.

    Transformations applied:
        - Filter out records with null product_id
        - Standardize product names
        - Ensure prices are non-negative
        - Standardize category names
        - Deduplicate by product_id

    Args:
        products: Raw products DataFrame from bronze layer.

    Returns:
        Cleaned products DataFrame for silver layer.
    """
    return (
        products
        # Filter invalid records
        .filter(F.col("product_id").isNotNull())

        # Standardize product name
        .withColumn(
            "product_name",
            F.when(
                F.col("product_name").isNotNull(),
                F.trim(F.col("product_name"))
            ).otherwise(
                F.coalesce(F.col("name"), F.col("description"))
            )
        )

        # Ensure unit_price is non-negative
        .withColumn(
            "unit_price",
            F.when(
                F.col("unit_price") < 0,
                F.lit(0.0)
            ).otherwise(
                F.coalesce(F.col("unit_price"), F.col("price"), F.lit(0.0))
            ).cast("double")
        )

        # Standardize category
        .withColumn(
            "category",
            F.when(
                F.col("category").isNotNull(),
                F.initcap(F.trim(F.col("category")))
            ).otherwise(None)
        )

        # Standardize SKU (uppercase, no spaces)
        .withColumn(
            "sku",
            F.when(
                F.col("sku").isNotNull(),
                F.upper(F.regexp_replace(F.col("sku"), r"\s+", ""))
            ).otherwise(None)
        )

        # Add active flag based on status
        .withColumn(
            "is_active",
            F.when(
                F.upper(F.col("status")).isin("ACTIVE", "AVAILABLE", "IN_STOCK"),
                F.lit(True)
            ).when(
                F.upper(F.col("status")).isin("INACTIVE", "DISCONTINUED", "OUT_OF_STOCK"),
                F.lit(False)
            ).otherwise(F.lit(True))  # Default to active
        )

        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate by product_id, keeping the most recent record
        .withColumn(
            "_row_num",
            F.row_number().over(
                F.Window.partitionBy("product_id")
                .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
