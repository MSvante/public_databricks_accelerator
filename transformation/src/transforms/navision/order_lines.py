"""
Order Lines Transformation for Navision.

This module transforms raw order line data from the bronze layer and enriches
it with order and product information from the silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(
    order_lines: DataFrame,
    orders_enriched: DataFrame,
    products_clean: DataFrame
) -> DataFrame:
    """
    Enrich order lines with order and product data.

    Transformations applied:
        - Filter out records with null line_id
        - Validate quantity is positive
        - Join with enriched orders for order context
        - Join with cleaned products for product details
        - Calculate line total
        - Deduplicate by line_id

    Args:
        order_lines: Raw order lines DataFrame from bronze layer.
        orders_enriched: Enriched orders DataFrame from silver layer.
        products_clean: Cleaned products DataFrame from silver layer.

    Returns:
        Enriched order lines DataFrame for silver layer.
    """
    # Select relevant order fields for the join
    order_fields = orders_enriched.select(
        F.col("order_id"),
        F.col("order_date"),
        F.col("order_status"),
        F.col("customer_id"),
        F.col("customer_name"),
    )

    # Select relevant product fields for the join
    product_fields = products_clean.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category").alias("product_category"),
        F.col("unit_price").alias("product_unit_price"),
        F.col("sku").alias("product_sku"),
    )

    return (
        order_lines
        # Filter invalid records
        .filter(F.col("line_id").isNotNull())

        # Ensure quantity is positive
        .withColumn(
            "quantity",
            F.when(
                F.col("quantity") > 0,
                F.col("quantity")
            ).otherwise(F.lit(1))
        )

        # Get unit price from line or default to 0
        .withColumn(
            "unit_price",
            F.coalesce(
                F.col("unit_price"),
                F.col("price"),
                F.lit(0.0)
            ).cast("double")
        )

        # Calculate discount (default to 0)
        .withColumn(
            "discount",
            F.coalesce(F.col("discount"), F.lit(0.0)).cast("double")
        )

        # Calculate line total
        .withColumn(
            "line_total",
            (F.col("quantity") * F.col("unit_price")) - F.col("discount")
        )

        # Ensure line_total is non-negative
        .withColumn(
            "line_total",
            F.when(F.col("line_total") < 0, F.lit(0.0))
            .otherwise(F.col("line_total"))
        )

        # Join with order data
        .join(
            order_fields,
            on="order_id",
            how="left"
        )

        # Join with product data
        .join(
            product_fields,
            on="product_id",
            how="left"
        )

        # Use product unit price if line unit price is 0
        .withColumn(
            "unit_price",
            F.when(
                F.col("unit_price") == 0,
                F.coalesce(F.col("product_unit_price"), F.lit(0.0))
            ).otherwise(F.col("unit_price"))
        )

        # Recalculate line total with potentially updated unit price
        .withColumn(
            "line_total",
            (F.col("quantity") * F.col("unit_price")) - F.col("discount")
        )

        # Add match flags
        .withColumn(
            "_order_matched",
            F.col("order_date").isNotNull()
        )
        .withColumn(
            "_product_matched",
            F.col("product_name").isNotNull()
        )

        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate by line_id, keeping the most recent record
        .withColumn(
            "_row_num",
            F.row_number().over(
                F.Window.partitionBy("line_id")
                .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
