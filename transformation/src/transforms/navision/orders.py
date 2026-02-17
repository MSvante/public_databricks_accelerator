"""
Orders Transformation for Navision.

This module transforms raw order data from the bronze layer and enriches
it with customer information from the silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(orders: DataFrame, customers_clean: DataFrame) -> DataFrame:
    """
    Enrich orders with customer data.

    Transformations applied:
        - Filter out records with null order_id
        - Parse and validate order dates
        - Join with cleaned customer data
        - Calculate order total if not present
        - Add order status standardization
        - Deduplicate by order_id

    Args:
        orders: Raw orders DataFrame from bronze layer.
        customers_clean: Cleaned customers DataFrame from silver layer.

    Returns:
        Enriched orders DataFrame for silver layer.
    """
    # Select relevant customer fields for the join
    customer_fields = customers_clean.select(
        F.col("customer_id"),
        F.col("name").alias("customer_name"),
        F.col("email").alias("customer_email"),
        F.col("city").alias("customer_city"),
        F.col("country").alias("customer_country"),
    )

    return (
        orders
        # Filter invalid records
        .filter(F.col("order_id").isNotNull())

        # Parse order date (handle multiple formats)
        .withColumn(
            "order_date",
            F.coalesce(
                F.to_date(F.col("order_date"), "yyyy-MM-dd"),
                F.to_date(F.col("order_date"), "dd/MM/yyyy"),
                F.to_date(F.col("order_date"), "MM/dd/yyyy"),
                F.col("order_date").cast("date"),
            )
        )

        # Standardize order status
        .withColumn(
            "order_status",
            F.when(F.upper(F.col("status")).isin("COMPLETED", "COMPLETE", "DONE"), "COMPLETED")
            .when(F.upper(F.col("status")).isin("PENDING", "NEW", "OPEN"), "PENDING")
            .when(F.upper(F.col("status")).isin("CANCELLED", "CANCELED", "VOID"), "CANCELLED")
            .when(F.upper(F.col("status")).isin("SHIPPED", "DISPATCHED", "SENT"), "SHIPPED")
            .when(F.upper(F.col("status")).isin("DELIVERED", "RECEIVED"), "DELIVERED")
            .otherwise(F.upper(F.col("status")))
        )

        # Calculate order total if not present
        .withColumn(
            "order_total",
            F.coalesce(
                F.col("order_total"),
                F.col("total_amount"),
                F.col("amount"),
                F.lit(0.0)
            ).cast("double")
        )

        # Ensure order total is non-negative
        .withColumn(
            "order_total",
            F.when(F.col("order_total") < 0, F.lit(0.0))
            .otherwise(F.col("order_total"))
        )

        # Join with customer data
        .join(
            customer_fields,
            on="customer_id",
            how="left"
        )

        # Add flag for orders without customer match
        .withColumn(
            "_customer_matched",
            F.col("customer_name").isNotNull()
        )

        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())

        # Deduplicate by order_id, keeping the most recent record
        .withColumn(
            "_row_num",
            F.row_number().over(
                F.Window.partitionBy("order_id")
                .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
