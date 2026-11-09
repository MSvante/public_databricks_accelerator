"""fact_sales_order_line: one driver, two satellites, hard deletes.

* sales_order_lines  driver
* sales_orders       satellite (header: order date, customer, channel)
* products           satellite (only needed for the product key's existence)

Known trade-off, on purpose: if ONLY the header changes (say the order date is
corrected), no line changes, so no event reaches this table and the fact keeps
the old order date until a line is touched. That is acceptable for header
attributes that practically never change. If one does change often, make the
header a second driver; it must then share this product's join keys (via
``alias``), or get its own fact at header grain.
"""

from pyspark.sql import functions as F

from framework.cdc import read_changes_and_snapshots
from framework.keys import surrogate_key

DATA_PRODUCT = {
    "keys": ["order_id", "line_no"],
    "drivers": [{"name": "sales_order_lines"}],
    "satellites": ["sales_orders", "products"],
}


def transform(spark, schemas, key_registry):
    changes, snap = read_changes_and_snapshots(spark, schemas, DATA_PRODUCT, key_registry)

    c = changes.alias("c")
    ln = snap["sales_order_lines"].alias("ln")
    hd = snap["sales_orders"].alias("hd")

    df = c.join(
        ln,
        (F.col("c.order_id") == F.col("ln.order_id")) & (F.col("c.line_no") == F.col("ln.line_no")),
        "left",
    ).join(hd, F.col("c.order_id") == F.col("hd.order_id"), "left")

    gross = F.col("ln.quantity") * F.col("ln.unit_price")
    discount = F.coalesce(F.col("ln.discount_pct"), F.lit(0)) / F.lit(100)

    return df.select(
        surrogate_key("c.order_id", "c.line_no").alias("sales_order_line_key"),
        F.col("c.order_id").alias("order_id"),
        F.col("c.line_no").alias("line_no"),
        F.col("hd.order_date").cast("date").alias("order_date"),
        surrogate_key("hd.customer_id").alias("customer_key"),
        surrogate_key("ln.product_id").alias("product_key"),
        F.col("hd.sales_channel").alias("sales_channel"),
        F.col("hd.currency").alias("currency"),
        F.col("ln.quantity").alias("quantity"),
        F.col("ln.unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.round(gross, 2).cast("decimal(18,2)").alias("gross_amount"),
        F.round(gross * (F.lit(1) - discount), 2).cast("decimal(18,2)").alias("net_amount"),
        F.col("ln.order_id").isNull().alias("is_deleted"),
        F.coalesce(F.col("ln.cdc_timestamp"), F.col("c._event_time")).alias("source_extract_datetime"),
        F.current_timestamp().alias("target_updated_at"),
    )
