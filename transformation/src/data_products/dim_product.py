"""dim_product: TWO drivers and one satellite.

* products       driver   (attributes)
* product_texts  driver   (English name and description; a text change must
                           update the dimension too, so it is a driver)
* product_categories      satellite (category name lookup; a rename alone
                           does not trigger a recompute)

The engine runs this once per driver, as two AUTO CDC flows into one table.
"""

from pyspark.sql import functions as F

from framework.cdc import read_changes_and_snapshots
from framework.keys import surrogate_key

DATA_PRODUCT = {
    "keys": ["product_id"],
    "drivers": [
        {"name": "products"},
        {
            "name": "product_texts",
            "filters": [{"column": "language", "operator": "=", "value": "EN"}],
        },
    ],
    "satellites": ["product_categories"],
}


def transform(spark, schemas, key_registry):
    changes, snap = read_changes_and_snapshots(spark, schemas, DATA_PRODUCT, key_registry)

    c = changes.alias("c")
    p = snap["products"].alias("p")
    t = snap["product_texts"].alias("t")
    cat = snap["product_categories"].alias("cat")

    df = (
        c.join(p, F.col("c.product_id") == F.col("p.product_id"), "left")
        .join(t, F.col("c.product_id") == F.col("t.product_id"), "left")
        .join(cat, F.col("p.category_id") == F.col("cat.category_id"), "left")
    )

    return df.select(
        surrogate_key("c.product_id").alias("product_key"),
        F.col("c.product_id").alias("product_id"),
        F.col("t.product_name").alias("product_name"),
        F.col("t.description").alias("description"),
        F.col("p.category_id").alias("category_id"),
        F.col("cat.category_name").alias("category_name"),
        F.col("p.unit_of_measure").alias("unit_of_measure"),
        F.col("p.net_weight_kg").cast("decimal(12,3)").alias("net_weight_kg"),
        (F.col("p.status") == F.lit("A")).alias("is_active"),
        # Deleted when the main driver row no longer exists in raw.
        F.col("p.product_id").isNull().alias("is_deleted"),
        # Sequence for the SCD1 merge. Must never be NULL, so fall back to the
        # change event's commit time when the row is gone.
        F.coalesce(
            F.greatest(F.col("p.cdc_timestamp"), F.col("t.cdc_timestamp")),
            F.col("c._event_time"),
        ).alias("source_extract_datetime"),
        F.current_timestamp().alias("target_updated_at"),
    )
