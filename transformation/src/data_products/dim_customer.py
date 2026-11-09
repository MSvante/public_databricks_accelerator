"""dim_customer: one driver, PII.

The YAML marks contact_name, email and phone as PII, so the engine builds two
variants of this table from the SAME code:

* pii_trusted_master_data.dim_customer  reads the vaulted raw table (clear text)
* trusted_master_data.dim_customer      reads the public raw table (hashed)

Nothing in this module knows about PII. That is the point.
"""

from pyspark.sql import functions as F

from framework.cdc import read_changes_and_snapshots
from framework.keys import surrogate_key

DATA_PRODUCT = {
    "keys": ["customer_id"],
    "drivers": [{"name": "customers"}],
    "satellites": [],
}


def transform(spark, schemas, key_registry):
    changes, snap = read_changes_and_snapshots(spark, schemas, DATA_PRODUCT, key_registry)

    c = changes.alias("c")
    cu = snap["customers"].alias("cu")

    return c.join(cu, F.col("c.customer_id") == F.col("cu.customer_id"), "left").select(
        surrogate_key("c.customer_id").alias("customer_key"),
        F.col("c.customer_id").alias("customer_id"),
        F.col("cu.contact_name").alias("contact_name"),
        F.col("cu.email").alias("email"),
        F.col("cu.phone").alias("phone"),
        F.col("cu.country").alias("country"),
        F.col("cu.segment").alias("segment"),
        F.col("cu.created_on").cast("date").alias("created_on"),
        F.col("cu.customer_id").isNull().alias("is_deleted"),
        F.coalesce(F.col("cu.cdc_timestamp"), F.col("c._event_time")).alias("source_extract_datetime"),
        F.current_timestamp().alias("target_updated_at"),
    )
