"""Write fictional landing files so the pipeline can run end to end.

    --batch initial     first full load (every row _op = I)
    --batch changes     updates, deletes and new rows. Includes two changes that
                        DON'T reach the trusted layer on their own (a header date
                        and a category rename: satellite-only changes)
    --batch type_drift  sales_order_lines with quantity as DECIMAL instead of INT,
                        to trigger the schema guard

All data is generated and fictional.
"""

import os
import random
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.join(sys.argv[sys.argv.index("--bundle-root") + 1], "src"))

from pyspark.sql import SparkSession  # noqa: E402

from framework.jobargs import parse_args  # noqa: E402

args = parse_args(("--batch", {"choices": ["initial", "changes", "type_drift"], "default": "initial"}))
spark = SparkSession.builder.getOrCreate()
rng = random.Random(42)
now = datetime.now(UTC).replace(microsecond=0, tzinfo=None)
stamp = now.strftime("%Y%m%dT%H%M%S")

SCHEMAS = {
    "products": "product_id string, category_id string, unit_of_measure string, net_weight_kg double, status string",
    "product_texts": "product_id string, language string, product_name string, description string",
    "product_categories": "category_id string, category_name string, parent_category_id string",
    "customers": "customer_id string, contact_name string, email string, phone string, country string, segment string, created_on date",
    "sales_orders": "order_id string, customer_id string, order_date date, currency string, sales_channel string, status string",
    "sales_order_lines": "order_id string, line_no int, product_id string, quantity int, unit_price decimal(18,2), discount_pct double",
}
DOMAIN = {
    "products": "master_data", "product_texts": "master_data", "product_categories": "master_data",
    "customers": "master_data", "sales_orders": "sales", "sales_order_lines": "sales",
}


def write(table, rows, op, schema=None):
    if not rows:
        return
    ddl = (schema or SCHEMAS[table]) + ", _op string, _extracted_at timestamp"
    df = spark.createDataFrame([(*r, op, now) for r in rows], ddl)
    # Timestamp first, so folder names sort chronologically (the schema guard relies on it).
    path = f"{args.landing_root}/{DOMAIN[table]}/{table}/{stamp}_{args.batch}_{op}"
    df.coalesce(1).write.mode("errorifexists").parquet(path)
    print(f"{table:20s} {op} {len(rows):4d} rows -> {path}")


categories = [(f"CAT{i:02d}", f"Category {i}", None) for i in range(1, 6)]
products = [
    (f"P{i:04d}", f"CAT{rng.randint(1, 5):02d}", rng.choice(["EA", "KG", "BOX"]),
     round(rng.uniform(0.2, 20), 3), "A")
    for i in range(1, 21)
]
texts = [(p[0], lang, f"Product {p[0][1:]} ({lang})", f"Sample description {p[0]}")
         for p in products for lang in ("EN", "DE")]
customers = [
    (f"C{i:04d}", f"Contact {i:04d}", f"contact{i:04d}@example.com", f"+00 000 {i:04d}",
     rng.choice(["DK", "SE", "NO", "DE", "NL"]), rng.choice(["retail", "wholesale", "online"]),
     date(2024, 1, 1) + timedelta(days=rng.randint(0, 600)))
    for i in range(1, 31)
]
orders = [
    (f"SO{i:05d}", f"C{rng.randint(1, 30):04d}", date(2026, 1, 1) + timedelta(days=rng.randint(0, 300)),
     "EUR", rng.choice(["web", "store", "partner"]), "OPEN")
    for i in range(1, 51)
]
lines = [
    (o[0], n, f"P{rng.randint(1, 20):04d}", rng.randint(1, 12),
     Decimal(str(round(rng.uniform(2, 250), 2))), rng.choice([0.0, 0.0, 5.0, 10.0]))
    for o in orders for n in range(1, rng.randint(2, 5))
]

if args.batch == "initial":
    for table, rows in [("product_categories", categories), ("products", products), ("product_texts", texts),
                        ("customers", customers), ("sales_orders", orders), ("sales_order_lines", lines)]:
        write(table, rows, "I")

elif args.batch == "changes":
    # Driver changes: these reach the trusted tables on the next run.
    write("products", [(*p[:3], round(p[3] * 1.1, 3), "B" if i == 0 else p[4]) for i, p in enumerate(products[:3])], "U")
    write("product_texts", [(texts[0][0], "EN", "Renamed product", texts[0][3])], "U")
    write("customers", [(*customers[0][:2], "new.address@example.com", *customers[0][3:])], "U")
    write("sales_order_lines", [lines[0], lines[1]], "D")
    new_orders = [(f"SO9{i:04d}", "C0001", date(2026, 11, 1), "EUR", "web", "OPEN") for i in range(1, 4)]
    write("sales_orders", new_orders, "I")
    write("sales_order_lines", [(o[0], 1, "P0001", 2, Decimal("19.99"), 0.0) for o in new_orders], "I")
    # Satellite-only changes: NOT visible in the trusted layer until a driver
    # touches the same key. This is the documented trade-off.
    write("sales_orders", [(orders[5][0], orders[5][1], date(2026, 12, 24), *orders[5][3:])], "U")
    write("product_categories", [("CAT01", "Category 1 (renamed)", None)], "U")

elif args.batch == "type_drift":
    drift_schema = SCHEMAS["sales_order_lines"].replace("quantity int", "quantity decimal(18,3)")
    write("sales_order_lines",
          [(orders[10][0], 99, "P0002", Decimal("1.500"), Decimal("9.99"), 0.0)], "I", schema=drift_schema)
