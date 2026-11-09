# Data product module contract

A data product is one Python module plus one entry in `conf/data_products.yml`.
The pipeline engine is never edited to add one.

```python
DATA_PRODUCT = {
    "keys": ["order_id", "line_no"],          # join keys shared by every driver
    "drivers": [                              # changes here update the product
        {"name": "sales_order_lines"},
        # {"name": "other_table",
        #  "alias": {"source_col": "order_id"},          # map to the join keys
        #  "filters": [{"column": "x", "operator": "=", "value": "y"}]},
    ],
    "satellites": ["sales_orders"],           # static lookups, no trigger
}

def transform(spark, schemas, key_registry) -> DataFrame:
    changes, snap = read_changes_and_snapshots(spark, schemas, DATA_PRODUCT, key_registry)
    ...
```

## Rules

1. **Output must include** the target keys from the YAML, `is_deleted`
   (boolean) and `source_extract_datetime` (never NULL: fall back to
   `_event_time` from the change stream).
2. **Don't output** `_change_type` or `_event_time`; select explicit columns.
3. **Values come from snapshots**, not from the change stream. The stream only
   says *which key* changed. Replaying an old event then yields today's truth.
4. **Never union streams.** The engine already runs one flow per driver.
5. **Driver or satellite?** If a change in the table must reach the product
   within the next run, it's a driver. If it's a slowly changing lookup that can
   wait until the next driver change, it's a satellite.
6. **Filters operators**: `=`, `!=`, `IN`, `NOT IN`.

## Checklist for a new product

- [ ] module in `src/data_products/<name>.py`
- [ ] entry in `conf/data_products.yml` (`name`, `domain`, `module`, `keys`)
- [ ] `pii_columns` if any output column carries personal data
- [ ] a reconciliation rule in `conf/reconciliation.yml`
- [ ] `pytest` passes (the contract tests import every module)
