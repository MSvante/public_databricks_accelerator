"""Run each data product's transform on small BATCH DataFrames (Spark, skipped
without Java). ``read_changes_and_snapshots`` is patched, so this checks the
product logic and output contract, not streaming or Delta."""

import importlib
from datetime import datetime
from decimal import Decimal

import pytest

T0 = datetime(2026, 1, 1, 12, 0)
T1 = datetime(2026, 1, 2, 12, 0)


def raw(spark, ddl, rows):
    return spark.createDataFrame([(*r, T0) for r in rows], ddl + ", cdc_timestamp timestamp")


def changes(spark, keys_ddl, rows):
    return spark.createDataFrame([(*r, "insert", T1) for r in rows],
                                 keys_ddl + ", _change_type string, _event_time timestamp")


@pytest.fixture
def run(monkeypatch, cfg):
    def _run(module_name, change_df, snapshots):
        module = importlib.import_module(f"data_products.{module_name}")
        monkeypatch.setattr(module, "read_changes_and_snapshots", lambda *a, **k: (change_df, snapshots))
        out = module.transform(None, {}, cfg.key_registry)
        product = next(p for p in cfg.data_products if p.module.endswith(module_name))
        cols = set(out.columns)
        assert set(product.keys) <= cols
        assert {"is_deleted", "source_extract_datetime"} <= cols
        assert not cols & {"_change_type", "_event_time"}
        rows = {r[product.keys[0]]: r for r in out.collect()}
        assert all(r.source_extract_datetime is not None for r in rows.values())
        return rows

    return _run


def test_dim_product(spark, run):
    snaps = {
        "products": raw(spark, "product_id string, category_id string, unit_of_measure string, "
                               "net_weight_kg double, status string",
                        [("P1", "C1", "EA", 1.5, "A")]),
        "product_texts": raw(spark, "product_id string, language string, product_name string, description string",
                             [("P1", "EN", "Widget", "A widget")]),
        "product_categories": raw(spark, "category_id string, category_name string, parent_category_id string",
                                  [("C1", "Tools", None)]),
    }
    rows = run("dim_product", changes(spark, "product_id string", [("P1",), ("P_GONE",)]), snaps)
    p1 = next(r for r in rows.values() if r.product_id == "P1")
    assert (p1.product_name, p1.category_name, p1.is_active, p1.is_deleted) == ("Widget", "Tools", True, False)
    gone = next(r for r in rows.values() if r.product_id == "P_GONE")
    assert gone.is_deleted and gone.source_extract_datetime == T1  # falls back to event time


def test_fact_sales_order_line_amounts(spark, run):
    snaps = {
        "sales_order_lines": raw(spark, "order_id string, line_no int, product_id string, quantity int, "
                                        "unit_price decimal(18,2), discount_pct double",
                                 [("SO1", 1, "P1", 3, Decimal("10.00"), 10.0)]),
        "sales_orders": raw(spark, "order_id string, customer_id string, order_date date, currency string, "
                                   "sales_channel string, status string",
                            [("SO1", "C1", T0.date(), "EUR", "web", "OPEN")]),
        "products": raw(spark, "product_id string", [("P1",)]),
    }
    rows = run("fact_sales_order_line", changes(spark, "order_id string, line_no int", [("SO1", 1)]), snaps)
    line = rows["SO1"]
    assert line.gross_amount == Decimal("30.00")
    assert line.net_amount == Decimal("27.00")
    assert line.is_deleted is False


def test_dim_customer(spark, run):
    snaps = {"customers": raw(spark, "customer_id string, contact_name string, email string, phone string, "
                                     "country string, segment string, created_on date",
                              [("C1", "Contact 1", "c1@example.com", "+00 1", "DK", "retail", T0.date())])}
    rows = run("dim_customer", changes(spark, "customer_id string", [("C1",)]), snaps)
    assert rows["C1"].country == "DK" and rows["C1"].is_deleted is False
