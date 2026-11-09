"""CDC scoping (pure Python) plus the Spark-backed behaviour (skipped without Java)."""

from decimal import Decimal

import pytest

from framework import cdc
from framework.reconciliation import Rule, parse_rules, render_query

TWO_DRIVERS = {"keys": ["id"], "drivers": [{"name": "a"}, {"name": "b"}], "satellites": ["c", "a"]}


def test_multiple_drivers_require_a_scope():
    with pytest.raises(ValueError, match="never unioned"):
        cdc.selected_driver(TWO_DRIVERS)
    with cdc.driver_scope("b"):
        assert cdc.selected_driver(TWO_DRIVERS)["name"] == "b"
    with cdc.driver_scope("zzz"), pytest.raises(ValueError, match="not declared"):
        cdc.selected_driver(TWO_DRIVERS)


def test_single_driver_needs_no_scope():
    assert cdc.selected_driver({"drivers": [{"name": "a"}]})["name"] == "a"


def test_input_tables_dedupes_drivers_first():
    assert cdc.input_tables(TWO_DRIVERS) == ["a", "b", "c"]


def test_keys_fall_back_to_first_driver():
    assert cdc.resolve_keys({"drivers": [{"name": "a"}]}, {"a": ["x", "y"]}) == ["x", "y"]
    with pytest.raises(ValueError):
        cdc.resolve_keys({"drivers": [{"name": "a"}]}, {})


def test_reconciliation_rules_and_placeholders():
    rules = parse_rules({"rules": [{"rule_id": 1, "table_name": "t", "key_columns": "a, b",
                                    "raw_query": "q1", "trusted_query": "q2"}]})
    assert rules[0].key_columns == ("a", "b")
    q = render_query("SELECT * FROM {raw_master_data}.x JOIN {trusted_sales}.y",
                     lambda layer, domain: f"cat.{layer}_{domain}")
    assert q == "SELECT * FROM cat.raw_master_data.x JOIN cat.trusted_sales.y"


def test_shipped_reconciliation_config_parses():
    from pathlib import Path

    import yaml

    doc = yaml.safe_load((Path(__file__).resolve().parents[1] / "conf" / "reconciliation.yml").read_text())
    assert len(parse_rules(doc)) >= 3


# --------------------------------------------------------------- Spark-backed

def test_mask_columns_hashes_and_fails_on_missing(spark):
    from framework.pii import PiiColumnMissing, mask_columns

    df = spark.createDataFrame([("1", "a@example.com")], "id string, email string")
    out = mask_columns(df, ["email"]).collect()[0]
    assert out.id == "1"
    assert len(out.email) == 64 and out.email != "a@example.com"
    with pytest.raises(PiiColumnMissing):
        mask_columns(df, ["phone"])


def test_apply_filters(spark):
    df = spark.createDataFrame([("1", "EN"), ("2", "DE"), ("3", "FR")], "id string, lang string")
    assert cdc.apply_filters(df, [{"column": "lang", "operator": "=", "value": "EN"}]).count() == 1
    assert cdc.apply_filters(df, [{"column": "lang", "operator": "IN", "value": ["EN", "DE"]}]).count() == 2
    assert cdc.apply_filters(df, [{"column": "lang", "operator": "NOT IN", "value": ["EN"]}]).count() == 2
    with pytest.raises(ValueError):
        cdc.apply_filters(df, [{"column": "lang", "operator": "LIKE", "value": "E%"}])


def test_surrogate_key_is_deterministic_and_null_safe(spark):
    from framework.keys import surrogate_key

    df = spark.createDataFrame([("A", None), (None, "A"), ("A", None)], "x string, y string")
    keys = [r.k for r in df.select(surrogate_key("x", "y").alias("k")).collect()]
    assert keys[0] == keys[2] and keys[0] != keys[1]


def test_reconciliation_compare_finds_all_three_failure_types(spark):
    from framework.reconciliation import compare

    raw = spark.createDataFrame([("o1", 2, Decimal("10.00")), ("o2", 1, Decimal("5.00")), ("o3", 1, None)],
                                "order_id string, lines int, amount decimal(18,2)")
    trusted = spark.createDataFrame([("o1", 2, Decimal("10.00")), ("o3", 1, Decimal("1.00")), ("o4", 1, None)],
                                    "order_id string, lines int, amount decimal(18,2)")
    rule = Rule(7, "fact", ("order_id",), "", "")
    found = {(r.failure_type, r.key_values, r.column_name) for r in compare(raw, trusted, rule).collect()}
    assert found == {
        ("MISSING_IN_TRUSTED", "o2", None),
        ("MISSING_IN_RAW", "o4", None),
        ("VALUE_MISMATCH", "o3", "amount"),  # NULL vs 1.00 is a mismatch (NULL-safe compare)
    }


def test_reconciliation_compare_passes_when_equal(spark):
    from framework.reconciliation import compare

    df = spark.createDataFrame([("o1", None)], "order_id string, note string")
    assert compare(df, df, Rule(1, "t", ("order_id",), "", "")).count() == 0
