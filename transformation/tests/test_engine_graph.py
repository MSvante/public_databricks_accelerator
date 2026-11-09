"""Build the whole pipeline graph against a recording fake of the ``dp`` API.

No Spark needed: registration only stores the (lazy) table functions.
"""

from collections import Counter

import pytest

from framework.engine import build_pipeline
from framework.naming import Naming


class FakeDp:
    def __init__(self):
        self.tables, self.streaming_tables, self.views, self.flows = {}, {}, {}, []

    def table(self, name, **kwargs):
        def register(fn):
            assert name not in self.tables, f"table {name} registered twice"
            self.tables[name] = fn
            return fn
        return register

    def temporary_view(self, name, **kwargs):
        def register(fn):
            assert name not in self.views, f"view {name} registered twice"
            self.views[name] = fn
            return fn
        return register

    def create_streaming_table(self, name, **kwargs):
        assert name not in self.streaming_tables, f"streaming table {name} created twice"
        self.streaming_tables[name] = kwargs

    def create_auto_cdc_flow(self, **kwargs):
        self.flows.append(kwargs)


def build(cfg, env):
    dp = FakeDp()
    naming = Naming(env=env, catalog="cat", sandbox_schema="sandbox_me")
    build_pipeline(dp, spark=object(), config=cfg, naming=naming)
    return dp


def test_dev_raw_layer(cfg):
    dp = build(cfg, "dev")
    # every source: a log table and an SCD1 table; PII sources twice (vault + public)
    assert "cat.raw_sales.raw_log_sales_order_lines" in dp.tables
    assert "cat.raw_sales.sales_order_lines" in dp.streaming_tables
    assert "cat.pii_raw_master_data.raw_log_customers" in dp.tables
    assert "cat.raw_master_data.raw_log_customers" in dp.tables
    assert "cat.pii_raw_master_data.customers" in dp.streaming_tables
    assert "cat.raw_master_data.customers" in dp.streaming_tables
    for name in [n for n in dp.streaming_tables if ".raw_" in n or ".pii_raw_" in n]:
        assert dp.streaming_tables[name]["table_properties"]["delta.enableChangeDataFeed"] == "true"


def test_public_customers_log_reads_the_vault_not_landing(cfg):
    dp = build(cfg, "dev")
    flows = {f["target"]: f for f in dp.flows}
    assert flows["cat.raw_master_data.customers"]["source"] == "cat.raw_master_data.raw_log_customers"
    assert flows["cat.pii_raw_master_data.customers"]["source"] == "cat.pii_raw_master_data.raw_log_customers"


def test_raw_cdc_settings(cfg):
    dp = build(cfg, "dev")
    flow = next(f for f in dp.flows if f["target"] == "cat.raw_sales.sales_order_lines")
    assert flow["keys"] == ["order_id", "line_no"]
    assert flow["sequence_by"] == "cdc_timestamp"
    assert flow["apply_as_deletes"] == "trim(_op) IN ('D')"
    assert flow["stored_as_scd_type"] == 1


def test_one_flow_per_driver_into_one_target(cfg):
    dp = build(cfg, "dev")
    target = "cat.trusted_master_data.dim_product"
    flows = [f for f in dp.flows if f["target"] == target]
    assert sorted(f["source"] for f in flows) == ["dim_product__product_texts", "dim_product__products"]
    assert all(f["name"] == f["source"] for f in flows)


def test_flow_names_are_unique(cfg):
    for env in ("dev", "sandbox"):
        dp = build(cfg, env)
        names = [f["name"] for f in dp.flows if f.get("name")]
        dupes = [n for n, c in Counter(names).items() if c > 1]
        assert not dupes, dupes


def test_pii_product_gets_vaulted_and_public_variant(cfg):
    dp = build(cfg, "dev")
    assert "cat.pii_trusted_master_data.dim_customer" in dp.streaming_tables
    assert "cat.trusted_master_data.dim_customer" in dp.streaming_tables
    assert "dim_customer_vaulted__customers" in dp.views
    assert "dim_customer__customers" in dp.views
    # non-PII products get no vault
    assert not any(t.endswith("pii_trusted_sales.fact_sales_order_line") for t in dp.streaming_tables)


def test_input_schemas_route_pii_sources_to_vault(cfg):
    import data_products.dim_customer as m
    from framework.engine import PipelineBuilder

    b = PipelineBuilder(FakeDp(), object(), cfg, Naming(env="dev", catalog="cat"))
    assert b.input_schemas(m.DATA_PRODUCT, vaulted=True) == {"customers": "cat.pii_raw_master_data"}
    assert b.input_schemas(m.DATA_PRODUCT, vaulted=False) == {"customers": "cat.raw_master_data"}


def test_hard_delete_setting_is_respected(cfg):
    dp = build(cfg, "dev")
    by_target = {f["target"]: f for f in dp.flows}
    assert by_target["cat.trusted_sales.fact_sales_order_line"]["apply_as_deletes"] == "is_deleted = true"
    assert by_target["cat.trusted_master_data.dim_product"]["apply_as_deletes"] is None


def test_sandbox_has_no_vault_and_one_schema(cfg):
    dp = build(cfg, "sandbox")
    everything = [*dp.tables, *dp.streaming_tables]
    assert all(n.startswith("cat.sandbox_me.") for n in everything)
    assert not any("pii_" in n for n in everything)


def test_unknown_source_in_product_fails(cfg):
    from types import SimpleNamespace

    from framework.engine import PipelineBuilder

    bad = SimpleNamespace(DATA_PRODUCT={"drivers": [{"name": "nope"}]}, transform=None)
    b = PipelineBuilder(FakeDp(), object(), cfg, Naming(env="dev", catalog="cat"))
    with pytest.raises(ValueError, match="unknown source table 'nope'"):
        b.input_schemas(bad.DATA_PRODUCT, vaulted=False)
