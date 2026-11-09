import pytest

from framework import config
from framework.config import ConfigError


def test_real_config_loads_and_builds_registries(cfg):
    assert {s.name for s in cfg.sources} == {
        "products", "product_texts", "product_categories", "customers", "sales_orders", "sales_order_lines",
    }
    assert cfg.domain_registry["sales_order_lines"] == "sales"
    assert cfg.key_registry["sales_order_lines"] == ["order_id", "line_no"]
    assert cfg.pii_sources == {"customers"}
    assert cfg.source("products").landing_path == "/Volumes/demo/landing/files/master_data/products/"


def test_defaults_are_applied_and_overridable():
    sources = config.parse_sources({
        "defaults": {"operation_column": "op", "delete_values": ["D", "X"]},
        "sources": [
            {"name": "a", "domain": "d", "landing_path": "/p", "keys": ["id"]},
            {"name": "b", "domain": "d", "landing_path": "/p", "keys": "id", "operation_column": "chg"},
        ],
    })
    assert sources[0].operation_column == "op"
    assert sources[0].delete_condition == "trim(op) IN ('D', 'X')"
    assert sources[1].operation_column == "chg"
    assert sources[1].keys == ("id",)


def test_unknown_placeholder_is_an_error():
    with pytest.raises(ConfigError, match="landng_root"):
        config.substitute("path: ${landng_root}/x", {"landing_root": "/v"})


def test_placeholder_strips_trailing_slash():
    assert config.substitute("${landing_root}/a", {"landing_root": "/v/"}) == "/v/a"


@pytest.mark.parametrize("missing", ["name", "domain", "landing_path", "keys"])
def test_source_missing_required_field(missing):
    raw = {"name": "a", "domain": "d", "landing_path": "/p", "keys": ["id"]}
    raw.pop(missing)
    with pytest.raises(ConfigError):
        config.parse_sources({"sources": [raw]})


def test_duplicate_names_rejected():
    with pytest.raises(ConfigError, match="Duplicate"):
        config.build(
            {"sources": [{"name": "a", "domain": "d", "landing_path": "/p", "keys": ["id"]}]},
            {"data_products": [{"name": "a", "domain": "d", "module": "m", "keys": ["k"]}]},
        )


def test_pii_key_rejected():
    # A hashed key would break the SCD1 merge of the public stream.
    with pytest.raises(ConfigError, match="cannot be PII"):
        config.build(
            {"sources": [{"name": "a", "domain": "d", "landing_path": "/p", "keys": ["email"],
                          "pii_columns": ["email"]}]},
            {"data_products": []},
        )


def test_identifiers_must_be_lower_case():
    with pytest.raises(ConfigError, match="identifier"):
        config.build({"sources": [{"name": "Orders", "domain": "d", "landing_path": "/p", "keys": ["id"]}]},
                     {"data_products": []})
