import pytest

from framework.naming import Naming


def test_shared_target_splits_by_layer_domain_and_pii():
    n = Naming(env="dev", catalog="cat")
    assert n.schema("raw", "Sales") == "raw_sales"
    assert n.schema("trusted", "sales") == "trusted_sales"
    assert n.schema("raw", "sales", vaulted=True) == "pii_raw_sales"
    assert n.schema("trusted", "sales", vaulted=True) == "pii_trusted_sales"
    assert n.fqn("raw", "sales", "orders") == "cat.raw_sales.orders"
    assert n.builds_vault


def test_sandbox_uses_one_schema_and_no_vault():
    n = Naming(env="sandbox", catalog="cat", sandbox_schema="sandbox_me")
    assert n.schema("raw", "sales") == "sandbox_me"
    assert n.schema("trusted", "sales", vaulted=True) == "sandbox_me"
    assert not n.builds_vault
    assert n.all_schemas({"a", "b"}, {"a"}) == ["sandbox_me"]


def test_sandbox_requires_schema():
    with pytest.raises(ValueError):
        Naming(env="sandbox", catalog="cat").schema("raw", "x")


def test_all_schemas_only_vaults_pii_domains():
    n = Naming(env="prod", catalog="cat")
    assert n.all_schemas({"sales", "master_data"}, {"master_data"}) == [
        "pii_raw_master_data", "pii_trusted_master_data",
        "raw_master_data", "raw_sales", "trusted_master_data", "trusted_sales",
    ]
