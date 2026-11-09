import importlib

from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType, StructField, StructType

from framework.naming import Naming
from framework.schema_drift import (
    find_type_changes,
    latest_data_file,
    product_drivers,
    refresh_selection,
)


def schema(**cols):
    return StructType([StructField(k, v) for k, v in cols.items()])


def test_type_change_detected():
    changes = find_type_changes(
        schema(order_id=StringType(), quantity=DecimalType(18, 3)),
        schema(order_id=StringType(), quantity=IntegerType()),
    )
    assert [(c.column, c.old_type, c.new_type) for c in changes] == [("quantity", "int", "decimal(18,3)")]


def test_new_and_dropped_columns_are_not_type_changes():
    assert find_type_changes(schema(a=StringType(), new_col=LongType()), schema(a=StringType(), gone=LongType())) == []


def test_matching_is_case_insensitive_and_ignores_bookkeeping():
    assert find_type_changes(schema(Amount=LongType()), schema(amount=LongType())) == []
    assert find_type_changes(schema(_rescued_data=LongType()), schema(_rescued_data=StringType())) == []
    assert find_type_changes(schema(_op=LongType()), schema(_op=StringType()), ignore=["_op"]) == []


def tree_lister(tree):
    def list_dir(path):
        return tree.get(path, [])
    return list_dir


def test_latest_file_follows_newest_folder():
    tree = {
        "/l/": [("/l/20260101_initial/", True, 0), ("/l/20261101_drift/", True, 0), ("/l/_checkpoint/", True, 0)],
        "/l/20261101_drift/": [
            ("/l/20261101_drift/_SUCCESS", False, 9),
            ("/l/20261101_drift/part-0.parquet", False, 5),
        ],
        "/l/20260101_initial/": [("/l/20260101_initial/part-0.parquet", False, 1)],
    }
    assert latest_data_file(tree_lister(tree), "/l/") == "/l/20261101_drift/part-0.parquet"


def test_latest_file_uses_mtime_before_name_and_backtracks():
    tree = {
        "/l/": [("/l/b/", True, 100), ("/l/a/", True, 200)],
        "/l/a/": [],  # newest but empty -> backtrack
        "/l/b/": [("/l/b/x.parquet", False, 1)],
    }
    assert latest_data_file(tree_lister(tree), "/l/") == "/l/b/x.parquet"


def test_no_data_file():
    assert latest_data_file(tree_lister({"/l/": [("/l/_SUCCESS", False, 1)]}), "/l/") is None


def drivers(cfg):
    return product_drivers(cfg, importlib.import_module)


def test_refresh_selection_includes_vault_and_driven_products(cfg):
    n = Naming(env="dev", catalog="cat")
    sel = refresh_selection(["customers", "sales_order_lines"], cfg, n, drivers(cfg))
    assert sel == [
        "cat.raw_master_data.customers",
        "cat.pii_raw_master_data.customers",
        "cat.raw_sales.sales_order_lines",
        "cat.trusted_master_data.dim_customer",
        "cat.pii_trusted_master_data.dim_customer",
        "cat.trusted_sales.fact_sales_order_line",
    ]


def test_satellite_change_refreshes_no_products(cfg):
    # sales_orders is only a satellite of the fact: static snapshot, no stream to reset
    sel = refresh_selection(["sales_orders"], cfg, Naming(env="dev", catalog="cat"), drivers(cfg))
    assert sel == ["cat.raw_sales.sales_orders"]


def test_refresh_selection_without_dependents_and_in_sandbox(cfg):
    n = Naming(env="sandbox", catalog="cat", sandbox_schema="sb")
    assert refresh_selection(["customers"], cfg, n, drivers(cfg), include_dependents=False) == ["cat.sb.customers"]
