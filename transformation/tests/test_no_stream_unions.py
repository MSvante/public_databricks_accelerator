"""Guard rails for the data-product contract.

The most important one: streams are never unioned. A union of streaming sources
shares ONE checkpoint, so adding or removing a driver later breaks recovery.
The engine runs one AUTO CDC flow per driver instead.
"""

import ast
import importlib
import inspect
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
STREAMING_CODE = [SRC / "framework" / "cdc.py", SRC / "framework" / "engine.py",
                  *sorted((SRC / "data_products").glob("*.py"))]
PRODUCT_MODULES = sorted(p.stem for p in (SRC / "data_products").glob("*.py") if p.stem != "__init__")


@pytest.mark.parametrize("path", STREAMING_CODE, ids=lambda p: p.name)
def test_streaming_code_never_unions(path):
    tree = ast.parse(path.read_text())
    unions = [n.lineno for n in ast.walk(tree)
              if isinstance(n, ast.Attribute) and n.attr in ("union", "unionAll", "unionByName")]
    assert not unions, f"{path.name} unions at lines {unions}; each driver must be its own flow"


def test_every_module_is_configured_and_vice_versa(cfg):
    configured = {p.module.split(".")[-1] for p in cfg.data_products}
    assert configured == set(PRODUCT_MODULES)


@pytest.mark.parametrize("name", PRODUCT_MODULES)
def test_module_contract(name, cfg):
    module = importlib.import_module(f"data_products.{name}")
    product = module.DATA_PRODUCT
    assert product.get("drivers"), "at least one driver"
    assert list(inspect.signature(module.transform).parameters) == ["spark", "schemas", "key_registry"]
    for table in [d["name"] for d in product["drivers"]] + product.get("satellites", []):
        assert table in cfg.domain_registry, f"{name} reads unknown source '{table}'"
    for d in product["drivers"]:
        assert set(d) <= {"name", "alias", "filters"}, f"unknown driver keys {set(d)}"


def test_pii_products_read_a_pii_source(cfg):
    """A product marked PII must read at least one vaulted source, otherwise the
    'vaulted' variant would hold nothing the public one doesn't."""
    for p in cfg.data_products:
        if not p.has_pii:
            continue
        product = importlib.import_module(p.module).DATA_PRODUCT
        inputs = {d["name"] for d in product["drivers"]} | set(product.get("satellites", []))
        assert inputs & cfg.pii_sources, f"{p.name} has pii_columns but reads no PII source"
