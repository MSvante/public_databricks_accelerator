"""Driver/satellite change propagation for trusted tables.

A data product names its inputs in two roles:

* **drivers**    tables whose changes must update the product. Each driver is
                 read as a Change Data Feed *stream* of keys.
* **satellites** lookup tables joined as *static snapshots*. A change in a
                 satellite alone does NOT trigger a recompute; the new value is
                 picked up the next time a driver touches that key.

The change stream only says "this key changed". The values always come from the
current snapshots, so replaying an old event produces the current truth.

One flow per driver
-------------------
The engine registers one AUTO CDC flow per driver into the same target table and
calls ``transform`` once per flow inside ``driver_scope(driver)``. This module
reads only the driver in scope. It never unions streams: a union of streaming
sources shares a single checkpoint, and adding or removing a driver later then
breaks checkpoint recovery. ``tests/test_no_stream_unions.py`` enforces this.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_active_driver: ContextVar[str | None] = ContextVar("active_driver", default=None)

# CDF emits both images of an update. The pre-image carries the same key, so it
# would only produce a duplicate event.
_IGNORED_CHANGE_TYPES = ("update_preimage",)

_OPERATORS = {
    "=": lambda c, v: c == v,
    "!=": lambda c, v: c != v,
    "IN": lambda c, v: c.isin(list(v) if isinstance(v, (list, tuple)) else [v]),
    "NOT IN": lambda c, v: ~c.isin(list(v) if isinstance(v, (list, tuple)) else [v]),
}


@contextmanager
def driver_scope(driver: str):
    """Select which driver ``read_changes_and_snapshots`` streams."""
    token = _active_driver.set(driver)
    try:
        yield
    finally:
        _active_driver.reset(token)


def driver_names(product: dict[str, Any]) -> list[str]:
    return [d["name"] for d in product.get("drivers", [])]


def satellite_names(product: dict[str, Any]) -> list[str]:
    return list(product.get("satellites", []))


def input_tables(product: dict[str, Any]) -> list[str]:
    """Every raw table a product reads, drivers first, without duplicates."""
    return list(dict.fromkeys(driver_names(product) + satellite_names(product)))


def resolve_keys(product: dict[str, Any], key_registry: dict[str, list[str]]) -> list[str]:
    """Join keys: explicit ``keys`` in the product, else the first driver's keys."""
    keys = product.get("keys") or key_registry.get(driver_names(product)[0] if product.get("drivers") else "")
    if not keys:
        raise ValueError("Cannot determine join keys; set 'keys' in DATA_PRODUCT")
    return list(keys)


def apply_filters(df, filters: list[dict[str, Any]] | None):
    from pyspark.sql import functions as F

    for f in filters or []:
        op = str(f.get("operator", "=")).upper()
        if op not in _OPERATORS:
            raise ValueError(f"Unsupported operator '{op}'. Use one of {sorted(_OPERATORS)}")
        df = df.filter(_OPERATORS[op](F.col(f["column"]), f["value"]))
    return df


def selected_driver(product: dict[str, Any]) -> dict[str, Any]:
    drivers = product.get("drivers") or []
    if not drivers:
        raise ValueError("DATA_PRODUCT declares no drivers")
    active = _active_driver.get()
    if active is None:
        if len(drivers) > 1:
            raise ValueError(
                f"{len(drivers)} drivers {driver_names(product)} but no driver_scope(). "
                "Each driver runs as its own flow; streams are never unioned."
            )
        return drivers[0]
    for d in drivers:
        if d["name"] == active:
            return d
    raise ValueError(f"Driver '{active}' is not declared in DATA_PRODUCT")


def read_changes_and_snapshots(spark, schemas: dict[str, str], product: dict[str, Any], key_registry):
    """Return ``(changes, snapshots)``.

    ``changes``   streaming DataFrame with the product's join keys (aliased via the
                  driver's ``alias`` map), ``_change_type`` and ``_event_time``.
    ``snapshots`` dict of table name -> static DataFrame for every input table.
    """
    from pyspark.sql import functions as F

    keys = resolve_keys(product, key_registry)
    driver = selected_driver(product)
    name = driver["name"]
    # alias maps SOURCE column -> join key, e.g. {"ref_product": "product_id"}
    source_for_key = {tgt: src for src, tgt in (driver.get("alias") or {}).items()}

    changes = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{schemas[name]}.{name}")
        .where(~F.col("_change_type").isin(*_IGNORED_CHANGE_TYPES))
    )
    changes = apply_filters(changes, driver.get("filters"))
    changes = changes.select(
        *[F.col(source_for_key.get(k, k)).alias(k) for k in keys],
        F.col("_change_type"),
        F.col("_commit_timestamp").alias("_event_time"),
    )

    filters_by_table = {d["name"]: d.get("filters") for d in product["drivers"]}
    snapshots = {
        t: apply_filters(spark.read.table(f"{schemas[t]}.{t}"), filters_by_table.get(t))
        for t in input_tables(product)
    }
    return changes, snapshots
