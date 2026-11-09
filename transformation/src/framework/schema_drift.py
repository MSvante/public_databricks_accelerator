"""Pre-flight check for column type changes in the landing zone.

New columns are fine: Auto Loader's ``addNewColumns`` evolves the schema. A
column that changes TYPE (int -> bigint, string -> decimal) is not: the SCD1
merge into the existing raw table fails, and the whole pipeline update fails
with it.

The schema guard job (src/jobs/schema_guard.py) runs before the pipeline:

1. read the landing schema from ONE file's footer (cheap, independent of volume)
2. compare it to the existing raw table
3. if types changed, run a pipeline update with ``full_refresh_selection`` for
   only the affected tables, plus the trusted tables those tables drive

Everything here is pure Python on ``pyspark.sql.types`` and needs no JVM.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from framework import cdc
from framework.config import FrameworkConfig
from framework.naming import Naming

# Columns the pipeline adds or the landing format carries for bookkeeping.
DEFAULT_IGNORED = frozenset(
    {"source_file_path", "source_file_modified_at", "_rescued_data", "_metadata"}
)


@dataclass(frozen=True)
class TypeChange:
    column: str
    old_type: str
    new_type: str


def normalize_type(data_type) -> str:
    """Comparable string for a Spark type, e.g. ``decimal(18,2)``, ``bigint``."""
    return data_type.simpleString().lower()


def find_type_changes(landing_schema, table_schema, ignore: Iterable[str] = ()) -> list[TypeChange]:
    """Columns present in both schemas whose type differs.

    Matching is case-insensitive. New and dropped columns are ignored on
    purpose: they don't break the merge.
    """
    ignored = {c.lower() for c in (*DEFAULT_IGNORED, *ignore)}
    existing = {f.name.lower(): f for f in table_schema.fields}
    changes = []
    for field in landing_schema.fields:
        key = field.name.lower()
        if key in ignored or key not in existing:
            continue
        old, new = normalize_type(existing[key].dataType), normalize_type(field.dataType)
        if old != new:
            changes.append(TypeChange(field.name, old, new))
    return changes


def latest_data_file(list_dir: Callable[[str], list[tuple[str, bool, int]]], root: str,
                     suffix: str = ".parquet") -> str | None:
    """A data file from the NEWEST part of the landing zone below ``root``.

    A type change only exists in files written after it happened, so checking an
    arbitrary (old) file can miss it. At each level this descends into the most
    recent entry (modification time, then name; landing folders are usually
    named by date or timestamp) and backtracks only if a branch holds no data
    file. Cost grows with the depth of the tree, not the number of files.

    ``list_dir(path)`` returns ``[(path, is_dir, mtime_ms), ...]``; injected so
    the job can use dbutils and tests can use a dict.
    """

    def visible(entry):
        name = entry[0].rstrip("/").rsplit("/", 1)[-1]
        return not name.startswith(("_", "."))  # _delta_log, _SUCCESS, hidden

    def newest_first(entries):
        return sorted(entries, key=lambda e: (e[2] or 0, e[0]), reverse=True)

    def search(path):
        for p, is_dir, _ in newest_first(e for e in list_dir(path) if visible(e)):
            if is_dir:
                found = search(p)
                if found:
                    return found
            elif p.endswith(suffix):
                return p
        return None

    return search(root)


def refresh_selection(changed_sources: Iterable[str], config: FrameworkConfig, naming: Naming,
                      product_drivers: dict[str, list[str]], include_dependents: bool = True) -> list[str]:
    """Fully qualified tables to pass as ``full_refresh_selection``.

    For each changed source: its SCD1 table in every variant (public, and vault
    if it has PII). The append-only ``raw_log_*`` is deliberately left alone:
    refreshing it re-reads the whole landing zone, and history is lost if the
    landing zone has retention.

    With ``include_dependents``: every trusted product that uses a changed
    source as a DRIVER, in every variant. A full refresh rewrites the raw table,
    and a CDF stream reading it can't continue across that rewrite. Satellites
    are read as static snapshots, so they don't need it.
    """
    changed = set(changed_sources)
    out: list[str] = []
    for name in sorted(changed):
        src = config.source(name)
        out.append(naming.fqn("raw", src.domain, src.name))
        if src.has_pii and naming.builds_vault:
            out.append(naming.fqn("raw", src.domain, src.name, vaulted=True))
    if include_dependents:
        for product in config.data_products:
            if changed & set(product_drivers.get(product.name, [])):
                out.append(naming.fqn("trusted", product.domain, product.name))
                if product.has_pii and naming.builds_vault:
                    out.append(naming.fqn("trusted", product.domain, product.name, vaulted=True))
    return list(dict.fromkeys(out))


def product_drivers(config: FrameworkConfig, import_module) -> dict[str, list[str]]:
    """Map product name -> its driver tables, read from the product modules."""
    return {
        p.name: cdc.driver_names(import_module(p.module).DATA_PRODUCT)
        for p in config.data_products
    }
