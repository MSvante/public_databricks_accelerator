"""Load and validate the YAML that drives the whole pipeline.

Everything the engine needs is derived from two files:

* ``conf/sources.yml``       -> raw layer
* ``conf/data_products.yml`` -> trusted layer

This module is pure Python so it can be unit tested without Spark.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_PLACEHOLDER = re.compile(r"\$\{([a-z_]+)\}")
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(ValueError):
    """Raised when the YAML configuration is inconsistent."""


@dataclass(frozen=True)
class SourceTable:
    name: str
    domain: str
    landing_path: str
    keys: tuple[str, ...]
    format: str = "parquet"
    timestamp_column: str = "_extracted_at"
    sequence_column: str = "cdc_timestamp"
    operation_column: str = "_op"
    delete_values: tuple[str, ...] = ("D",)
    pii_columns: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()

    @property
    def raw_log_name(self) -> str:
        return f"raw_log_{self.name}"

    @property
    def has_pii(self) -> bool:
        return bool(self.pii_columns)

    @property
    def delete_condition(self) -> str:
        """SQL predicate that marks a landing row as a delete."""
        values = ", ".join(f"'{v}'" for v in self.delete_values)
        return f"trim({self.operation_column}) IN ({values})"


@dataclass(frozen=True)
class DataProduct:
    name: str
    domain: str
    module: str
    keys: tuple[str, ...]
    allow_hard_deletes: bool = False
    pii_columns: tuple[str, ...] = ()

    @property
    def has_pii(self) -> bool:
        return bool(self.pii_columns)


@dataclass(frozen=True)
class FrameworkConfig:
    sources: tuple[SourceTable, ...]
    data_products: tuple[DataProduct, ...]
    # Registries the engine and the data-product modules rely on.
    domain_registry: dict[str, str] = field(default_factory=dict)
    key_registry: dict[str, list[str]] = field(default_factory=dict)
    pii_sources: frozenset[str] = frozenset()

    def source(self, name: str) -> SourceTable:
        for s in self.sources:
            if s.name == name:
                return s
        raise ConfigError(f"Unknown source table '{name}'")


def substitute(text: str, values: dict[str, str]) -> str:
    """Replace ``${name}`` placeholders. Unknown placeholders are an error,
    so a typo can't silently produce a path like ``${landng_root}/...``."""

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ConfigError(f"No value for placeholder ${{{key}}}")
        return values[key].rstrip("/")

    return _PLACEHOLDER.sub(repl, text)


def _tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def parse_sources(doc: dict[str, Any]) -> tuple[SourceTable, ...]:
    defaults = doc.get("defaults", {}) or {}
    out = []
    for raw in doc.get("sources", []) or []:
        merged = {**defaults, **raw}
        missing = [k for k in ("name", "domain", "landing_path", "keys") if not merged.get(k)]
        if missing:
            raise ConfigError(f"Source {raw.get('name', '?')} is missing {missing}")
        out.append(
            SourceTable(
                name=merged["name"],
                domain=merged["domain"],
                landing_path=merged["landing_path"],
                keys=_tuple(merged["keys"]),
                format=merged.get("format", "parquet"),
                timestamp_column=merged.get("timestamp_column", "_extracted_at"),
                sequence_column=merged.get("sequence_column", "cdc_timestamp"),
                operation_column=merged.get("operation_column", "_op"),
                delete_values=_tuple(merged.get("delete_values", ["D"])),
                pii_columns=_tuple(merged.get("pii_columns")),
                cluster_by=_tuple(merged.get("cluster_by")),
            )
        )
    return tuple(out)


def parse_data_products(doc: dict[str, Any]) -> tuple[DataProduct, ...]:
    out = []
    for raw in doc.get("data_products", []) or []:
        missing = [k for k in ("name", "domain", "module", "keys") if not raw.get(k)]
        if missing:
            raise ConfigError(f"Data product {raw.get('name', '?')} is missing {missing}")
        out.append(
            DataProduct(
                name=raw["name"],
                domain=raw["domain"],
                module=raw["module"],
                keys=_tuple(raw["keys"]),
                allow_hard_deletes=bool(raw.get("allow_hard_deletes", False)),
                pii_columns=_tuple(raw.get("pii_columns")),
            )
        )
    return tuple(out)


def validate(sources: tuple[SourceTable, ...], products: tuple[DataProduct, ...]) -> None:
    names = [s.name for s in sources] + [p.name for p in products]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ConfigError(f"Duplicate table names: {dupes}")
    for obj in (*sources, *products):
        for value in (obj.name, obj.domain):
            if not _IDENTIFIER.match(value):
                raise ConfigError(f"'{value}' is not a valid lower-case identifier")
    for s in sources:
        if s.sequence_column in s.pii_columns or set(s.keys) & set(s.pii_columns):
            # Hashing a key would break the SCD1 merge in the public stream.
            raise ConfigError(f"{s.name}: keys and the sequence column cannot be PII")


def build(sources_doc: dict[str, Any], products_doc: dict[str, Any]) -> FrameworkConfig:
    sources = parse_sources(sources_doc)
    products = parse_data_products(products_doc)
    validate(sources, products)
    return FrameworkConfig(
        sources=sources,
        data_products=products,
        domain_registry={s.name: s.domain for s in sources},
        key_registry={s.name: list(s.keys) for s in sources},
        pii_sources=frozenset(s.name for s in sources if s.has_pii),
    )


def load(conf_dir: str | Path, values: dict[str, str]) -> FrameworkConfig:
    """Read both YAML files from ``conf_dir`` and build the config."""
    conf_dir = Path(conf_dir)
    sources_doc = yaml.safe_load(substitute((conf_dir / "sources.yml").read_text(), values))
    products_doc = yaml.safe_load((conf_dir / "data_products.yml").read_text())
    return build(sources_doc, products_doc)
