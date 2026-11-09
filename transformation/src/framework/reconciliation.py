"""Raw-vs-trusted reconciliation: data quality that checks the RESULT.

Inline pipeline expectations check rows as they flow past: is this value null,
is that amount negative. They can't see a row that never arrived, a join that
silently dropped half the lines, or a total that drifted. This compares what
the trusted layer contains with what the raw layer says it should contain.

Each rule is a pair of SQL queries that return the same columns. The two
results are compared on the key columns, and three kinds of failure are
reported:

* ``MISSING_IN_TRUSTED``  key in raw, not in trusted
* ``MISSING_IN_RAW``      key in trusted, not in raw
* ``VALUE_MISMATCH``      key in both, a non-key column differs (NULL-safe)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_SCHEMA_PLACEHOLDER = re.compile(r"\{((?:raw|trusted)_[a-z0-9_]+)\}")

RESULT_COLUMNS = (
    "rule_id", "table_name", "status", "failure_type", "key_values",
    "column_name", "raw_value", "trusted_value", "message",
)


@dataclass(frozen=True)
class Rule:
    rule_id: int
    table_name: str
    key_columns: tuple[str, ...]
    raw_query: str
    trusted_query: str
    is_active: bool = True


def parse_rules(doc: dict[str, Any]) -> list[Rule]:
    rules = []
    seen = set()
    for r in doc.get("rules", []) or []:
        keys = r["key_columns"]
        keys = [k.strip() for k in keys.split(",")] if isinstance(keys, str) else list(keys)
        if r["rule_id"] in seen:
            raise ValueError(f"Duplicate rule_id {r['rule_id']}")
        seen.add(r["rule_id"])
        rules.append(Rule(int(r["rule_id"]), r["table_name"], tuple(keys),
                          r["raw_query"], r["trusted_query"], bool(r.get("is_active", True))))
    return rules


def render_query(query: str, schema_for) -> str:
    """Replace ``{raw_<domain>}`` / ``{trusted_<domain>}`` with ``schema_for(layer, domain)``."""

    def repl(m: re.Match[str]) -> str:
        layer, domain = m.group(1).split("_", 1)
        return schema_for(layer, domain)

    return _SCHEMA_PLACEHOLDER.sub(repl, query)


def compare(raw_df, trusted_df, rule: Rule):
    """Return a DataFrame of findings (RESULT_COLUMNS). Empty means it passed."""
    from functools import reduce

    from pyspark.sql import functions as F

    keys = list(rule.key_columns)
    for side, df in (("raw", raw_df), ("trusted", trusted_df)):
        missing = [k for k in keys if k not in df.columns]
        if missing:
            raise ValueError(f"rule {rule.rule_id}: {side} query lacks key columns {missing}")
    value_cols = sorted((set(raw_df.columns) & set(trusted_df.columns)) - set(keys))

    r = raw_df.select(*keys, *[F.col(c).alias(f"r__{c}") for c in value_cols]).withColumn("r__present", F.lit(True))
    t = trusted_df.select(*keys, *[F.col(c).alias(f"t__{c}") for c in value_cols]).withColumn("t__present", F.lit(True))
    joined = r.join(t, on=keys, how="full_outer")

    key_values = F.concat_ws(",", *[F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in keys])

    def finding(failure_type, column_name, raw_value, trusted_value, message):
        return [
            F.lit(rule.rule_id).alias("rule_id"),
            F.lit(rule.table_name).alias("table_name"),
            F.lit("FAILED").alias("status"),
            F.lit(failure_type).alias("failure_type"),
            key_values.alias("key_values"),
            F.lit(column_name).cast("string").alias("column_name"),
            raw_value.cast("string").alias("raw_value"),
            trusted_value.cast("string").alias("trusted_value"),
            F.lit(message).alias("message"),
        ]

    null = F.lit(None)
    parts = [
        joined.where(F.col("t__present").isNull()).select(
            *finding("MISSING_IN_TRUSTED", None, null, null, "Key present in raw but missing in trusted")),
        joined.where(F.col("r__present").isNull()).select(
            *finding("MISSING_IN_RAW", None, null, null, "Key present in trusted but missing in raw")),
    ]
    both = joined.where(F.col("r__present") & F.col("t__present"))
    for c in value_cols:
        parts.append(
            both.where(~F.col(f"r__{c}").eqNullSafe(F.col(f"t__{c}"))).select(
                *finding("VALUE_MISMATCH", c, F.col(f"r__{c}"), F.col(f"t__{c}"), f"Mismatch in column '{c}'"))
        )
    return reduce(lambda a, b: a.unionByName(b), parts)
