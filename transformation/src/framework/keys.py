"""Deterministic surrogate keys for trusted tables."""

from __future__ import annotations

_NULL_TOKEN = "<null>"
_SEPARATOR = "␟"  # unit separator symbol; will not appear in business keys


def surrogate_key(*cols):
    """BIGINT key derived from one or more business-key columns.

    Same input -> same key, on every run and in every environment, so facts
    and dimensions can be rebuilt independently and still join. NULLs are
    replaced with a token so (NULL, 'A') and ('A', NULL) don't collide.
    """
    from pyspark.sql import Column
    from pyspark.sql import functions as F

    parts = [
        F.coalesce((c if isinstance(c, Column) else F.col(c)).cast("string"), F.lit(_NULL_TOKEN))
        for c in cols
    ]
    return F.xxhash64(F.concat_ws(_SEPARATOR, *parts))
