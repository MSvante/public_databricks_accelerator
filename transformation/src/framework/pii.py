"""PII masking with native Spark functions (no Python UDFs).

Masking happens exactly once: at the raw boundary, vault -> public. Every
public table downstream reads the already-hashed values, so the same email
hashes to the same value in every table and hashed columns stay joinable.
"""

from __future__ import annotations

from collections.abc import Iterable


class PiiColumnMissing(ValueError):
    """A configured PII column is not in the DataFrame.

    Failing hard is deliberate. Silently skipping a missing column (renamed
    upstream, typo in YAML) would let clear-text PII into a public schema.
    """


def mask_columns(df, pii_columns: Iterable[str]):
    from pyspark.sql import functions as F

    pii_columns = list(pii_columns)
    if not pii_columns:
        return df
    missing = sorted(set(pii_columns) - set(df.columns))
    if missing:
        raise PiiColumnMissing(
            f"PII columns {missing} are configured but not present. "
            f"Available: {sorted(df.columns)}"
        )
    return df.withColumns(
        {c: F.sha2(F.col(c).cast("string"), 256) for c in pii_columns}
    )
