# Claude Instructions

Config-driven Lakeflow Declarative Pipelines framework on Databricks, and the
companion code for the talk "Declarative Pipelines, Imperative Problems".
Public repository: keep everything generic. No client names, real table names,
storage accounts, workspace URLs or personal data.

## Layout

- `transformation/` is the Asset Bundle. Run every command from there.
- `transformation/src/pipeline.py`: the ONE pipeline file. Thin; logic lives in
  `src/framework/engine.py`.
- `transformation/conf/`: `sources.yml` (raw), `data_products.yml` (trusted),
  `reconciliation.yml` (DQ rules).
- `transformation/src/data_products/`: one module per trusted table; contract
  in its README.
- `transformation/src/jobs/`: setup, sample data, schema guard, reconcile.

## Commands

```bash
cd transformation
pytest                                   # Spark tests skip without Java
ruff check src tests
databricks bundle validate -t sandbox
databricks bundle deploy -t sandbox
```

## Rules

- Never union streaming DataFrames in `framework/cdc.py`, `framework/engine.py`
  or `data_products/`. One AUTO CDC flow per driver. A test enforces it.
- Pure-Python modules (`config`, `naming`, `schema_drift`, `engine`) must not
  touch Spark at import or registration time, so the graph tests run without a
  JVM. Spark calls go inside the functions the pipeline executes.
- Data products output their keys, `is_deleted` and a non-NULL
  `source_extract_datetime`, and never `_change_type` / `_event_time`.
- PII is hashed once, at the raw boundary. Don't add masking in trusted code.
- New behaviour gets a test in `tests/`.
