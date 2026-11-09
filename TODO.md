# TODO

Open items for the `ldp-framework` rewrite. Nothing here blocks reading the code
or running the unit tests.

## Verify on a real workspace (not done yet)

- [ ] `databricks bundle validate` for `sandbox`, `dev`, `prod`. No CLI was
      available when this was written, so the bundle YAML is unvalidated.
- [ ] Full Free Edition run: `ldp_setup` → `ldp_sample_data` → `ldp_ingest`,
      then the `changes` and `type_drift` batches.
- [ ] Serverless pipeline `environment.dependencies` (pyyaml) is accepted, or
      pyyaml is already in the serverless image.
- [ ] `dp.temporary_view` exists on Databricks (the engine falls back to
      `dp.view`).
- [ ] `create_auto_cdc_flow(..., except_column_list=[...])` drops the `_op`
      column from raw SCD1 tables as intended.
- [ ] Publishing to many schemas with fully qualified names from one pipeline
      works with the bundle's `catalog` + `schema` settings.
- [ ] `full_refresh_selection` takes `catalog.schema.table` names. If it wants
      `schema.table`, change `Naming.fqn` usage in `schema_drift.refresh_selection`.
- [ ] After a selective full refresh of a raw table, do the dependent trusted
      flows really need a full refresh too (the guard includes them by default:
      `--include-dependents true`)? Confirm and document the behaviour.
- [ ] `dbutils.fs.ls` on volumes returns a useful `modificationTime` for
      folders; otherwise the guard falls back to name order (timestamped names).
- [ ] Spark-backed tests pass in CI (`.github/workflows/tests.yml`). They are
      skipped locally without Java.

## Design follow-ups

- [ ] Duplicate events for one key in one micro-batch: both rows carry the same
      snapshot values and sequence. Confirm AUTO CDC handles that cleanly, or
      add a tie-breaker to `sequence_by`.
- [ ] Soft delete keeps the key but NULLs the attributes (values come from a
      snapshot where the row is gone). Option: `ignore_null_updates` on trusted
      flows, with its own trade-off.
- [ ] Reconciliation: optional date-window parameters for very large facts.
- [ ] Event-log based run summary (rows processed per flow) in `ops`.
- [ ] A selective (non-full) refresh helper job for manual reprocessing of one
      table.

## Talk alignment

- [ ] Cross-check snippets in the talk manuscript against this code once both
      are final (names: `DATA_PRODUCT`, `read_changes_and_snapshots`,
      `driver_scope`, `latest_data_file`, `refresh_selection`).
