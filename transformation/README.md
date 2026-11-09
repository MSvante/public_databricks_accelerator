# ldp_framework

The Databricks Asset Bundle. See the [repository README](../README.md) for the
architecture, quick start and how it maps to the talk.

```bash
databricks bundle deploy -t sandbox
databricks bundle run ldp_setup -t sandbox
databricks bundle run ldp_sample_data -t sandbox
databricks bundle run ldp_ingest -t sandbox
pytest
```

## Design notes

- **Values come from snapshots, events only carry keys.** The CDF stream of a
  driver says *which* key changed; the product reads the current values from
  static snapshots. Replaying an old event yields today's truth, and two drivers
  touching the same key converge to the same row.
- **One flow per driver.** Each driver is its own AUTO CDC flow (and its own
  checkpoint) into the shared target. Unioning streams would share a checkpoint
  and make adding or removing a driver a recovery problem.
- **Satellites don't trigger.** A change only in a satellite reaches the trusted
  table when a driver next touches that key. Fine for slowly changing lookups;
  promote the table to a driver if it isn't.
- **PII is hashed once,** at the raw boundary (vault → public). Downstream
  public tables inherit the hashes, so the same email has the same hash
  everywhere and hashed columns remain joinable. A missing PII column fails the
  update instead of leaking clear text.
- **The schema guard checks the newest landing file,** not an arbitrary one: a
  type change only exists in files written after it.
- **The guard waits** for the selective refresh to finish before the regular
  update starts, so the two never collide.
- **`raw_log_*` is never refreshed by the guard:** a full refresh re-reads the
  whole landing zone, and with landing retention that loses history.
- **Pipeline retries are off** (`pipelines.maxFlowRetryAttempts: 0`): the job
  owns retries, and a broken flow fails fast.
- **Sequence columns are never NULL:** deleted rows fall back to the change
  event's commit time.
