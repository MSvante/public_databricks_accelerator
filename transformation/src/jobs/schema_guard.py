"""Pre-flight schema guard. Runs BEFORE the regular pipeline update.

For every source table, compare the type of each column in the landing zone
with the existing raw table. If any type changed, start a pipeline update with
``full_refresh_selection`` for only the affected tables (and the trusted tables
they drive), and WAIT for it, so the regular incremental update that follows
in the job doesn't collide with it.

Exit states (printed and stored as task value ``schema_guard_status``):
    NO_CHANGES                nothing to do
    REFRESHED:<update_id>     selective full refresh completed
Raises if validation of any table fails or the refresh doesn't complete.
"""

import importlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(sys.argv[sys.argv.index("--bundle-root") + 1], "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

from framework import config as framework_config  # noqa: E402
from framework.jobargs import parse_args  # noqa: E402
from framework.naming import Naming  # noqa: E402
from framework.schema_drift import (  # noqa: E402
    find_type_changes,
    latest_data_file,
    product_drivers,
    refresh_selection,
)

args = parse_args(
    ("--pipeline-id", {"required": True}),
    ("--include-dependents", {"default": "true"}),
    ("--timeout-minutes", {"type": int, "default": 120}),
)
spark = SparkSession.builder.getOrCreate()

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.runtime import dbutils  # noqa: E402

naming = Naming(env=args.env, catalog=args.catalog, sandbox_schema=args.sandbox_schema)
cfg = framework_config.load(os.path.join(args.bundle_root, "conf"), {"landing_root": args.landing_root})


def list_dir(path):
    return [(f.path, f.isDir(), getattr(f, "modificationTime", 0)) for f in dbutils.fs.ls(path)]


def check(src):
    try:
        landing_file = latest_data_file(list_dir, src.landing_path)
        if landing_file is None:
            return src.name, "SKIPPED", []
        landing = spark.read.format(src.format).load(landing_file).schema
        existing = spark.table(naming.fqn("raw", src.domain, src.name)).schema
        changes = find_type_changes(landing, existing, ignore=[src.operation_column, src.timestamp_column])
        return src.name, ("TYPE_CHANGE" if changes else "OK"), changes
    except AnalysisException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return src.name, "NEW_TABLE", []
        return src.name, f"FAILED: {e}", []
    except Exception as e:  # noqa: BLE001 - reported below, job fails
        return src.name, f"FAILED: {e}", []


with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(check, cfg.sources))

for name, status, changes in results:
    detail = ", ".join(f"{c.column}: {c.old_type} -> {c.new_type}" for c in changes)
    print(f"{name:30s} {status} {detail}")

failed = [name for name, status, _ in results if status.startswith("FAILED")]
if failed:
    raise RuntimeError(f"Schema validation failed for {failed}; not starting any refresh")

changed = [name for name, status, _ in results if status == "TYPE_CHANGE"]
if not changed:
    dbutils.jobs.taskValues.set("schema_guard_status", "NO_CHANGES")
    print("NO_CHANGES")
    sys.exit(0)

selection = refresh_selection(
    changed, cfg, naming,
    product_drivers(cfg, importlib.import_module),
    include_dependents=args.include_dependents.lower() == "true",
)
print("Full refresh selection:\n  " + "\n  ".join(selection))

w = WorkspaceClient()
update_id = w.pipelines.start_update(pipeline_id=args.pipeline_id, full_refresh_selection=selection).update_id
deadline = time.time() + args.timeout_minutes * 60
while True:
    state = w.pipelines.get_update(pipeline_id=args.pipeline_id, update_id=update_id).update.state.value
    print(f"update {update_id}: {state}")
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    if time.time() > deadline:
        raise TimeoutError(f"Selective refresh {update_id} still {state} after {args.timeout_minutes} min")
    time.sleep(30)

if state != "COMPLETED":
    raise RuntimeError(f"Selective refresh {update_id} ended {state}")
dbutils.jobs.taskValues.set("schema_guard_status", f"REFRESHED:{update_id}")
print(f"REFRESHED:{update_id}")
