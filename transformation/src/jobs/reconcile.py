"""Run every active reconciliation rule, store the findings, fail on any.

Runs AFTER the pipeline update. Results are appended to
``<catalog>.ops.reconciliation_results`` (one PASSED row per clean rule, one
row per finding otherwise), so failures can be queried and trended.
"""

import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(sys.argv[sys.argv.index("--bundle-root") + 1], "src"))

import yaml  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from framework.jobargs import parse_args  # noqa: E402
from framework.naming import Naming  # noqa: E402
from framework.reconciliation import RESULT_COLUMNS, compare, parse_rules, render_query  # noqa: E402

args = parse_args(("--results-table", {"default": ""}), ("--max-workers", {"type": int, "default": 8}))
spark = SparkSession.builder.getOrCreate()
naming = Naming(env=args.env, catalog=args.catalog, sandbox_schema=args.sandbox_schema)
results_table = args.results_table or f"{args.catalog}.ops.reconciliation_results"
run_id = str(uuid.uuid4())

with open(os.path.join(args.bundle_root, "conf", "reconciliation.yml")) as fh:
    rules = [r for r in parse_rules(yaml.safe_load(fh)) if r.is_active]


def schema_for(layer, domain):
    return f"{naming.catalog}.{naming.schema(layer, domain)}"


def run(rule):
    try:
        raw = spark.sql(render_query(rule.raw_query, schema_for))
        trusted = spark.sql(render_query(rule.trusted_query, schema_for))
        rows = [r.asDict() for r in compare(raw, trusted, rule).limit(1000).collect()]
    except Exception as e:  # noqa: BLE001 - a broken rule is a failed rule
        rows = [{"rule_id": rule.rule_id, "table_name": rule.table_name, "status": "FAILED",
                 "failure_type": "RULE_ERROR", "message": str(e)[:2000]}]
    if not rows:
        rows = [{"rule_id": rule.rule_id, "table_name": rule.table_name, "status": "PASSED",
                 "message": "All keys and values matched"}]
    return rows


with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
    findings = [row for rows in pool.map(run, rules) for row in rows]

ddl = ", ".join(f"{c} {'bigint' if c == 'rule_id' else 'string'}" for c in RESULT_COLUMNS)
(
    spark.createDataFrame([tuple(f.get(c) for c in RESULT_COLUMNS) for f in findings], ddl)
    .withColumn("run_id", F.lit(run_id))
    .withColumn("checked_at", F.current_timestamp())
    .write.mode("append").saveAsTable(results_table)
)

failed_rules = sorted({f["rule_id"] for f in findings if f["status"] == "FAILED"})
print(f"{len(rules)} rules, {len(failed_rules)} failed. Details: {results_table} WHERE run_id = '{run_id}'")
if failed_rules:
    raise RuntimeError(f"Reconciliation failed for rules {failed_rules}")
