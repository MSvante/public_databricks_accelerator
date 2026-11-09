"""Create every schema the pipeline publishes to, plus the landing volume.

Schemas are derived from the YAML (domains x layers x PII), so a new domain in
sources.yml needs no change here either. Idempotent: safe to run on every deploy.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.argv[sys.argv.index("--bundle-root") + 1], "src"))

from pyspark.sql import SparkSession  # noqa: E402

from framework import config as framework_config  # noqa: E402
from framework.jobargs import parse_args  # noqa: E402
from framework.naming import Naming  # noqa: E402

args = parse_args()
spark = SparkSession.builder.getOrCreate()
naming = Naming(env=args.env, catalog=args.catalog, sandbox_schema=args.sandbox_schema)
cfg = framework_config.load(os.path.join(args.bundle_root, "conf"), {"landing_root": args.landing_root or "/unused"})

domains = {s.domain for s in cfg.sources} | {p.domain for p in cfg.data_products}
pii_domains = {s.domain for s in cfg.sources if s.has_pii} | {p.domain for p in cfg.data_products if p.has_pii}

statements = [f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{s}" for s in naming.all_schemas(domains, pii_domains)]
statements += [
    f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.landing",
    f"CREATE VOLUME IF NOT EXISTS {args.catalog}.landing.files",
    f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.ops",
]
for sql in statements:
    print(sql)
    spark.sql(sql)
