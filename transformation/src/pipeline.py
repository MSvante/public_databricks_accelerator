# Databricks notebook source
"""The ONE pipeline source file.

Developers never edit this. Adding a source or a data product is YAML plus
(for a data product) one module in src/data_products/.

Pipeline settings (set in resources/pipeline.yml):
    bundle_root     workspace path of the deployed bundle files
    env             sandbox | dev | prod
    catalog         Unity Catalog catalog
    sandbox_schema  personal schema, sandbox only
    landing_root    UC volume path with the landing files
"""

import logging
import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
logging.basicConfig(level=logging.INFO)

BUNDLE_ROOT = spark.conf.get("bundle_root")
SRC = os.path.join(BUNDLE_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from framework import config as framework_config  # noqa: E402
from framework.engine import build_pipeline  # noqa: E402
from framework.naming import Naming  # noqa: E402

naming = Naming(
    env=spark.conf.get("env"),
    catalog=spark.conf.get("catalog"),
    sandbox_schema=spark.conf.get("sandbox_schema", ""),
)
cfg = framework_config.load(
    os.path.join(BUNDLE_ROOT, "conf"),
    {"landing_root": spark.conf.get("landing_root")},
)

build_pipeline(dp, spark, cfg, naming)
