import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
CONF = ROOT / "conf"
sys.path.insert(0, str(SRC))


def _java_available() -> bool:
    return bool(os.environ.get("JAVA_HOME") or shutil.which("java"))


@pytest.fixture(scope="session")
def spark():
    """Local Spark session. Tests using it are skipped when there is no Java,
    so the pure-Python tests still run anywhere."""
    if not _java_available():
        pytest.skip("Java not available; Spark tests run in CI")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("ldp-framework-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg():
    from framework import config

    return config.load(CONF, {"landing_root": "/Volumes/demo/landing/files"})
