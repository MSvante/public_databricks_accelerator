"""Config-driven Lakeflow Declarative Pipelines framework.

Pure-Python modules (config, naming, schema_drift) import nothing from Spark at
module level, so they can be unit tested without a JVM.
"""
