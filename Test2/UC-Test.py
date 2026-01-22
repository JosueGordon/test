# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import os
os.chdir("/Workspace/Users/isaac.salazar@databricks.com/test-")

def check_paths(iterator):
    import sys
    yield [p for p in sys.path if '/Workspace/' in p]

paths = sc.parallelize([1], 1).mapPartitions(check_paths).collect()
print("Workspace paths on executor:", paths)

# Test 1: import failure
def test_import(iterator):
    try:
        import pandas
        yield {"status": "success"}
    except OSError as e:
        yield {"status": "failed", "error": str(e)}

result = sc.parallelize([1], 1).mapPartitions(test_import).collect()
print("Import test:", result)
# Expected: success
# Actual: OSError with "/Workspace/..." path

# Test 2: workaround
def test_import_with_fix(iterator):
    import sys
    sys.path = [p for p in sys.path if '/Workspace/' not in p]  # Fix
    try:
        import pandas
        yield {"status": "success"}
    except OSError as e:
        yield {"status": "failed", "error": str(e)}

result_fixed = sc.parallelize([1], 1).mapPartitions(test_import_with_fix).collect()
print("Import test with path fix:", result_fixed)