import sys

try:
    catalog = sys.argv[1]
except IndexError:
    catalog = "cjc"
try:
    schema = sys.argv[2]
except IndexError:
    schema = "kimball_modeling"

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ModuleNotFoundError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{flights_schema};")