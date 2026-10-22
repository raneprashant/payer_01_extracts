from pyspark.sql import SparkSession

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    spark = SparkSession.builder.getOrCreate()

print("Hello from payer_01_extracts Databricks Job")

spark.range(5).show()

print("Current catalog:")
spark.sql("SELECT current_catalog()").show(truncate=False)

print("Current schema:")
spark.sql("SELECT current_schema()").show(truncate=False)