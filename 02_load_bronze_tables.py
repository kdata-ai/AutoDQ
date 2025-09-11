# Databricks notebook source
# MAGIC %md
# MAGIC #LOAD BRONZE TABLES FROM SCHEMA

# COMMAND ----------

# %run ./01_setup_environment
from pyspark.sql.functions import monotonically_increasing_id

bronze_schema = "bronze"

def load_tables(schema):
    tables = {}
    for tbl in spark.catalog.listTables(schema):
        table_name = tbl.name
        df = spark.read.table(f"{schema}.{table_name}") \
                       .withColumn("row_id", monotonically_increasing_id())
        df = df.cache()

        # compute row count here
        row_count = df.count()
        print(f" Loaded table: {table_name} ({row_count} rows)")

        tables[table_name] = df
    return tables

# Load all tables under the bronze schema
spark_tables = load_tables(bronze_schema)

# Print summary
print("\n Tables in schema:", bronze_schema)
for t, df in spark_tables.items():
    print(f"- {t}: {df.count()} rows")