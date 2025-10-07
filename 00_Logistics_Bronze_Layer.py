# Databricks notebook source
# Clear all cached data from Spark (DataFrames and tables)
# testing first pr
# testing it again
spark.catalog.clearCache()
#Load Bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion and saving as Delta Table.

# COMMAND ----------

from pyspark.sql import SparkSession
import json
from datetime import datetime

# COMMAND ----------


#  Initialize Spark session
spark = SparkSession.builder.appName("BronzeLayerAutomation").getOrCreate()

# COMMAND ----------


#  Define your Bronze schema
target_schema = "bronze"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.catalog.setCurrentDatabase(target_schema)

# COMMAND ----------


#  GCS paths 
gcs_input_path = "gs://bronze_kdata/european_logistics_dataset/"
gcs_log_path = f"{gcs_input_path}/logs/bronze_ingestion_logs/"

# COMMAND ----------

#  Start audit logging
audit_logs = []

# COMMAND ----------

supported_ext = [".csv", ".parquet", ".json", ".avro", ".orc"]

# List all supported files
file_list = [
    file.name for file in dbutils.fs.ls(gcs_input_path) 
    if any(file.name.endswith(ext) for ext in supported_ext)
]


# COMMAND ----------

# ----------------------------------------
#  Process each CSV file
# ----------------------------------------
for file_name in file_list:
    table_name = file_name.split(".")[0].lower()
    input_path = f"{gcs_input_path}{file_name}"

    log_entry = {
        "file_name": file_name,
        "table_name": f"{target_schema}.{table_name}",
        "status": "Started",
        "start_time": str(datetime.now()),
        "records": 0,
        "error": ""
    }

    try:
        print(f" ---> Reading: {file_name}")

        # Pick reader dynamically by file extension
        if file_name.endswith(".csv"):
            df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
        elif file_name.endswith(".parquet"):
            df = spark.read.parquet(input_path)
        elif file_name.endswith(".json"):
            df = spark.read.option("multiLine", True).json(input_path)
        elif file_name.endswith(".avro"):
            df = spark.read.format("avro").load(input_path)
        elif file_name.endswith(".orc"):
            df = spark.read.orc(input_path)
        else:
            raise ValueError(f"Unsupported file format: {file_name}")

        # Save to Delta table in Bronze schema
        df.write.format("delta").mode("overwrite").saveAsTable(f"{target_schema}.{table_name}")

        print(f" ------> Saved table: {target_schema}.{table_name}")
        log_entry["status"] = "Success"
        log_entry["records"] = df.count()

    except Exception as e:
        print(f"  Error processing {file_name}: {str(e)}")
        log_entry["status"] = "Failed"
        log_entry["error"] = str(e)

    log_entry["end_time"] = str(datetime.now())
    audit_logs.append(log_entry)

# COMMAND ----------


#  Save audit logs as JSON in GCS

log_json = [json.dumps(log) for log in audit_logs]
log_df = spark.read.json(spark.sparkContext.parallelize(log_json))

log_output_path = f"{gcs_log_path}audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
log_df.write.mode("overwrite").json(log_output_path)

print("  Bronze ingestion completed. All tables saved in schema. Logs written.")
