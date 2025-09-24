"""
# Copied from lines 1-447 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
# Databricks notebook source
# 🔧 Spark Configuration for Databricks on GCP

# ========== Core Execution ==========
spark.conf.set("spark.sql.shuffle.partitions", "256")                # Higher shuffle partitions for GCP clusters
spark.conf.set("spark.sql.adaptive.enabled", "true")                 # Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# ========== Delta Optimizations ==========
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # Needed if VACUUM with <7d retain

# ========== Storage & GCS Connector ==========
spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.fs.gs.metadata.cache.enable", "true")   # Metadata caching for GCS

# ========== DataFrame / Execution ==========
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable Arrow for PySpark <-> Pandas
spark.conf.set("spark.sql.broadcastTimeout", "900")                  # Longer timeout for big joins on GCP
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")     # 128 MB partitions
spark.conf.set("spark.sql.files.openCostInBytes", "67108864")        # 64 MB → improves GCS listing perf

# ========== Parquet / ORC ==========
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")

# ========== Cleaner / GC ==========
# spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")  # Cannot modify this config
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")

print("Spark configuration applied for GCP cluster")

# COMMAND ----------

# MAGIC %run ./01_setup_environment

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./02_load_bronze_tables

# COMMAND ----------

# MAGIC %run ./03_define_rules

# COMMAND ----------

from pyspark.sql.types import StringType, NumericType, StructType, StructField
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator
import uuid
import logging
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# COMMAND ----------

#  Broadcast for Spark workers
rule_to_metric_broadcast = spark.sparkContext.broadcast(rule_to_metric)


# COMMAND ----------

# MAGIC %md
# MAGIC # GX Validation

# COMMAND ----------

# -------------------------------
# 3. Widgets (from UI)
# -------------------------------
dbutils.widgets.text("table_names", "")
dbutils.widgets.text("schema_name", "bronze")
dbutils.widgets.text("validation_schema", "dq_validation")

selected_tables = dbutils.widgets.get("table_names").split(",")
source_schema = dbutils.widgets.get("schema_name")
validation_schema = dbutils.widgets.get("validation_schema")

print("Running GX Validation for tables:", selected_tables)
print("Source Schema:", source_schema)
print("Validation Schema:", validation_schema)

# COMMAND ----------

# -------------------------------
# 4. GX Helpers 
# -------------------------------
    # Rule: Check if column values are of allowed types
    #  Works without reference data. You just define a list of valid Spark types.
def get_expectation_kwargs(rule, col_name=None, df=None):
    if rule == "expect_column_values_to_be_in_type_list":
        return {"column": col_name, "type_list": ["StringType", "IntegerType", "DoubleType"]}
    
        # Rule: Check if values are in an approved set (row-level) OR
    #       check if distinct values match an approved set (aggregate-level).
    #  IMPORTANT:
    #    - By default, this code builds the "approved list" dynamically from the same dataset.
    #    - This means it does NOT truly validate correctness (it just reuses whatever is present).
    #    - In practice, you should pass a predefined list OR fetch values from a master/reference table
    #      (e.g., valid Status codes, valid Country codes, CustomerIDs from a customer master).
    
    elif rule in ["expect_column_values_to_be_in_set", "expect_column_distinct_values_to_be_in_set"]:
        if col_name and df is not None and col_name in df.columns:
            try:
                ref_values = [row[col_name] for row in df.select(col_name).distinct().collect()]
                return {"column": col_name, "value_set": ref_values}
            except Exception as e:
                print(f" Could not fetch dynamic approved list for {col_name}: {e}")
        return {"column": col_name}
    
      
    # Rule: Check if column exists in table schema
    #  Does not need reference/master table. Just validates presence in the DataFrame.

    # -----------------------------------------------------------------------------
# Schema Contract Configuration (for expect_column_to_exist rule)
# -----------------------------------------------------------------------------
# Example placeholder:
# schema_contract = {
#     "customers": ["customer_id", "status", "created_at"],
#     "orders": ["order_id", "customer_id", "order_date", "amount"]
# }
#
# NOTE:
# - This dictionary is only for illustration/testing.
# - The client should provide schema_contract as either:
#     1. A JSON file stored in DBFS/Cloud Storage, OR
#     2. A Delta table maintained inside Databricks.
# - This defines the "required columns" for each dataset/table.
# - If any required column is missing in the dataset, the rule
#   `expect_column_to_exist` will fail with Failure_Type = "Schema".
# -----------------------------------------------------------------------------
    elif rule == "expect_column_to_exist":
        return {"column": col_name}

    return {"column": col_name}

# COMMAND ----------

# Column Validation Function
# -------------------------------
def validate_column(validator, df, table_name, col_name, rule):
    # Base metadata that will be included in every record
    base = {
        "Table": table_name,
        "Rule": rule,
        "Rule_Display_Name": rule_display_names.get(rule, rule),
        "Rule_Description": rule_descriptions.get(rule, ""),
        "Column": col_name,
        "Metric": rule_to_metric_broadcast.value.get(rule, "Other"),
        "Run_Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Execution_ID": str(uuid.uuid4())
    }

    try:
        # Run the GX expectation dynamically
        method = getattr(validator, rule)
        kwargs = get_expectation_kwargs(rule, col_name, df)
        result = method(col_name, **kwargs)

        # -------------------------------
        # CASE 1: Rule Passed
        # -------------------------------
        if result.success:
            return [{
                **base,
                "Status": "Passed",
                "Failure_Type": None,
                "Failed_Value": None,
                "Failed_Row_ID": None,
                "Failure_Count": 0,
                "GX_Sample_Failed_Value": None,
                "Comments": "Validation passed successfully."
            }]

        # -------------------------------
       # -------------------------------
        # CASE 2: Row-level Failures
        # -------------------------------
        # - Applies to set-based rules (in_set, distinct_in_set)
        # - GX provides partial_unexpected_list (sample of failed values)
        # - Spark counts all failing rows
        # - Collect only 10 sample rows to avoid driver overload
        unexpected = result.result.get("partial_unexpected_list", [])
        if unexpected and col_name in df.columns:
            failed_df = df.filter(col(col_name).isin(unexpected))
            failure_count = failed_df.count()           # total failing rows
            rows = failed_df.limit(10).collect()        # sample rows only

            return [{
                **base,
                "Status": "Failed",
                "Failure_Type": "RowLevel",
                "Failed_Value": str(row[col_name]),     # sample failed value
                "Failed_Row_ID": str(i+1),              # row index (sample only)
                "Failure_Count": failure_count,
                "GX_Sample_Failed_Value": str(unexpected),  # GX sample values
                "Comments": rule_comments.get(rule, "Row-level validation failed. Please review.")
            } for i, row in enumerate(rows)]

        # -------------------------------
        # CASE 3: Aggregate Failures
        # -------------------------------
        # - Covers type mismatches or schema-wide checks
        # - GX gives observed_value (e.g., actual datatype)
        observed_value = result.result.get("observed_value")
        if observed_value is not None:
         return [{
        **base,
        "Status": "Failed",
        "Failure_Type": "Aggregate",
        "Failed_Value": str(observed_value),
        "Failed_Row_ID": "Aggregate",
        "Failure_Count": None,
        "GX_Sample_Failed_Value": None,
        "Comments": rule_comments.get(rule, "Aggregate validation failed. Please review.")
    }]

        # -------------------------------
        # CASE 4: Unknown Failures
        # -------------------------------
        # - Safety net if GX doesn’t return details
        return [{
            **base,
            "Status": "Failed",
            "Failure_Type": "Unknown",
            "Failed_Value": "UnknownFailure",
            "Failed_Row_ID": "N/A",
            "Failure_Count": None,
            "GX_Sample_Failed_Value": None,
            "Comments": "Validation failed but reason could not be identified."
        }]

    # -------------------------------
    # CASE 5: Exception Handling
    # -------------------------------
    # - Captures runtime errors so the job doesn’t crash
    except Exception as e:
        logger.warning(f" Exception on {table_name}.{col_name}.{rule}: {e}")
        return [{
            **base,
            "Status": "Failed",
            "Failure_Type": "Error",
            "Failed_Value": str(e),
            "Failed_Row_ID": "N/A",
            "Failure_Count": None,
            "GX_Sample_Failed_Value": None,
            "Comments": f"Validation error occurred: {e}"
        }]

# COMMAND ----------

# -------------------------------
# 5. Column Validation (Only your 4 rules)
# -------------------------------
from pyspark.sql.functions import col, lit
import uuid
from datetime import datetime
 # Base metadata that will be included in every record
def validate_column(validator, df, table_name, col_name, rule):
    base = {
        "Table": table_name,
        "Rule": rule,
        "Rule_Display_Name": rule_display_names.get(rule, rule),
        "Rule_Description": rule_descriptions.get(rule, ""),
        "Column": col_name,
        "Metric": rule_to_metric_broadcast.value.get(rule, "Other"),
        "Run_Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Execution_ID": str(uuid.uuid4())
    }
    try:
                # Run the GX expectation dynamically
        kwargs = get_expectation_kwargs(rule, col_name, df)
        method = getattr(validator, rule)
        result = method(**kwargs)

        if result.success:
            return [{**base, "Status": "Passed", "Failure_Type": None,
                     "Failed_Value": None, "Failed_Row_ID": None}]

        # Handle failures
        if col_name in df.columns:
            condition = None
            if rule in ["expect_column_values_to_be_in_set", "expect_column_distinct_values_to_be_in_set"]:
                values = kwargs.get("value_set", [])
                condition = ~col(col_name).isin(values)
            elif rule == "expect_column_to_exist":
                # If column missing, return immediate failure
                return [{**base, "Status": "Failed", "Failure_Type": "Schema",
                         "Failed_Value": f"Column {col_name} does not exist",
                         "Failed_Row_ID": "N/A"}]
            elif rule == "expect_column_values_to_be_in_type_list":
                # Type check failures are aggregate in GX, not row-level
                observed_type = result.result.get("observed_value", "UnknownType")
                return [{**base, "Status": "Failed", "Failure_Type": "TypeMismatch",
                         "Failed_Value": str(observed_type), "Failed_Row_ID": "Aggregate"}]

            if condition is not None:
                failed_df = df.filter(condition)
                rows = failed_df.collect()
                return [
                    {**base, "Status": "Failed", "Failure_Type": "RowLevel",
                     "Failed_Value": str(r[col_name]), "Failed_Row_ID": str(i+1)}
                    for i, r in enumerate(rows)
                ]

        observed_value = result.result.get("observed_value")
        if observed_value is not None:
            return [{**base, "Status": "Failed", "Failure_Type": "Aggregate",
                     "Failed_Value": str(observed_value), "Failed_Row_ID": "Aggregate"}]

        return [{**base, "Status": "Failed", "Failure_Type": "Unknown",
                 "Failed_Value": "UnknownFailure", "Failed_Row_ID": "N/A"}]

    except Exception as e:
        logger.warning(f" Exception on {table_name}.{col_name}.{rule}: {e}")
        return [{**base, "Status": "Failed", "Failure_Type": "Error",
                 "Failed_Value": str(e), "Failed_Row_ID": "N/A"}]


# COMMAND ----------

# -------------------------------
# 6. Table Validation
# -------------------------------
def validate_table(table_name, df):
    try:
        df = df.persist()
        suite = ExpectationSuite(f"{table_name}_suite")
        engine = SparkDFExecutionEngine()
        batch = Batch(data=df)
        validator = Validator(execution_engine=engine, batches=[batch], expectation_suite=suite)

        results = []
        for field in df.schema.fields:
            col_name = field.name
            col_type = "string" if isinstance(field.dataType, StringType) else \
                       "numeric" if isinstance(field.dataType, NumericType) else "other"
            rules = get_rules(col_name, col_type)
            for rule in rules:
                results.extend(validate_column(validator, df, table_name, col_name, rule))
        return results
    finally:
        df.unpersist()

# COMMAND ----------

# -------------------------------
# 7. Parallel Validation
# -------------------------------
def parallel_validate_selected_tables(selected_tables, max_workers=4):
    spark_tables = {t: spark.table(f"{source_schema}.{t.strip()}") for t in selected_tables if t.strip()}
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(validate_table, name, df): name for name, df in spark_tables.items()}
        for future in futures:
            try:
                all_results.extend(future.result())
            except Exception as e:
                logger.error(f" Failed on table {futures[future]}: {e}")
    return all_results


# COMMAND ----------

# -------------------------------
# 8. Logger setup
import logging
# -------------------------------
logger = logging.getLogger("GXValidationLogger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.info("Logger ready.")

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor


# COMMAND ----------

# -------------------------------
# 9. Run Validation
# -------------------------------
validation_results = parallel_validate_selected_tables(selected_tables, max_workers=4)

validation_schema_struct = StructType([
    StructField("Table", StringType(), True),
    StructField("Column", StringType(), True),
    StructField("Rule", StringType(), True),
    StructField("Rule_Display_Name", StringType(), True),
    StructField("Rule_Description", StringType(), True),
    StructField("Metric", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Failure_Type", StringType(), True),
    StructField("Failed_Value", StringType(), True),
    StructField("Failed_Row_ID", StringType(), True),
    StructField("Execution_ID", StringType(), True),
    StructField("Run_Timestamp", StringType(), True)
])

if not validation_results:
    print(" No validation results generated.")
else:
    df_result = spark.createDataFrame(validation_results, schema=validation_schema_struct)

    df_passed = df_result.filter(col("Status") == "Passed")
    df_failed = df_result.filter((col("Status") == "Failed") & (col("Failure_Type") == "RowLevel"))
    df_errors = df_result.filter(col("Failure_Type").isin(["Error", "Unknown", "Aggregate"]))

    df_passed.write.mode("overwrite").saveAsTable(f"{validation_schema}.gx_dq_validation_passed")
    df_failed.write.mode("overwrite").saveAsTable(f"{validation_schema}.gx_dq_validation_failed")
    df_errors.write.mode("overwrite").saveAsTable(f"{validation_schema}.gx_dq_validation_errors")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_validation.gx_dq_validation_passed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_validation.gx_dq_validation_failed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_validation.gx_dq_validation_errors


