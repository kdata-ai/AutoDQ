# Copied from 06_Smart Data Cleaning.py lines 1-203
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import mlflow.deployments
import json
import re
from datetime import datetime

# COMMAND ----------

# --- Config ---
spark = SparkSession.builder.appName("FullAICleaningPipeline").getOrCreate()
client = mlflow.deployments.get_deploy_client("databricks")

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
log_rows = []

# COMMAND ----------

# --- Widgets (accept from job / UI) ---
dbutils.widgets.text("table_names", "")
dbutils.widgets.text("source_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("metadata_schema", "dq_metadata")

selected_tables = [t.strip() for t in dbutils.widgets.get("table_names").split(",") if t.strip()]
source_schema = dbutils.widgets.get("source_schema")
silver_schema = dbutils.widgets.get("silver_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")

print("Running Data Cleaning for tables:", selected_tables)
print("Source Schema:", source_schema)
print("Silver Schema:", silver_schema)
print("Metadata Schema:", metadata_schema)


# COMMAND ----------

# --- Helpers ---
def get_stats(df, col_name):
    s = df.select(
        count(col(col_name)).alias("count"),
        countDistinct(col(col_name)).alias("distinct"),
        count(when(col(col_name).isNull(), col(col_name))).alias("nulls"),
        min(col(col_name)).alias("min"),
        max(col(col_name)).alias("max")
    ).collect()[0].asDict()
    return s

def clean_ai_response(raw):
    cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
    cleaned = re.sub(r"\n?```$", "", cleaned).strip()
    return cleaned

def get_ai_cleaning(column, data_type, sample_values, stats, table):
    prompt = f"""
You are an expert data cleaning assistant.

You need to suggest a PySpark expression to clean the provided column based on the data type, sample values, and statistics.

**Column Details**
- Table Name: {table}
- Column Name: {column}
- Data Type: {data_type}
- Sample Values: {sample_values}
- Statistics: {stats}

**Cleaning Goals by Data Type:**
- For **String/Text**: remove leading/trailing spaces, standardize case (upper/lower), remove special characters if present, correct known formatting issues (emails, phone numbers).
- For **Numeric**: handle nulls, remove outliers, round decimals if necessary, replace negative values if invalid, normalize or scale values if required.
- For **Date/Time**: convert to proper date format, parse inconsistent date strings, fill missing dates with a default or infer from related data.
- For **Boolean**: standardize to True/False or 1/0.
- For **Categorical**: standardize categories, fix inconsistent naming, apply consistent case.
- For **IDs or Keys**: trim spaces, convert to uppercase if alphanumeric, ensure format consistency.

**Instructions**
- Provide a **PySpark expression** that can clean this column.
- If the column does not require cleaning, return "None".
- Also return a **short description of the cleaning logic** applied.
- If skipped, provide the **reason for skipping**.

**Expected JSON format (no markdown or code block):**
{
    "column_name": "{column}",
    "cleaning_expression": "PySpark expression using functions like lower, upper, trim, regexp_replace, coalesce, when, round, to_date, lit, etc.",
    "description": "Description of the cleaning logic applied.",
    "reason_if_skipped": "Explain why no cleaning is needed."
}
"""
    try:
        response = client.predict(
            endpoint=LLM_ENDPOINT,
            inputs={"messages": [{"role": "user", "content": prompt}]}
        )
        raw = response["choices"][0]["message"]["content"].strip()
        cleaned_raw = clean_ai_response(raw)
        ai_json = json.loads(cleaned_raw)
        return ai_json
    except Exception as e:
        print(f" Failed to parse AI response for `{column}` in `{table}`: {e}")
        return None

# COMMAND ----------

# --- Main Cleaning Process ---
for table in selected_tables:
    try:
        df = spark.table(f"{source_schema}.{table}").cache()
        if df.rdd.isEmpty():
            print(f" Skipping empty table: {table}")
            continue

        cleaned_df = df
        is_cleaned = False
        print(f"\n--> Cleaning Table: {table}")

        for field in df.schema.fields:
            col_name = field.name
            dtype = field.dataType.simpleString()
            stats = get_stats(df, col_name)
            sample = df.select(col(col_name)).dropna().distinct().limit(3).rdd.flatMap(lambda x: x).collect()

            ai = get_ai_cleaning(col_name, dtype, sample, stats, table)
            expr_str = ai.get('cleaning_expression') if ai else None

            if expr_str is None or expr_str.strip().lower() == "none":
                log_rows.append({
                    "table_name": table, "saved_table_name": None, "column_name": col_name, "data_type": dtype,
                    "status": "Skipped", "rule": None, "description": None,
                    "pre_stats": str(stats), "post_stats": None,
                    "reason_if_skipped": ai.get("reason_if_skipped", "No AI response") if ai else "No AI response",
                    "run_at": timestamp_now
                })
                continue

            try:
                expr_str = expr_str.replace("F.", "")
                expr_str = re.sub(rf'\b{col_name}\b', f'col("{col_name}")', expr_str)

                eval_ctx = {
                    "col": col, "trim": trim, "lower": lower, "upper": upper,
                    "regexp_replace": regexp_replace, "coalesce": coalesce, "lit": lit,
                    "round": round, "when": when, "to_date": to_date
                }

                cleaned = eval(expr_str, eval_ctx)
                cleaned_df = cleaned_df.withColumn(col_name, cleaned)
                post_stats = get_stats(cleaned_df, col_name)

                is_cleaned = True

                log_rows.append({
                    "table_name": table, "saved_table_name": None, "column_name": col_name, "data_type": dtype,
                    "status": "Cleaned", "rule": expr_str, "description": ai.get("description"),
                    "pre_stats": str(stats), "post_stats": str(post_stats),
                    "reason_if_skipped": None,
                    "run_at": timestamp_now
                })
                print(f"-----> Cleaned `{col_name}` in `{table}` with rule: {expr_str}")

            except Exception as e:
                log_rows.append({
                    "table_name": table, "saved_table_name": None, "column_name": col_name, "data_type": dtype,
                    "status": "Failed", "rule": expr_str, "description": ai.get("description") if ai else None,
                    "pre_stats": str(stats), "post_stats": None,
                    "reason_if_skipped": f"Eval failed: {e}",
                    "run_at": timestamp_now
                })

        # Save table in "silver" schema with SAME name as original
        final_table_name = table
        df_to_write = cleaned_df if is_cleaned else df
        df_to_write.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{silver_schema}.{final_table_name}")

        print(f" Table saved as: {silver_schema}.{final_table_name} (Cleaned: {is_cleaned})")

        # Update saved_table_name in log
        for log in log_rows:
            if log["table_name"] == table and log["saved_table_name"] is None:
                log["saved_table_name"] = final_table_name

    except Exception as e:
        print(f" Failed to process table `{table}`: {e}")

# COMMAND ----------

# --- Save Cleaning Log with Ordered Columns ---
if log_rows:
    desired_column_order = [
        "table_name", "saved_table_name", "column_name", "data_type", "status",
        "rule", "description", "pre_stats", "post_stats", "reason_if_skipped", "run_at"
    ]
    log_df = spark.createDataFrame(log_rows).select(*desired_column_order)
    log_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{metadata_schema}.data_cleaning_log")
    print(f"\n-----------------------> Cleaning log saved to: {metadata_schema}.data_cleaning_log")
else:
    print(" No cleaning logs to save.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_metadata.data_cleaning_log;

