# Databricks notebook source
# MAGIC %md
# MAGIC # AI-Powered Anomaly Detection 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import mlflow.deployments
import json, re, logging
from datetime import datetime, date

# COMMAND ----------

# =====================================================
# 1. Initialize
# =====================================================
spark = SparkSession.builder.appName("EnhancedAIAnomalyDetection").getOrCreate()
client = mlflow.deployments.get_deploy_client("databricks")

# --- Widgets (accept from job / UI) ---
dbutils.widgets.text("table_names", "")
dbutils.widgets.text("source_schema", "bronze")
dbutils.widgets.text("metadata_schema", "dq_metadata")

selected_tables = [t.strip() for t in dbutils.widgets.get("table_names").split(",") if t.strip()]
source_schema = dbutils.widgets.get("source_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")

print("Running Anomaly Detection for tables:", selected_tables)
print("Source Schema:", source_schema)
print("Validation Schema:", metadata_schema)

llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# =====================================================
# Config for Sampling
# =====================================================
MAX_SAMPLE_ROWS = 10000   # cap rows sent to AI
SAMPLE_FRACTION = 0.01    # default fraction (1%)

# =====================================================
# Anomaly log schema
# =====================================================
schema = StructType([
    StructField("table", StringType(), True),
    StructField("column", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("confidence_score", DoubleType(), True),
    StructField("detected_at", StringType(), True)
])

# COMMAND ----------

# =====================================================
# Helper Functions
# =====================================================
def convert_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def dynamic_sample(df):
    total_count = df.count()
    if total_count == 0:
        return df.limit(0)

    fraction = SAMPLE_FRACTION
    approx_size = int(total_count * fraction)
    if approx_size > MAX_SAMPLE_ROWS:
        fraction = MAX_SAMPLE_ROWS / total_count

    return df.sample(withReplacement=False, fraction=fraction, seed=42).limit(MAX_SAMPLE_ROWS)

# COMMAND ----------

# =====================================================
# Core Functions
# =====================================================
def detect_anomalies_per_column(table_list):
    """Run anomaly detection per column → exactly one row per column."""
    logs = []
    for table in table_list:
        try:
            full_table = f"{source_schema}.{table}"
            df = spark.table(full_table)
            if df.count() == 0:
                print(f" Skipping {table} (empty).")
                continue

            sampled_df = dynamic_sample(df)

            # Loop through each column once
            for field in sampled_df.schema.fields:
                col_name = field.name
                col_type = field.dataType.simpleString()
                col_sample = sampled_df.select(col_name).toPandas().to_dict(orient="records")

                prompt = f"""
                You are an AI anomaly detection system. Analyze column `{col_name}` (type: {col_type}).
                Based on the sample, describe the **most relevant anomaly pattern** (nulls, outliers, unusual values).
                Respond ONLY with a single JSON object:
                {{
                    "reason": "text",
                    "severity": "Low/Medium/High",
                    "confidence_score": float
                }}
                Sample: {json.dumps(col_sample, default=convert_json_safe)}
                """

                response = client.predict(
                    endpoint=llm_endpoint,
                    inputs={"messages": [{"role": "user", "content": prompt}]}
                )

                content = response["choices"][0]["message"]["content"]
                match = re.search(r"\{.*\}", content.strip(), re.DOTALL)

                if match:
                    try:
                        parsed = json.loads(match.group(0))
                        logs.append({
                            "table": table,
                            "column": col_name,
                            "reason": parsed.get("reason"),
                            "severity": parsed.get("severity"),
                            "confidence_score": float(parsed.get("confidence_score", 0)),
                            "detected_at": datetime.now().isoformat()
                        })
                    except json.JSONDecodeError as json_err:
                        logging.error(f" JSON parsing failed for {table}.{col_name}: {json_err}")
                        logging.error(f"Raw AI content:\n{content}")
                else:
                    logs.append({
                        "table": table,
                        "column": col_name,
                        "reason": "No anomaly detected",
                        "severity": "Low",
                        "confidence_score": 0.0,
                        "detected_at": datetime.now().isoformat()
                    })

        except Exception as e:
            logging.error(f" Failed on {table}: {str(e)}")
    return logs

# COMMAND ----------

# -------------------------------
# Save logs
# -------------------------------
def save_logs(logs, delta_table):
    if logs:
        df = spark.createDataFrame(logs, schema)
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(delta_table)
        print(f"----> Anomalies saved to {delta_table}")
    else:
        spark.createDataFrame([], schema) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(delta_table)
        print(f"-> No anomalies found. Empty table created at {delta_table}")

# COMMAND ----------

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    if not selected_tables:
        print("⚠️ No tables provided via UI/job widget.")
    else:
        all_logs = detect_anomalies_per_column(selected_tables)
        target_table = f"{metadata_schema}.ai_anomaly_detection_log"
        save_logs(all_logs, target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_metadata.ai_anomaly_detection_log