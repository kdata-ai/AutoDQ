# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import mlflow.deployments
import json, re, logging

# COMMAND ----------


# --------------------------------
#  Setup
# --------------------------------
spark = SparkSession.builder.appName("MetadataLabeling").getOrCreate()
client = mlflow.deployments.get_deploy_client("databricks")

llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# --------------------------------
#  Widgets (accept from job / UI)
# --------------------------------
dbutils.widgets.text("table_names", "")
dbutils.widgets.text("source_schema", "bronze")
dbutils.widgets.text("metadata_schema", "dq_metadata")

selected_tables = [t.strip() for t in dbutils.widgets.get("table_names").split(",") if t.strip()]
source_schema = dbutils.widgets.get("source_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")

print("Running Column Classification for tables:", selected_tables)
print("Source Schema:", source_schema)
print("metadata_schema:", metadata_schema)

# COMMAND ----------

# --------------------------------
#  Helper: Extract pure JSON array from LLM response
# --------------------------------
def safe_extract_json_array(text):
    try:
        if "```json" in text:
            text = text.split("```json")[-1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].strip()
        matches = re.findall(r"\[.*?\]", text.strip(), re.DOTALL)
        if matches:
            return json.loads(matches[0])
    except json.JSONDecodeError as e:
        logging.error(f" JSON decode failed: {e}")
    return None

# COMMAND ----------

# --------------------------------
#  Output Schema
# --------------------------------
label_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("label", StringType(), True)
])

# COMMAND ----------

# --------------------------------
#  Collect all labels
# --------------------------------
label_rows = []

for table in selected_tables:
    try:
        df = spark.table(f"{source_schema}.{table}").limit(50)
        schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}

        # Prompt
        prompt = f"""
        You are a metadata labeling assistant.

        For the given table schema, assign exactly ONE most meaningful label to each column.
        Choose the best label for each column from the following options:
        ["primary_key", "pii", "timestamp", "measure", "category"]

        Respond ONLY with a valid JSON array.
        Do NOT include markdown or explanations.

        Example:
        [
          {{"column_name": "id", "label": "primary_key"}},
          {{"column_name": "amount_eur", "label": "measure"}},
          {{"column_name": "created_at", "label": "timestamp"}}
        ]

        Schema:
        {json.dumps(schema)}
        """

        # Query LLM
        response = client.predict(
            endpoint=llm_endpoint,
            inputs={"messages": [{"role": "user", "content": prompt}]}
        )

        content = response["choices"][0]["message"]["content"]
        tags = safe_extract_json_array(content)

        if tags:
            for item in tags:
                label_rows.append({
                    "table_name": table,
                    "column_name": item.get("column_name"),
                    "label": item.get("label")
                })
        else:
            logging.warning(f" No usable labels found for {table}")
            print(f" Raw LLM output for {table}:\n{content}")

    except Exception as e:
        logging.error(f" Labeling failed for {table}: {str(e)}")

# COMMAND ----------

# --------------------------------
#  Save Results
# --------------------------------
if label_rows:
    label_df = spark.createDataFrame(label_rows, label_schema)
    label_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{metadata_schema}.column_metadata_labels_silverlayer")
    print(f"  Labels saved to {metadata_schema}.column_metadata_labels_silverlayer")
else:
    print("  No labels to save.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_metadata.column_metadata_labels_silverlayer;