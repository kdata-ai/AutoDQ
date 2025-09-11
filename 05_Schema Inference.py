# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Inference Using LLM 

# COMMAND ----------

import json
import requests
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import mlflow.deployments

# COMMAND ----------

# Get Spark session, workspace host, and internal token
spark = SparkSession.builder.getOrCreate()
host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

LLM_ENDPOINT="databricks-meta-llama-3-3-70b-instruct"


api_url = f"/serving-endpoints/{LLM_ENDPOINT}/invocations"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# COMMAND ----------

# =====================================================
# 2. Widgets (accept from job / Streamlit)
# =====================================================
dbutils.widgets.text("table_names", "")
dbutils.widgets.text("schema_name", "bronze")
dbutils.widgets.text("metadata_schema", "dq_metadata")

selected_tables = [t.strip() for t in dbutils.widgets.get("table_names").split(",") if t.strip()]
source_schema = dbutils.widgets.get("schema_name")
metadata_schema = dbutils.widgets.get("metadata_schema")

print("Running Schema Inference for tables:", selected_tables)
print("Source Schema:", source_schema)
print("metadata_schema:", metadata_schema)

# COMMAND ----------

# =====================================================
# 3. Main logic
# =====================================================
final_results = []

for table in selected_tables:
    try:
        print(f"\n Processing: {table}")
        df = spark.table(f"{source_schema}.{table}")
        actual_schema = df.schema

        # Build prompt from schema
        schema_text = "\n".join([f"{field.name}: {field.dataType.simpleString()}" for field in actual_schema])
        prompt = f"""
You are a data expert. Based on the schema below, infer the most suitable data type for each column.
Return the result in pure JSON format like:
[
  {{"column_name": "column1", "inferred_data_type": "string"}},
  ...
]

Schema:
{schema_text}
"""

        # Call LLM endpoint
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 512
        }

        response = requests.post(
            host + api_url,
            headers=headers,
            data=json.dumps(payload)
        )

        print(f" Raw LLM response for `{table}`:\n{response.text[:500]}")
        response.raise_for_status()

        # Extract LLM content
        llm_content = response.json()["choices"][0]["message"]["content"]

        # Try to extract JSON block safely
        try:
            json_match = re.search(r"```(?:json)?\s*(\[\s*\{.*?\}\s*\])\s*```", llm_content, re.DOTALL)
            if json_match:
                llm_content = json_match.group(1)
            else:
                fallback_match = re.search(r"(\[\s*\{.*?\}\s*\])", llm_content, re.DOTALL)
                if fallback_match:
                    llm_content = fallback_match.group(1)
                else:
                    raise ValueError("No valid JSON array found in LLM response.")

            llm_output = json.loads(llm_content)

        except Exception as parse_error:
            print(f" JSON Parsing failed for table `{table}`: {str(parse_error)}")
            continue  # Skip this table and move on

        # Compare actual vs inferred schema
        for field in actual_schema:
            actual_col = field.name
            actual_type = field.dataType.simpleString()
            inferred_type = next(
                (entry["inferred_data_type"].lower()
                 for entry in llm_output
                 if entry["column_name"].lower() == actual_col.lower()),
                None
            )
            is_matched = (inferred_type == actual_type.lower())
            final_results.append((table, actual_col, actual_type, inferred_type, is_matched))

    except Exception as e:
        print(f" Error processing `{table}`: {str(e)}")

# COMMAND ----------

# =====================================================
# 4. Save final results
# =====================================================
# Define schema so empty DataFrame works too
empty_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("actual_data_type", StringType(), True),
    StructField("inferred_data_type", StringType(), True),
    StructField("is_matched", BooleanType(), True)
])

if final_results:
    result_df = spark.createDataFrame(
        final_results,
        ["table_name", "column_name", "actual_data_type", "inferred_data_type", "is_matched"]
    )
else:
    # Always overwrite with empty DF if no results
    result_df = spark.createDataFrame([], empty_schema)

# Overwrite table every run (no stale results)
result_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{metadata_schema}.inferred_schema_validation")

print(f"Results saved to '{metadata_schema}.inferred_schema_validation'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_metadata.inferred_schema_validation;