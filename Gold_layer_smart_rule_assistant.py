# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import uuid
import mlflow.deployments
import json
import re
from datetime import datetime

# COMMAND ----------

# --------------------------------
#  Config
# --------------------------------
source_schema = "silver"    
llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

spark = SparkSession.builder.getOrCreate()
client = mlflow.deployments.get_deploy_client("databricks")

# COMMAND ----------

# --------------------------------
#  Step 1: Get User Input (widget)
# --------------------------------
dbutils.widgets.text("natural_language_rule", "Enter Rule Here ..")
user_input = dbutils.widgets.get("natural_language_rule").strip()

if not user_input:
    raise ValueError("❌ Please provide a valid natural language rule.")

# COMMAND ----------

# --------------------------------
#  Step 2: Discover Tables + Extract Columns
# --------------------------------
silver_tables = [
    t.name for t in spark.catalog.listTables(source_schema) if not t.isTemporary
]

print(f"\nTables in `{source_schema}`:")
all_columns = set()
table_column_map = {}

for table in silver_tables:
    try:
        df = spark.table(f"{source_schema}.{table}")
        table_column_map[table] = df.columns
        all_columns.update(df.columns)
        sample_row = (
            df.limit(1).toPandas().to_dict(orient="records")[0] if df.count() > 0 else {}
        )
        print(f"\n {table} → Columns: {df.columns}")
        if sample_row:
            print(f"--> Sample Values: {sample_row}")
    except Exception as e:
        print(f" Failed to preview {table}: {e}")

column_list_str = ", ".join(sorted(all_columns))


# COMMAND ----------

# --------------------------------
#  Step 3: Generate Rule via LLM
# --------------------------------
prompt = f"""
You are a data quality rule generator.

Available column names: {column_list_str}

Based on the input: "{user_input}"

Generate a rule that finds bad data (rows with issues). Use the best matching actual column from the list.

Do NOT use:
- Subqueries
- GROUP BY
- JOINs
- Comparisons between two columns (e.g., email = customer_id)

Only use direct WHERE conditions comparing a column to a fixed value or checking for NULLs or emptiness.

Return ONLY a minified JSON object with these keys:
- rule: SQL-compatible WHERE clause (e.g., "email IS NULL OR email = ''")
- rule_display_name: short business-friendly name
- rule_description: a formal business rule (at least 10 words)
- rule_expression: full SQL like "SELECT * FROM <table> WHERE email IS NULL OR email = ''"

Do NOT invent column names. Use only from the list.
"""

try:
    response = client.predict(
        endpoint=llm_endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": "You are a strict JSON-only assistant that generates SQL-based validation rules."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2
        }
    )

    if response and "choices" in response:
        raw_output = response["choices"][0]["message"]["content"]
        print("\n----> Raw LLM Output:\n", raw_output)

        result = json.loads(raw_output)
        rule_expr = result["rule"]
        rule_display = result["rule_display_name"]
        rule_desc = result["rule_description"]
        rule_expr_sql_template = result["rule_expression"]

        print("\n Parsed Rule:", rule_expr)
    else:
        raise ValueError("❌ No valid LLM response returned.")

except Exception as e:
    raise ValueError(f"❌ LLM call failed: {e}")

# COMMAND ----------

# --------------------------------
#  Step 4: Detect Required Columns
# --------------------------------
required_columns = [col for col in all_columns if re.search(rf"\b{col}\b", rule_expr)]
if not required_columns:
    raise ValueError(" Could not detect any valid column in the rule. Please rephrase your input.")

print(f"\n Required columns used in rule: {required_columns}")

# COMMAND ----------

# --------------------------------
#  Step 5: Apply Rule and Collect Logs
# --------------------------------
execution_id = str(uuid.uuid4())
timestamp = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
log_entries, participated_tables = [], []

# Extract WHERE clause only
where_clause = rule_expr_sql_template.replace("SELECT * FROM table WHERE ", "")

for table in silver_tables:
    try:
        df = spark.table(f"{source_schema}.{table}")
        if not all(col in df.columns for col in required_columns):
            continue

        failed_df = df.filter(where_clause).withColumn("row_index", monotonically_increasing_id())
        failed_count = failed_df.count()
        participated_tables.append(table)

        print(f"\n📊 Table: {table} | Columns Checked: {required_columns}")
        print(f"🔍 WHERE: {where_clause}")
        if failed_count > 0:
            print(f" Rule Failed on {failed_count} rows")
            for row in failed_df.collect():
                for col in required_columns:
                    log_entries.append({
                        "Column": col,
                        "Execution_ID": execution_id,
                        "Failed_Row_ID": int(row["row_index"]) + 1,
                        "Failed_Value": str(row[col]) if row[col] is not None else None,
                        "Failure_Type": "Rule Failed",
                        "Metric": "User Generated Rule",
                        "Rule": rule_expr,
                        "Rule_Description": rule_desc,
                        "Rule_Display_Name": rule_display,
                        "Run_Timestamp": timestamp,
                        "Status": "Failed",
                        "Table": table,
                    })
        else:
            print(" Rule Passed (No Violations)")
            log_entries.append({
                "Column": required_columns[0],
                "Execution_ID": execution_id,
                "Failed_Row_ID": None,
                "Failed_Value": None,
                "Failure_Type": None,
                "Metric": "User Generated Rule",
                "Rule": rule_expr,
                "Rule_Description": rule_desc,
                "Rule_Display_Name": rule_display,
                "Run_Timestamp": timestamp,
                "Status": "Passed",
                "Table": table,
            })

    except Exception as e:
        print(f"⚠ Error applying rule to {table}: {e}")

# COMMAND ----------

# --------------------------------
#  Step 6: Save Logs
# --------------------------------
result_table = "user_generated_rule_for_logging"
if log_entries:
    schema = StructType([
    StructField("Column", StringType(), True),
    StructField("Execution_ID", StringType(), True),
    StructField("Failed_Row_ID", StringType(), True),  
    StructField("Failed_Value", StringType(), True),
    StructField("Failure_Type", StringType(), True),
    StructField("Metric", StringType(), True),
    StructField("Rule", StringType(), True),
    StructField("Rule_Description", StringType(), True),
    StructField("Rule_Display_Name", StringType(), True),
    StructField("Run_Timestamp", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Table", StringType(), True)
])


    log_df = spark.createDataFrame(log_entries, schema=schema)
    ordered_cols = [f.name for f in schema.fields]
    log_df = log_df.select(ordered_cols)

    log_df.write.mode("append") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .saveAsTable(f"gold.{result_table}")

    print(f"\n Validation log written to `{source_schema}.{result_table}` with {len(log_entries)} entries.")
    print(f" Participated Tables: {participated_tables}")
else:
    print("\n⚠️ No validation results to write. Please check the rule or try a different input.")