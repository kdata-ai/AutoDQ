# Databricks notebook source
# MAGIC %md
# MAGIC # Define_rules

# COMMAND ----------

from pyspark.sql.types import StringType, NumericType

# COMMAND ----------

# -------------------------------
# 1. Rule Metadata
# -------------------------------
rule_to_metric = {
    "expect_column_to_exist": "Completeness",
    "expect_column_values_to_be_in_type_list": "Validity",
    "expect_column_values_to_be_in_set": "Validity",
    "expect_column_distinct_values_to_be_in_set": "Uniqueness"
}

rule_display_names = {
    "expect_column_to_exist": "Column Must Be Present",
    "expect_column_values_to_be_in_type_list": "Consistent Data Type",
    "expect_column_values_to_be_in_set": "Value Must Be from Approved List",
    "expect_column_distinct_values_to_be_in_set": "Distinct Values from Approved List"
}
rule_descriptions = {
    "expect_column_to_exist": "Ensures the specified column exists in the dataset.",
    "expect_column_values_to_be_in_type_list": "Validates that column values match the expected data type.",
    "expect_column_values_to_be_in_set": "Ensures values are part of a predefined set.",
    "expect_column_distinct_values_to_be_in_set": "Checks that all distinct values are from an approved set."
}

# -------------------------------
# 2. Rule Fetcher 
# -------------------------------
rule_cache = {}

ALLOWED_RULES = list(rule_to_metric.keys())

def get_rules(col_name, col_type):
    if col_name in rule_cache:
        return rule_cache[col_name]
    rules = ALLOWED_RULES  
    rule_cache[col_name] = rules
    return rules

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

def enrich_with_rule_metadata(df, rule_col="Rule_Name"):
    for mapping_name, mapping_dict in {
        "Rule_Display_Name": rule_display_names,
        "DQ_Metric": rule_to_metric,
        "Rule_Description": rule_descriptions  
    }.items():
        mapping_expr = F.create_map([lit(x) for pair in mapping_dict.items() for x in pair])
        df = df.withColumn(mapping_name, mapping_expr.getItem(F.col(rule_col)))
    return df

# COMMAND ----------

def enrich_with_rule_metadata(df, rule_col="Rule_Name"):
    df = df.copy()
    df["Rule_Display_Name"] = df[rule_col].map(rule_display_names)
    df["DQ_Metric"] = df[rule_col].map(rule_to_metric)
    df["Rule_Description"] = df[rule_col].map(rule_descriptions)
    return df

# COMMAND ----------

# Clear all cached data from Spark (DataFrames and tables)
spark.catalog.clearCache()
