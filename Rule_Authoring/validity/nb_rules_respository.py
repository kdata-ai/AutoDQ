# Copied from 03_define_rules.py lines 7-76
from pyspark.sql.types import StringType, NumericType

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

# Copied from GX_Rule_Metadata(Rule Details).py lines 1-193
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Define the metadata with ALL rules
rule_metadata = [
    # (rule, rule_name, rule_description, sql_expression, dq_dimension)
    (
        "expect_column_to_exist",
        "Column Must Be Present",
        "Ensures the specified column exists in the dataset.",
        "CASE WHEN column_name IS NOT NULL THEN TRUE ELSE FALSE END",
        "Completeness"
    ),
    (
        "expect_column_values_to_not_be_null",
        "No Missing Values Allowed",
        "Ensures there are no missing or null values in the column.",
        "column_name IS NOT NULL",
        "Completeness"
    ),
    (
        "expect_column_values_to_be_unique",
        "No Duplicate Values",
        "Checks that values in the column are unique with no duplicates.",
        "COUNT(column_name) = COUNT(DISTINCT column_name)",
        "Uniqueness"
    ),
    (
        "expect_column_values_to_be_in_type_list",
        "Consistent Data Type",
        "Validates that values match the expected data type.",
        "CAST(column_name AS EXPECTED_TYPE)",
        "Validity"
    ),
    (
        "expect_column_values_to_be_in_set",
        "Value Must Be from Approved List",
        "Ensures that values belong to a predefined set.",
        "column_name IN ('APPROVED_VAL1','APPROVED_VAL2')",
        "Validity"
    ),
    (
        "expect_column_most_common_value_to_be_in_set",
        "Most Frequent Values Approved",
        "Checks that the most frequent value in the column is from an approved list.",
        "MODE(column_name) IN ('APPROVED_VAL1','APPROVED_VAL2')",
        "Validity"
    ),
    (
        "expect_column_proportion_of_unique_values_to_be_between",
        "Required Level of Uniqueness",
        "Validates that the proportion of unique values in the column falls within a defined range.",
        "(COUNT(DISTINCT column_name) / COUNT(*)) BETWEEN LOWER_BOUND AND UPPER_BOUND",
        "Uniqueness"
    ),
    (
        "expect_column_distinct_values_to_be_in_set",
        "Distinct Values from Approved List",
        "Ensures that all distinct values are within the approved list.",
        "ALL DISTINCT column_name IN ('APPROVED_VAL1','APPROVED_VAL2')",
        "Uniqueness"
    ),
    (
        "expect_column_value_lengths_to_be_between",
        "Field Length Within Limits",
        "Validates that the length of each column value is within defined limits.",
        "LENGTH(column_name) BETWEEN MIN_LEN AND MAX_LEN",
        "Validity"
    ),
    (
        "expect_column_values_to_match_regex",
        "Format Must Match Expected Pattern",
        "Checks that all values follow the required regex pattern.",
        "column_name RLIKE 'YOUR_REGEX_PATTERN'",
        "Accuracy"
    ),
    (
        "expect_column_values_to_not_match_regex",
        "Format Must Not Match Restricted Pattern",
        "Ensures that column values do not match restricted regex patterns.",
        "NOT (column_name RLIKE 'RESTRICTED_PATTERN')",
        "Accuracy"
    ),
    (
        "expect_column_values_to_be_dateutil_parseable",
        "Values Must Be Valid Dates",
        "Validates that all column values can be parsed as valid dates.",
        "TO_DATE(column_name, 'yyyy-MM-dd') IS NOT NULL",
        "Accuracy"
    ),
    (
        "expect_column_values_to_be_between",
        "Values Within Business Range",
        "Ensures that all values in the column fall within a defined numeric range.",
        "column_name BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_min_to_be_between",
        "Minimum Value Within Limits",
        "Checks that the minimum column value lies within specified limits.",
        "MIN(column_name) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_max_to_be_between",
        "Maximum Value Within Limits",
        "Checks that the maximum column value lies within specified limits.",
        "MAX(column_name) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_mean_to_be_between",
        "Average Value Within Range",
        "Validates that the average (mean) of column values is within the defined range.",
        "AVG(column_name) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_sum_to_be_between",
        "Total Value Within Range",
        "Ensures that the sum of column values falls within specified boundaries.",
        "SUM(column_name) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_values_to_be_increasing",
        "Values Should Increase",
        "Validates that values increase sequentially without decreases.",
        "column_name = SORTED_ASC(column_name)",
        "Consistency"
    ),
    (
        "expect_column_values_to_be_decreasing",
        "Values Should Decrease",
        "Validates that values decrease sequentially without increases.",
        "column_name = SORTED_DESC(column_name)",
        "Consistency"
    ),
    (
        "expect_column_quantile_values_to_be_between",
        "Percentiles Within Range",
        "Checks that column percentile values fall within limits.",
        "QUANTILE(column_name, P) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_stdev_to_be_between",
        "Expected Variability (Standard Deviation)",
        "Ensures that the standard deviation of values is within the expected range.",
        "STDDEV(column_name) BETWEEN MIN_VAL AND MAX_VAL",
        "Validity"
    ),
    (
        "expect_column_value_z_scores_to_be_less_than",
        "No Extreme Outliers",
        "Checks that z-scores remain below the defined threshold to avoid extreme outliers.",
        "ABS((column_name - AVG(column_name))/STDDEV(column_name)) < THRESHOLD",
        "Accuracy"
    ),
    (
        "expect_column_kl_divergence_to_be_less_than",
        "Distribution Matches Expected",
        "Validates that the distribution of column values matches the expected distribution using KL divergence.",
        "KL_DIVERGENCE(column_name, EXPECTED_DISTRIBUTION) < THRESHOLD",
        "Accuracy"
    ),
    (
        "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
        "No Significant Distribution Difference",
        "Ensures that the distribution of values does not significantly differ from the expected distribution using the KS test.",
        "KS_TEST(column_name, EXPECTED_DISTRIBUTION) > THRESHOLD",
        "Accuracy"
    )
]

# Define column names
columns = ["rule", "rule_name", "rule_description", "sql_expression", "dq_dimension"]

# Create Spark DataFrame
metadata_df = spark.createDataFrame(rule_metadata, columns)

# Save the DataFrame as a Delta table
metadata_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("dq_metadata.silver_layer_rule_metadata_AutoDQ")

print(" Metadata Table Created with ALL rules.")


# COMMAND ----------
# MAGIC %sql
# MAGIC Select * from dq_metadata.silver_layer_rule_metadata_AutoDQ
# MAGIC

