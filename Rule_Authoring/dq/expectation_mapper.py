"""
# Copied from lines 100-151 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
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


