"""
# Copied from lines 155-265 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
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

"""
# Copied from lines 338-360 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
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

"""
# Copied from lines 383-391 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
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


