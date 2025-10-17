"""
# Copied from lines 336-376 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
# Databricks notebook source
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


#testing


