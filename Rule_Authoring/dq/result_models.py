"""
# Copied from lines 405-418 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
# Databricks notebook source
from pyspark.sql.types import StringType, StructType, StructField

validation_schema_struct = StructType([
    StructField("Table", StringType(), True),
    StructField("Column", StringType(), True),
    StructField("Rule", StringType(), True),
    StructField("Rule_Display_Name", StringType(), True),
    StructField("Rule_Description", StringType(), True),
    StructField("Metric", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Failure_Type", StringType(), True),
    StructField("Failed_Value", StringType(), True),
    StructField("Failed_Row_ID", StringType(), True),
    StructField("Execution_ID", StringType(), True),
    StructField("Run_Timestamp", StringType(), True)
])


