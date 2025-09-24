"""
# Copied from lines 134-158 of AutoDQ/05_Schema Inference.py
"""
# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

empty_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("actual_data_type", StringType(), True),
    StructField("inferred_data_type", StringType(), True),
    StructField("is_matched", BooleanType(), True)
])


