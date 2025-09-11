# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kdataai.bronze_testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kdataai.silver_testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kdataai.gold_testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kdataai.dq_metadata_testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kdataai.dq_validation_testing;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dq_validation_testing.user_generated_rule_saved_data_for_dashboard (
# MAGIC     Column STRING,
# MAGIC     Execution_ID STRING,
# MAGIC     Failed_Row_ID STRING,
# MAGIC     Failed_Value STRING,
# MAGIC     Failure_Type STRING,
# MAGIC     Metric STRING,
# MAGIC     Rule STRING,
# MAGIC     Rule_Display_Name STRING,
# MAGIC     Run_Timestamp STRING,
# MAGIC     Status STRING,
# MAGIC     Table STRING,
# MAGIC     Rule_Description STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC