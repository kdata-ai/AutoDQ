"""
# Copied from lines 1-36 of AutoDQ/04_Great_Expectation_ New Logic.py
"""
# Databricks notebook source
# 🔧 Spark Configuration for Databricks on GCP

# ========== Core Execution ==========
spark.conf.set("spark.sql.shuffle.partitions", "256")                # Higher shuffle partitions for GCP clusters
spark.conf.set("spark.sql.adaptive.enabled", "true")                 # Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# ========== Delta Optimizations ==========
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # Needed if VACUUM with <7d retain

# ========== Storage & GCS Connector ==========
spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.fs.gs.metadata.cache.enable", "true")   # Metadata caching for GCS

# ========== DataFrame / Execution ==========
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable Arrow for PySpark <-> Pandas
spark.conf.set("spark.sql.broadcastTimeout", "900")                  # Longer timeout for big joins on GCP
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")     # 128 MB partitions
spark.conf.set("spark.sql.files.openCostInBytes", "67108864")        # 64 MB → improves GCS listing perf

# ========== Parquet / ORC ==========
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")

# ========== Cleaner / GC ==========
# spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")  # Cannot modify this config
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")

print("Spark configuration applied for GCP cluster")


