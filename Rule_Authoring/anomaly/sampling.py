"""
# Copied from lines 42-77 of AutoDQ/07_Anomaly Detection.py
"""
# Databricks notebook source
MAX_SAMPLE_ROWS = 10000   # cap rows sent to AI
SAMPLE_FRACTION = 0.01    # default fraction (1%)

def dynamic_sample(df):
    total_count = df.count()
    if total_count == 0:
        return df.limit(0)

    fraction = SAMPLE_FRACTION
    approx_size = int(total_count * fraction)
    if approx_size > MAX_SAMPLE_ROWS:
        fraction = MAX_SAMPLE_ROWS / total_count

    return df.sample(withReplacement=False, fraction=fraction, seed=42).limit(MAX_SAMPLE_ROWS)


