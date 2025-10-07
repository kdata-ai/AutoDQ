"""
# Copied from lines 51-55 of AutoDQ/06_Smart Data Cleaning.py
"""
# Databricks notebook source
import re

def clean_ai_response(raw):
    cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
    cleaned = re.sub(r"\n?```$", "", cleaned).strip()
    return cleaned

