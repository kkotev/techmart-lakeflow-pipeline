# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Build and test 
- No local test suite yet; validate by running the pipeline in a Databricks workspace
- Notebooks are imported via Databricks Repos or Asset Bundles, not run locally

# Stack 
- Lakeflow Declarative Pipelines (from pyspark import pipelines as dp)
- Notebooks are Databricks source format — keep # Databricks notebook source and # COMMAND ---------- markers intact

# Layout
- notebooks/01_data_exploration.py — source profiling only, no pipeline logic
- notebooks/02_sdp_pipeline.py — all transforms, bronze → silver → gold
- notebooks/03_reports.py — SDK deployment, metric view, YoY queries

# Conventions
- Every @dp.table needs name and comment
- Every source gets a <table>_quarantine step before downstream use — no exceptions for "clean" sources
- SCD1 for orders used for dedup, SCD2 for customers/products — don't swap
- Surrogate keys: xxhash64(business_key, __START_AT), always
- Fact-to-dim joins are temporal as-of joins (order_date BETWEEN valid_from AND valid_to), never a plain equi-join
- Unmatched dim keys resolve to -1, not NULL
- New metrics go in the YAML metric view (03_reports.py), not as one-off queries

# Gotchas
- Bronze functions stamp source_file_name, source_file_modification_timestamp, ingestion_timestamp — match this for any new source
- Quarantined rows go to published silver_<table>_bad, never silently dropped
- catalog, schema, raw_data_path come from Spark Config at pipeline creation, not hardcoded in notebook logic