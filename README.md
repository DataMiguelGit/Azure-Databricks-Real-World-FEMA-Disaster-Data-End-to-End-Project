# Azure Databricks Real-World FEMA Disaster Data — End-to-End Project

A production-ready (learning-focused) data engineering pipeline that ingests FEMA public data into an Azure Lakehouse and serves curated Bronze/Silver/Gold datasets for analytics and ML consumption.

## Business Problem

Public FEMA datasets are available via APIs, but teams often struggle with:
- Inconsistent ingestion patterns (no run correlation / no replayability)
- Unclear “raw vs curated” boundaries
- Lack of data quality + quarantine
- No deterministic backfills
- Weak observability (no per-run metrics)

**Impact:** higher cloud costs and engineering churn due to repeated full reloads, non-repeatable outputs, and pipelines that degrade in performance as historical data accumulates.

This project builds a governed, replayable, and auditable dataset foundation so Data Analysts, BI, and Data Scientists can reliably answer questions and build models on top of FEMA disaster data.

## Who is this for?
- **Data Analysts / BI**: query-ready tables (Gold) and stable definitions.
- **Data Scientists / ML**: clean, standardized Silver + reproducible training slices.
- **Data Engineers**: reference implementation for ingestion → medallion → ops.

## Goals
- Provide raw, immutable landing files with strict run correlation (`load_id`).
- Build Bronze/Silver/Gold Delta tables with quality gates and quarantine.
- Enable deterministic backfills by `ingest_date` + `load_id` (reprocess from immutable landing batches).
- Provide per-run observability (row counts, quarantine counts, duration).

## Non-goals
- No dashboards / BI reports included.
- No ML model training included (only ML-ready datasets).
- No ADF ingestion (explicitly forbidden in this project).