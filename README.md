# Marketing Campaign Data Transformation

This repository contains a Python-based data transformation pipeline designed to clean, validate, and standardize **raw marketing campaign performance data**.  

It’s built to work seamlessly with **Google Cloud Storage (GCS)** or **local files**, making it easy to use in production workflows (e.g., Composer) or simple batch jobs.

---

## Overview

Many marketing teams receive campaign performance data from multiple platforms (e.g., Google Ads, Meta Ads, retail media) in inconsistent formats.  

This project provides a **reusable Python script** that:

- Reads daily campaign performance files (CSV or Parquet) from GCS or local storage.  
- Cleans and standardizes campaign fields (e.g., campaign ID, name, dates, spend, impressions, clicks).  
- Validates data (e.g., required fields, negative values, clicks vs. impressions).  
- Computes key metrics like CTR, CPC, and CPM.  
- Writes **clean partitioned Parquet files** ready for downstream loading into BigQuery or a data warehouse.  
- Routes bad or invalid records to a separate dead-letter file for debugging.

---

## Project Structure


- `marketing_transform.py` → main Python script that runs the transformation logic.  
- `data/raw/` → place a few CSV/Parquet files to test locally.  
- `data/curated/` and `data/badrows/` → automatically generated output folders.

---

## What the Script Does

Inside `marketing_transform.py`, the script performs the following steps:

1. **Read files** from a local folder or GCS bucket using `fsspec/gcsfs`.  
2. **Clean and normalize strings**, harmonize column names, and coerce data types.  
3. **Deduplicate** records based on business keys (e.g., `date`, `campaign_id`, `channel`).  
4. **Validate data**, splitting valid and invalid records.  
5. **Derive KPIs** like:
   - CTR = clicks / impressions  
   - CPC = spend / clicks  
   - CPM = (spend / impressions) × 1000
6. **Aggregate metrics** by date and channel (or other dimensions).  
7. **Write outputs** back as:
   - Partitioned Parquet files (clean data)
   - Aggregated metrics
   - Dead-letter JSONL for bad rows

---

## Installation

```bash
# Clone the repo
git clone https://github.com/<your-username>/marketing-campaign-transform.git
cd marketing-campaign-transform

# Create a virtual environment (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate  # (on Windows: .venv\Scripts\activate)

# Install dependencies
pip install -r requirements.txt

Usage
Run locally
python src/marketing_transform.py \
  --input "./data/raw/*.csv" \
  --output_prefix "./data/curated/campaigns" \
  --badrows_prefix "./data/badrows/campaigns" \
  --aggregate_dims "channel,country"

Run with GCS
python src/marketing_transform.py \
  --input "gs://your-bucket/raw/*.csv" \
  --output_prefix "gs://your-bucket/curated/campaigns" \
  --badrows_prefix "gs://your-bucket/badrows/campaigns" \
  --aggregate_dims "channel,country"


You can schedule this script with:

Cloud Composer (Airflow)

Cloud Run jobs

or a simple cron job for on-prem/local workflows.


