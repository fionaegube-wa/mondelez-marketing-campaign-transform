#!/usr/bin/env python3
"""
Mondelez Marketing Campaign Transformation

PURPOSE
-------
Read raw campaign performance files (CSV/Parquet) from local disk or GCS,
clean and validate them, compute KPIs (CTR, CPC, CPM), produce curated
row-level data and daily aggregates, and write outputs back as *partitioned*
Parquet. Invalid records are routed to a dead-letter JSONL.

EXAMPLES
--------
Local:
  python mondelez_marketing_transform.py \
    --input "./data/raw/2025-10-23/*.csv" \
    --output_prefix "./data/curated/campaigns" \
    --badrows_prefix "./data/badrows/campaigns" \
    --aggregate_dims "channel,country"

GCS:
  python mondelez_marketing_transform.py \
    --input "gs://mondelez-marketing/raw/2025-10-23/*.parquet" \
    --output_prefix "gs://mondelez-marketing/curated/campaigns" \
    --badrows_prefix "gs://mondelez-marketing/badrows/campaigns" \
    --aggregate_dims "channel,country"
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
import gcsfs  # noqa: F401  # (import registers the gs:// protocol for fsspec)

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("campaign-transform")


# ------------------------------------------------------------------------------
# Configuration dataclass
# ------------------------------------------------------------------------------
@dataclass
class Config:
    """
    Runtime configuration for the transformation.

    input_glob:      Glob path (local or GCS) for input files (CSV or Parquet).
    output_prefix:   Prefix folder (local or GCS) for *curated* Parquet output.
    badrows_prefix:  Prefix folder (local or GCS) for dead-letter JSONL.
    partition_col:   Column to partition outputs by (typically 'date').
    write_compression: Parquet compression codec.
    aggregate_dims:  Dimensions used for daily aggregation (e.g., channel,country).
    required_fields: Fields that must be present and non-empty for a row to be valid.
    """
    input_glob: str
    output_prefix: str
    badrows_prefix: str
    partition_col: str = "date"
    write_compression: str = "snappy"
    aggregate_dims: Tuple[str, ...] = ("channel",)
    required_fields: Tuple[str, ...] = (
        "date", "campaign_id", "campaign_name", "channel",
        "impressions", "clicks", "spend"
    )


# ------------------------------------------------------------------------------
# Path & Filesystem helpers
# ------------------------------------------------------------------------------
def _is_gcs_path(path: str) -> bool:
    """Return True if the path is a GCS URI."""
    return path.startswith("gs://")


def _fs_for_path(path: str):
    """
    Return an fsspec filesystem instance for the given path.
    - gs://... -> GCS filesystem
    - everything else -> local filesystem
    """
    return fsspec.filesystem("gcs") if _is_gcs_path(path) else fsspec.filesystem("file")


def _glob_paths(glob_uri: str) -> List[str]:
    """Expand a glob (local or GCS) into a concrete list of file URIs."""
    fs = _fs_for_path(glob_uri)
    return fs.glob(glob_uri)


# ------------------------------------------------------------------------------
# Reading inputs
# ------------------------------------------------------------------------------
def _read_any(paths: List[str]) -> pd.DataFrame:
    """
    Read a list of CSV or Parquet files (local or GCS) into a single DataFrame.
    Adds a __source_file column for lineage/debugging.
    Skips unsupported types gracefully.
    """
    if not paths:
        raise FileNotFoundError("No matching input files.")

    frames: List[pd.DataFrame] = []
    for p in paths:
        fs = _fs_for_path(p)
        with fs.open(p, "rb") as f:
            if p.lower().endswith(".parquet"):
                df = pd.read_parquet(f)
            elif p.lower().endswith(".csv") or p.lower().endswith(".csv.gz"):
                df = pd.read_csv(f, low_memory=False)
            else:
                log.warning("Skipping unsupported file type: %s", p)
                continue
            df["__source_file"] = p  # lineage column
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d rows from %d files", len(out), len(frames))
    return out


# ------------------------------------------------------------------------------
# Cleaning & normalization
# ------------------------------------------------------------------------------
_STD_STR_COLS = ("campaign_id", "campaign_name", "channel", "country", "device", "creative_id")


def _clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize string columns: strip, collapse whitespace, remove control chars.
    This prevents duplicate keys due to whitespace or hidden characters.
    """
    def norm(x):
        if pd.isna(x):
            return None
        s = str(x)
        s = re.sub(r"[\x00-\x1F\x7F]", " ", s)  # control chars -> spaces
        s = re.sub(r"\s+", " ", s).strip()      # collapse whitespace, trim
        return s if s else None

    for c in _STD_STR_COLS:
        if c in df.columns:
            df[c] = df[c].map(norm)
    return df


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce core column types with 'best effort' conversions.
    - date -> YYYY-MM-DD (string)
    - impressions, clicks -> float (but expected to be integer-ish)
    - spend -> float
    """
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")

    for c in ("impressions", "clicks"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    if "spend" in df.columns:
        df["spend"] = pd.to_numeric(df["spend"], errors="coerce").astype("float64")

    return df


def _apply_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonize platform-specific column names to our canonical schema.
    Example: 'adset_name' -> 'campaign_name', 'spend_usd' -> 'spend'.
    """
    alias_map = {
        "campaign": "campaign_name",
        "adset_name": "campaign_name",
        "channel_name": "channel",
        "spend_usd": "spend",
        "date_time": "date",
    }
    for src, dst in alias_map.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    return df


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate by a business key: (date, campaign_id, channel, creative_id).
    Keep the 'most complete' row; heuristic:
      - if 'updated_at' exists, keep latest
      - else keep the row with highest impressions
    """
    keys = [c for c in ("date", "campaign_id", "channel", "creative_id") if c in df.columns]
    if not keys:
        return df.drop_duplicates()

    sort_cols = []
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        sort_cols = ["updated_at"]
    elif "impressions" in df.columns:
        sort_cols = ["impressions"]

    if sort_cols:
        return (
            df.sort_values(keys + sort_cols, ascending=[True]*len(keys) + [False])
              .drop_duplicates(subset=keys, keep="first")
        )
    return df.drop_duplicates(subset=keys, keep="last")


# ------------------------------------------------------------------------------
# Validation & splitting into good/bad
# ------------------------------------------------------------------------------
def _validate_and_split(df: pd.DataFrame, required: Tuple[str, ...]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply basic business rules:
      - Required fields must be present and non-empty.
      - impressions, clicks, spend must be non-negative.
      - clicks <= impressions.
      - impressions/clicks should be integer-like (0, 1, 2, ...).

    Returns
    -------
    good : DataFrame  -> valid rows
    bad  : DataFrame  -> invalid rows with rejection_reason column
    """
    work = df.copy()

    # Required fields present and non-empty
    mask_req = np.ones(len(work), dtype=bool)
    for col in required:
        if col not in work.columns:
            work[col] = np.nan
        mask_req &= work[col].notna() & (work[col] != "")

    # Non-negative numerics
    def nonneg(col: str):
        return work[col].fillna(0) >= 0 if col in work.columns else True

    mask_nonneg = nonneg("impressions") & nonneg("clicks") & nonneg("spend")

    # Integer-like counts for impressions/clicks (after coercion to float)
    mask_counts_intlike = True
    for c in ("impressions", "clicks"):
        if c in work.columns:
            s = work[c]
            mask_counts_intlike = mask_counts_intlike & (np.isfinite(s)) & (np.floor(s) == s)

    # clicks must not exceed impressions
    mask_bounds = True
    if "impressions" in work.columns and "clicks" in work.columns:
        mask_bounds = work["clicks"] <= work["impressions"]

    bad_mask = ~(mask_req & mask_nonneg & mask_counts_intlike & mask_bounds)
    good = work[~bad_mask].copy()
    bad = work[bad_mask].copy()

    # Human-readable failure reasons for debugging
    reasons = []
    for i in bad.index:
        r = []
        if not mask_req.loc[i]: r.append("missing_required")
        if not mask_nonneg.loc[i]: r.append("negative_values")
        if not (mask_counts_intlike.loc[i] if hasattr(mask_counts_intlike, "loc") else True):
            r.append("counts_not_integer")
        if ("clicks" in work.columns and "impressions" in work.columns
                and work.loc[i, "clicks"] > work.loc[i, "impressions"]):
            r.append("clicks_gt_impressions")
        reasons.append(",".join(r) if r else "invalid")
    if not bad.empty:
        bad["rejection_reason"] = reasons

    return good, bad


# ------------------------------------------------------------------------------
# KPI derivation & aggregation
# ------------------------------------------------------------------------------
def _derive_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute standard marketing metrics:
      - CTR = clicks / impressions
      - CPC = spend / clicks
      - CPM = (spend / impressions) * 1000
    Uses safe divisions (avoid div-by-zero).
    """
    if {"clicks", "impressions"}.issubset(df.columns):
        df["ctr"] = np.where(df["impressions"] > 0, df["clicks"] / df["impressions"], 0.0)

    if {"spend", "clicks"}.issubset(df.columns):
        df["cpc"] = np.where(df["clicks"] > 0, df["spend"] / df["clicks"], np.nan)

    if {"spend", "impressions"}.issubset(df.columns):
        df["cpm"] = np.where(df["impressions"] > 0, (df["spend"] / df["impressions"]) * 1000.0, np.nan)

    return df


def _aggregate(df: pd.DataFrame, dims: Tuple[str, ...]) -> pd.DataFrame:
    """
    Daily aggregates by requested dimensions.
    Example dims: ('channel', 'country') -> aggregates per (date, channel, country).
    """
    group_cols = ["date"] + [d for d in dims if d in df.columns]
    agg = (
        df.groupby(group_cols, dropna=False)
          .agg(impressions=("impressions", "sum"),
               clicks=("clicks", "sum"),
               spend=("spend", "sum"))
          .reset_index()
    )
    agg = _derive_kpis(agg)
    return agg


# ------------------------------------------------------------------------------
# Writing outputs
# ------------------------------------------------------------------------------
def _write_parquet_partitioned(df: pd.DataFrame, prefix: str, partition_col: str, compression: str = "snappy"):
    """
    Write a DataFrame to partitioned Parquet, one folder per partition value:
      {prefix}/{partition_col}=YYYY-MM-DD/part-<timestamp>.parquet
    This layout plays well with BigQuery external tables or ingestion.
    """
    if df.empty:
        log.info("No rows to write for %s", prefix)
        return
    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' missing in dataframe.")

    fs = _fs_for_path(prefix)
    for part_val, grp in df.groupby(partition_col):
        part_str = str(part_val)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        out_path = f"{prefix}/{partition_col}={part_str}/part-{ts}.parquet"
        table = pa.Table.from_pandas(grp, preserve_index=False)
        with fs.open(out_path, "wb") as f:
            pq.write_table(table, f, compression=compression)
        log.info("Wrote %d rows -> %s", len(grp), out_path)


def _write_badrows_jsonl(bad: pd.DataFrame, prefix: str):
    """
    Write rejected rows as JSON Lines with a 'rejection_reason' column.
    Keeps bad data visible for triage without polluting curated data.
    """
    if bad.empty:
        return

    fs = _fs_for_path(prefix)
    out_path = f"{prefix}/bad-{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
    with fs.open(out_path, "wb") as f:
        for _, row in bad.iterrows():
            # Convert numpy types to JSON-friendly values
            rec = {k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v))
                   for k, v in row.to_dict().items()}
            line = json.dumps(rec, default=str).encode("utf-8")
            f.write(line + b"\n")
    log.warning("Wrote %d bad rows -> %s", len(bad), out_path)


# ------------------------------------------------------------------------------
# Orchestration: one function to run the full transform
# ------------------------------------------------------------------------------
def transform(cfg: Config):
    """
    End-to-end transformation driver:
      1) Read inputs
      2) Clean/normalize and harmonize schemas
      3) Deduplicate
      4) Validate & split into good/bad
      5) Derive KPIs
      6) Aggregate
      7) Write curated + aggregates + bad rows
    """
    # 1) Read
    paths = _glob_paths(cfg.input_glob)
    log.info("Found %d files", len(paths))
    raw = _read_any(paths)
    if raw.empty:
        log.warning("No data to process.")
        return

    # 2) Clean/normalize + alias mapping
    df = _clean_strings(raw)
    df = _coerce_types(df)
    df = _apply_aliases(df)

    # 3) Deduplicate business keys
    df = _deduplicate(df)

    # 4) Validate & split
    good, bad = _validate_and_split(df, cfg.required_fields)

    # 5) KPIs
    good = _derive_kpis(good)

    # 6) Aggregates
    agg = _aggregate(good, cfg.aggregate_dims)

    # 7) Curated selection (keep only the important analytics columns)
    keep_cols = [
        "date", "campaign_id", "campaign_name", "channel",
        "country", "device", "creative_id",
        "impressions", "clicks", "spend",
        "ctr", "cpc", "cpm",
        "__source_file",
    ]
    curated = good[[c for c in keep_cols if c in good.columns]]

    # 8) Write outputs
    _write_parquet_partitioned(curated, cfg.output_prefix, cfg.partition_col, cfg.write_compression)

    # Write aggregates to a sibling folder (suffix "_agg")
    agg_prefix = f"{cfg.output_prefix}_agg"
    _write_parquet_partitioned(agg, agg_prefix, cfg.partition_col, cfg.write_compression)

    # 9) Dead-letter invalid rows
    _write_badrows_jsonl(bad, cfg.badrows_prefix)

    log.info("Done. curated=%d, aggregated=%d, bad=%d", len(curated), len(agg), len(bad))


# ------------------------------------------------------------------------------
# CLI: parse arguments into Config
# ------------------------------------------------------------------------------
def parse_args() -> Config:
    """
    Parse CLI args and return a Config object. Keep CLI minimal and explicit
    so this script is easy to call from local dev, cron, Composer, or Cloud Run.
    """
    ap = argparse.ArgumentParser(description="Transform marketing campaign files to curated Parquet.")
    ap.add_argument("--input", dest="input_glob", required=True, help="Input glob (gs://... or local).")
    ap.add_argument("--output_prefix", required=True, help="Output prefix (partitioned Parquet).")
    ap.add_argument("--badrows_prefix", required=True, help="Dead-letter JSONL output prefix.")
    ap.add_argument("--partition_col", default="date", help="Partition column (default: date).")
    ap.add_argument("--write_compression", default="snappy", choices=["snappy", "gzip", "zstd"],
                    help="Parquet compression codec.")
    ap.add_argument("--aggregate_dims", default="channel",
                    help="Comma-separated dims for aggregation (e.g., channel,country).")
    args = ap.parse_args()

    dims = tuple([d.strip() for d in args.aggregate_dims.split(",") if d.strip()])
    return Config(
        input_glob=args.input_glob,
        output_prefix=args.output_prefix.rstrip("/"),
        badrows_prefix=args.badrows_prefix.rstrip("/"),
        partition_col=args.partition_col,
        write_compression=args.write_compression,
        aggregate_dims=dims or ("channel",),
    )


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    cfg = parse_args()
    transform(cfg)
