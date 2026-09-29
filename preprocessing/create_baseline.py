"""
create_baseline.py - Phase 1: Baseline CSV Pipeline
===================================================
Loads train and test Dreaddit CSVs, combines them, validates schemas,
filters out/handles any malformed rows, and generates the standardized:
    data/baseline/baseline_stress_logs.csv

Crucial requirement: Original post text is NOT modified at this stage.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("CreateBaseline")

EXPECTED_COLUMNS = [
    "post_id",
    "subreddit",
    "social_timestamp",
    "text",
    "label",
    "confidence",
    "social_karma",
    "social_num_comments",
    "syntax_ari"
]

def load_and_combine(train_path: str, test_path: str) -> pd.DataFrame:
    """Loads train and test sets and performs vertical concatenation."""
    logger.info(f"Loading train dataset from: {train_path}")
    if not os.path.exists(train_path):
        raise FileNotFoundError(f"Train dataset file not found: {train_path}")
    df_train = pd.read_csv(train_path)
    logger.info(f"Loaded {len(df_train):,} train records.")

    logger.info(f"Loading test dataset from: {test_path}")
    if not os.path.exists(test_path):
        raise FileNotFoundError(f"Test dataset file not found: {test_path}")
    df_test = pd.read_csv(test_path)
    logger.info(f"Loaded {len(df_test):,} test records.")

    # Validate column presence before slicing
    for col in EXPECTED_COLUMNS:
        if col not in df_train.columns:
            raise KeyError(f"Expected column '{col}' missing from train dataset!")
        if col not in df_test.columns:
            raise KeyError(f"Expected column '{col}' missing from test dataset!")

    df_combined = pd.concat([df_train[EXPECTED_COLUMNS], df_test[EXPECTED_COLUMNS]], ignore_index=True)
    logger.info(f"Combined total records: {len(df_combined):,}")
    return df_combined

def validate_and_clean_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates data integrity and removes/handles malformed records:
      - Validates non-null primary attributes (post_id, text, subreddit, label, social_timestamp)
      - Verifies label is binary {0, 1}
      - Verifies confidence is numeric in range [0.0, 1.0]
      - Verifies timestamp is positive Unix epoch
      - Preserves original raw text exactly as-is
    """
    initial_count = len(df)
    logger.info("Executing row-level schema and integrity validation...")

    # 1. Null check on critical columns
    critical_cols = ["post_id", "subreddit", "social_timestamp", "text", "label"]
    null_mask = df[critical_cols].isnull().any(axis=1)
    if null_mask.sum() > 0:
        logger.warning(f"Dropping {null_mask.sum()} rows with null critical fields.")
        df = df[~null_mask]

    # 2. Type conversions & range validations
    # post_id & subreddit as string
    df["post_id"] = df["post_id"].astype(str).str.strip()
    df["subreddit"] = df["subreddit"].astype(str).str.strip().str.lower()
    
    # Remove rows with empty post_id or empty text
    empty_mask = (df["post_id"] == "") | (df["text"].astype(str).str.strip() == "")
    if empty_mask.sum() > 0:
        logger.warning(f"Dropping {empty_mask.sum()} rows with empty post_id or text.")
        df = df[~empty_mask]

    # 3. Label validation: binary {0, 1}
    df["label"] = pd.to_numeric(df["label"], errors="coerce")
    valid_label_mask = df["label"].isin([0, 1])
    if (~valid_label_mask).sum() > 0:
        logger.warning(f"Dropping {(~valid_label_mask).sum()} rows with invalid label values.")
        df = df[valid_label_mask]
    df["label"] = df["label"].astype(int)

    # 4. Confidence validation [0.0, 1.0]
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    valid_conf_mask = df["confidence"].between(0.0, 1.0, inclusive="both")
    if (~valid_conf_mask).sum() > 0:
        logger.warning(f"Dropping {(~valid_conf_mask).sum()} rows with out-of-bounds confidence.")
        df = df[valid_conf_mask]

    # 5. Timestamp validation: positive float/int Unix epoch
    df["social_timestamp"] = pd.to_numeric(df["social_timestamp"], errors="coerce")
    valid_ts_mask = df["social_timestamp"] > 0
    if (~valid_ts_mask).sum() > 0:
        logger.warning(f"Dropping {(~valid_ts_mask).sum()} rows with invalid timestamp.")
        df = df[valid_ts_mask]

    # 6. Social metrics numeric casting
    df["social_karma"] = pd.to_numeric(df["social_karma"], errors="coerce").fillna(0).astype(int)
    df["social_num_comments"] = pd.to_numeric(df["social_num_comments"], errors="coerce").fillna(0).astype(int)
    df["syntax_ari"] = pd.to_numeric(df["syntax_ari"], errors="coerce").fillna(0.0).astype(float)

    # Reorder columns explicitly
    df = df[EXPECTED_COLUMNS]

    final_count = len(df)
    logger.info(f"Validation complete: {final_count:,}/{initial_count:,} records passed (preserved: {final_count/initial_count:.2%}).")
    return df

def generate_baseline(train_path: str, test_path: str, output_path: str) -> str:
    """Main pipeline execution function for Phase 1."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_combined = load_and_combine(train_path, test_path)
    df_clean = validate_and_clean_rows(df_combined)

    # Save to baseline CSV
    df_clean.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Standardized baseline CSV successfully saved to: {output_path}")
    logger.info(f"File size: {os.path.getsize(output_path):,} bytes")
    
    # Verify file can be read back cleanly
    df_verify = pd.read_csv(output_path)
    logger.info(f"Verification readback successful: {len(df_verify):,} rows, {len(df_verify.columns)} columns.")
    return output_path

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    
    train_csv = os.path.join(project_root, "data", "raw", "train.csv")
    test_csv = os.path.join(project_root, "data", "raw", "test.csv")
    baseline_csv = os.path.join(project_root, "data", "baseline", "baseline_stress_logs.csv")
    
    generate_baseline(train_csv, test_csv, baseline_csv)
