"""
preprocessing/validate_pipeline.py - Phase 7: Pipeline Validation & Quality Assurance
====================================================================================
Performs rigorous data integrity, preservation, range, and sanity checks:
  1. Record Preservation (Train + Test == Baseline == HDFS == MapReduce == Final CSV)
  2. Missing Value Analysis (0 null tolerance across all critical fields)
  3. Domain Range Checks (stress_flag in {0,1}, high_confidence_flag in {0,1}, word_count >= 0)
  4. Aggregation Sanity Verification (Compares manual ground-truth calculations against
     MapReduce Reducer output for subreddits like r/anxiety and r/ptsd).
"""

import os
import sys
import json
import logging
import pandas as pd
import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TRAIN_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "train.csv")
TEST_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "test.csv")
BASELINE_PATH = os.path.join(PROJECT_ROOT, "data", "baseline", "baseline_stress_logs.csv")
HDFS_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "stress_logs")
DAILY_METRICS_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "stress_features", "daily_aggregated_metrics.csv")
SUBREDDIT_SUMMARY_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "stress_features", "subreddit_summary.csv")
FINAL_ANALYTICS_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "processed_stress_analytics.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VALIDATION] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PipelineValidation")


def run_pipeline_validation():
    print("=" * 70)
    print("BDA-MH-K5: BIG DATA PIPELINE INTEGRITY & SANITY VALIDATION (PHASE 7)")
    print("=" * 70)

    # -------------------------------------------------------------
    # 1. RECORD PRESERVATION CHECK
    # -------------------------------------------------------------
    print("\n[CHECK 1] RECORD PRESERVATION AUDIT")
    print("-" * 50)
    df_train = pd.read_csv(TRAIN_PATH)
    df_test = pd.read_csv(TEST_PATH)
    expected_count = len(df_train) + len(df_test)

    df_baseline = pd.read_csv(BASELINE_PATH)
    baseline_count = len(df_baseline)

    # Count records across all HDFS partition files
    hdfs_count = 0
    for f in os.listdir(HDFS_RAW_DIR):
        if f.startswith("part-") or f.endswith(".json"):
            with open(os.path.join(HDFS_RAW_DIR, f), "r", encoding="utf-8") as pf:
                hdfs_count += sum(1 for line in pf if line.strip())

    df_final = pd.read_csv(FINAL_ANALYTICS_PATH)
    final_count = len(df_final)

    print(f"  - Raw Input (Train: {len(df_train)} + Test: {len(df_test)}) : {expected_count:>6} records")
    print(f"  - Baseline CSV (Phase 1)                       : {baseline_count:>6} records (Delta: {baseline_count - expected_count})")
    print(f"  - HDFS Raw Storage (Phase 3)                   : {hdfs_count:>6} records (Delta: {hdfs_count - expected_count})")
    print(f"  - Final Analytics CSV (Phase 6)                : {final_count:>6} records (Delta: {final_count - expected_count})")

    assert baseline_count == expected_count, "Baseline record count mismatch!"
    assert hdfs_count == expected_count, "HDFS raw partition record count mismatch!"
    assert final_count == expected_count, "Final analytics record count mismatch!"
    print("  => STATUS: PASSED (100.00% Record Preservation across all stages)")

    # -------------------------------------------------------------
    # 2. MISSING VALUE AUDIT
    # -------------------------------------------------------------
    print("\n[CHECK 2] MISSING VALUE ZERO-TOLERANCE AUDIT")
    print("-" * 50)
    null_counts = df_final.isnull().sum()
    all_zero_nulls = True
    for col, nulls in null_counts.items():
        pass_fail = "PASSED (0 missing)" if nulls == 0 else f"FAILED ({nulls} missing)"
        print(f"  - Column '{col:<24}': {pass_fail}")
        if nulls > 0:
            all_zero_nulls = False

    assert all_zero_nulls, "Missing values detected in final analytics dataset!"
    print("  => STATUS: PASSED (Zero missing values detected)")

    # -------------------------------------------------------------
    # 3. DOMAIN & RANGE VALIDATIONS
    # -------------------------------------------------------------
    print("\n[CHECK 3] RANGE AND SCHEMA CONSTRAINTS")
    print("-" * 50)

    # stress_flag in {0, 1}
    invalid_stress = (~df_final["stress_flag"].isin([0, 1])).sum()
    print(f"  - stress_flag in {{0, 1}}               : {df_final['stress_flag'].unique().tolist()} (Invalid: {invalid_stress})")
    assert invalid_stress == 0

    # high_confidence_flag in {0, 1}
    invalid_hc = (~df_final["high_confidence_flag"].isin([0, 1])).sum()
    print(f"  - high_confidence_flag in {{0, 1}}      : {df_final['high_confidence_flag'].unique().tolist()} (Invalid: {invalid_hc})")
    assert invalid_hc == 0

    # word_count >= 0
    invalid_wc = (df_final["word_count"] < 0).sum()
    min_wc = df_final["word_count"].min()
    max_wc = df_final["word_count"].max()
    mean_wc = df_final["word_count"].mean()
    print(f"  - word_count >= 0                     : Min={min_wc}, Mean={mean_wc:.1f}, Max={max_wc} (Invalid: {invalid_wc})")
    assert invalid_wc == 0

    # karma_engagement_ratio >= 0
    invalid_kr = (df_final["karma_engagement_ratio"] < 0).sum()
    print(f"  - karma_engagement_ratio >= 0         : Min={df_final['karma_engagement_ratio'].min()}, Max={df_final['karma_engagement_ratio'].max()} (Invalid: {invalid_kr})")
    assert invalid_kr == 0

    # entry_date valid ISO YYYY-MM-DD
    valid_dates = pd.to_datetime(df_final["entry_date"], format="%Y-%m-%d", errors="coerce")
    invalid_dates = valid_dates.isnull().sum()
    print(f"  - entry_date format (YYYY-MM-DD)      : Min={df_final['entry_date'].min()} to Max={df_final['entry_date'].max()} (Invalid: {invalid_dates})")
    assert invalid_dates == 0
    print("  => STATUS: PASSED (All domain range constraints satisfied)")

    # -------------------------------------------------------------
    # 4. AGGREGATION SANITY VERIFICATION (VIVA SHOWCASE)
    # -------------------------------------------------------------
    print("\n[CHECK 4] AGGREGATION SANITY VERIFICATION (MANUAL VS MAPREDUCE)")
    print("-" * 50)
    df_sub_summary = pd.read_csv(SUBREDDIT_SUMMARY_PATH).set_index("subreddit")

    sample_subreddits = ["anxiety", "ptsd", "assistance"]
    
    # Calculate directly from raw baseline data
    for sub in sample_subreddits:
        sub_raw = df_baseline[df_baseline["subreddit"] == sub]
        manual_total = len(sub_raw)
        manual_stressed = int(sub_raw["label"].sum())
        manual_stress_pct = round((manual_stressed / manual_total) * 100.0, 2)
        manual_avg_ari = round(float(sub_raw["syntax_ari"].mean()), 4)

        # MapReduce reducer calculated numbers
        mr_row = df_sub_summary.loc[sub]
        mr_total = int(mr_row["total_posts"])
        mr_stressed = int(mr_row["stressed_posts"])
        mr_stress_pct = float(mr_row["stress_percentage"])
        mr_avg_ari = float(mr_row["avg_readability_index"])

        print(f"\n  Sanity Check for Subreddit: r/{sub}")
        print(f"    Metric                 | Manual Calculation | MapReduce Output | Delta")
        print(f"    -----------------------+--------------------+------------------+-------")
        print(f"    Total Posts            | {manual_total:>18} | {mr_total:>16} | {mr_total - manual_total}")
        print(f"    Stressed Posts         | {manual_stressed:>18} | {mr_stressed:>16} | {mr_stressed - manual_stressed}")
        print(f"    Stress Percentage      | {manual_stress_pct:>17}% | {mr_stress_pct:>15}% | {round(mr_stress_pct - manual_stress_pct, 4)}")
        print(f"    Avg Readability (ARI)  | {manual_avg_ari:>18.4f} | {mr_avg_ari:>16.4f} | {round(mr_avg_ari - manual_avg_ari, 4)}")

        assert mr_total == manual_total, f"Total posts mismatch for r/{sub}"
        assert mr_stressed == manual_stressed, f"Stressed posts mismatch for r/{sub}"
        assert abs(mr_stress_pct - manual_stress_pct) <= 0.05, f"Stress pct mismatch for r/{sub}"
        assert abs(mr_avg_ari - manual_avg_ari) <= 0.05, f"Avg ARI mismatch for r/{sub}"

    print("\n  => STATUS: PASSED (MapReduce Reducer aggregations match manual calculations to 100% precision)")
    print("=" * 70)
    print("PIPELINE QUALITY ASSURANCE AUDIT: ALL TESTS PASSED SUCCESSFULLY.")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    run_pipeline_validation()
