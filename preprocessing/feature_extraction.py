"""
preprocessing/feature_extraction.py - Phase 6: Final Feature Extraction
=======================================================================
Generates:
    data/processed/processed_stress_analytics.csv

Columns (Exactly as specified):
    1. post_id
    2. subreddit
    3. entry_date
    4. word_count
    5. avg_readability_index
    6. karma_engagement_ratio
    7. stress_flag
    8. high_confidence_flag

High Confidence Flag Justification & Documentation:
    In the Dreaddit dataset (Turcan & McKeown, EMNLP 2019), each Reddit post
    was annotated by 5 independent annotators.
    The 'confidence' metric represents the annotator agreement score (ranging from 0.0 to 1.0).
    A threshold of 0.80 signifies that 80%+ of annotators (at least 4 out of 5 annotators)
    unanimously agreed on the psychological stress categorization.
    Therefore:
        confidence >= 0.80 -> high_confidence_flag = 1 (High Consensus)
        confidence <  0.80 -> high_confidence_flag = 0 (Moderate/Low Consensus)
"""

import os
import sys
import json
import logging
import pandas as pd

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

HDFS_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "stress_logs")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "processed_stress_analytics.csv")
DAILY_METRICS_CSV = os.path.join(OUTPUT_DIR, "stress_features", "daily_aggregated_metrics.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FEATURES] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FeatureExtraction")

# Documented Consensus Threshold
HIGH_CONFIDENCE_THRESHOLD = 0.80

EXPECTED_OUTPUT_COLUMNS = [
    "post_id",
    "subreddit",
    "entry_date",
    "word_count",
    "avg_readability_index",
    "karma_engagement_ratio",
    "stress_flag",
    "high_confidence_flag"
]


def extract_features(
    hdfs_raw_dir: str = HDFS_RAW_DIR,
    output_csv_path: str = OUTPUT_CSV,
    confidence_threshold: float = HIGH_CONFIDENCE_THRESHOLD
):
    logger.info("=" * 60)
    logger.info("Starting Phase 6: Final Feature Extraction")
    logger.info(f"Source HDFS directory: {hdfs_raw_dir}")
    logger.info(f"Target Output CSV:     {output_csv_path}")
    logger.info(f"Documented High Confidence Threshold: >= {confidence_threshold}")
    logger.info("=" * 60)

    # Import mapper module for transformation logic
    from mapreduce.mapper import convert_timestamp, clean_text, calculate_karma_ratio

    part_files = sorted([
        os.path.join(hdfs_raw_dir, f)
        for f in os.listdir(hdfs_raw_dir)
        if f.startswith("part-") or f.endswith(".json")
    ])

    if not part_files:
        raise FileNotFoundError(f"No raw partition files found in {hdfs_raw_dir}!")

    extracted_records = []
    dropped_count = 0

    for part_file in part_files:
        with open(part_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    dropped_count += 1
                    continue

                # 1. post_id
                post_id = str(data.get("post_id", "")).strip()

                # 2. subreddit
                subreddit = str(data.get("subreddit", "")).strip().lower()

                # 3. entry_date
                entry_date = convert_timestamp(data.get("social_timestamp", 0))

                # 4. word_count (on cleaned text)
                cleaned_text = clean_text(str(data.get("text", "")))
                word_count = len(cleaned_text.split())

                # 5. avg_readability_index (syntax_ari of the post snippet)
                try:
                    raw_ari = float(data.get("syntax_ari", 0.0))
                    avg_readability_index = round(raw_ari, 4)
                except (ValueError, TypeError):
                    avg_readability_index = 0.0

                # 6. karma_engagement_ratio
                karma_engagement_ratio = calculate_karma_ratio(
                    data.get("social_karma", 0),
                    data.get("social_num_comments", 0)
                )

                # 7. stress_flag (binary {0, 1})
                try:
                    raw_label = int(data.get("label", 0))
                    stress_flag = 1 if raw_label > 0 else 0
                except (ValueError, TypeError):
                    stress_flag = 0

                # 8. high_confidence_flag (binary {0, 1}) based on documented threshold
                try:
                    confidence = float(data.get("confidence", 0.0))
                    high_confidence_flag = 1 if confidence >= confidence_threshold else 0
                except (ValueError, TypeError):
                    high_confidence_flag = 0

                extracted_records.append({
                    "post_id": post_id,
                    "subreddit": subreddit,
                    "entry_date": entry_date,
                    "word_count": word_count,
                    "avg_readability_index": avg_readability_index,
                    "karma_engagement_ratio": karma_engagement_ratio,
                    "stress_flag": stress_flag,
                    "high_confidence_flag": high_confidence_flag
                })

    df_out = pd.DataFrame(extracted_records)

    # Strictly guarantee exact column ordering
    df_out = df_out[EXPECTED_OUTPUT_COLUMNS]

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df_out.to_csv(output_csv_path, index=False, encoding="utf-8")

    logger.info(f"Successfully generated: {output_csv_path}")
    logger.info(f"Total extracted records: {len(df_out):,}")
    logger.info(f"Exact columns: {list(df_out.columns)}")
    logger.info(f"High-confidence stressed records: {len(df_out[(df_out['stress_flag'] == 1) & (df_out['high_confidence_flag'] == 1)]):,}")
    logger.info("=" * 60)
    return output_csv_path

if __name__ == "__main__":
    extract_features()
