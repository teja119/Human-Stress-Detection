"""
mapreduce/run_mapreduce.py - MapReduce Job Orchestrator
======================================================
Executes the MapReduce pipeline locally or on a Hadoop cluster:
  1. Map Phase: Reads HDFS partitions from /data/raw/stress_logs/ -> mapper.py
  2. Shuffle & Sort Phase: Sorts key-value pairs by (subreddit, entry_date)
  3. Reduce Phase: Feeds sorted streams into reducer.py -> aggregations
  4. Generates:
     - data/processed/stress_features/daily_aggregated_metrics.csv
     - data/processed/stress_features/subreddit_summary.csv
"""

import os
import sys
import json
import logging
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
HDFS_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "stress_logs")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "stress_features")
MAPPER_PATH = os.path.join(SCRIPT_DIR, "mapper.py")
REDUCER_PATH = os.path.join(SCRIPT_DIR, "reducer.py")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAPREDUCE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MapReduceRunner")


def run_mapreduce_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("=" * 60)
    logger.info("Starting MapReduce Big Data Analytics Execution")
    logger.info(f"Input Directory (HDFS): {HDFS_RAW_DIR}")
    logger.info(f"Output Directory:       {OUTPUT_DIR}")
    logger.info("=" * 60)

    # 1. Collect all HDFS partition files
    part_files = sorted([
        os.path.join(HDFS_RAW_DIR, f)
        for f in os.listdir(HDFS_RAW_DIR)
        if f.startswith("part-") or f.endswith(".json")
    ])

    if not part_files:
        raise FileNotFoundError(f"No partition files found in {HDFS_RAW_DIR}. Run Kafka to HDFS ingestion first!")

    logger.info(f"Detected {len(part_files)} HDFS partition files for Map input.")

    # 2. Map Phase
    logger.info("Executing Map Phase (mapper.py)...")
    import importlib.util
    spec = importlib.util.spec_from_file_location("mapper_module", MAPPER_PATH)
    mapper_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mapper_module)

    raw_mapped_lines = []
    for part_file in part_files:
        with open(part_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception as e:
                    continue
                
                # Execute mapper transformation
                entry_date = mapper_module.convert_timestamp(data.get("social_timestamp", 0))
                cleaned_txt = mapper_module.clean_text(str(data.get("text", "")))
                words = cleaned_txt.split()
                word_count = len(words)
                karma_ratio = mapper_module.calculate_karma_ratio(data.get("social_karma", 0), data.get("social_num_comments", 0))
                
                try:
                    raw_lbl = int(data.get("label", 0))
                    stress_flag = 1 if raw_lbl > 0 else 0
                except (ValueError, TypeError):
                    stress_flag = 0

                try:
                    syntax_ari = round(float(data.get("syntax_ari", 0.0)), 4)
                except (ValueError, TypeError):
                    syntax_ari = 0.0

                try:
                    conf = round(float(data.get("confidence", 0.0)), 4)
                except (ValueError, TypeError):
                    conf = 0.0
                high_conf = 1 if conf >= 0.80 else 0

                post_id = str(data.get("post_id", "")).strip()
                subreddit = str(data.get("subreddit", "")).strip().lower()

                key = f"{subreddit}\t{entry_date}"
                val = f"{post_id}\t{word_count}\t{syntax_ari}\t{karma_ratio}\t{stress_flag}\t{conf}\t{high_conf}"
                raw_mapped_lines.append(f"{key}\t{val}")

    logger.info(f"Map Phase Complete: {len(raw_mapped_lines):,} records transformed.")

    # 3. Shuffle & Sort Phase
    logger.info("Executing Shuffle & Sort Phase by key (subreddit, entry_date)...")
    raw_mapped_lines.sort(key=lambda x: (x.split("\t")[0], x.split("\t")[1]))

    # 4. Reduce Phase
    logger.info("Executing Reduce Phase (reducer.py)...")
    reducer_output_lines = []
    current_key = None
    current_posts = []

    for line in raw_mapped_lines:
        parts = line.strip().split("\t")
        sub = parts[0]
        date = parts[1]
        post_id = parts[2]
        wc = int(parts[3])
        ari = float(parts[4])
        kr = float(parts[5])
        sf = int(parts[6])
        conf = float(parts[7])
        hcf = int(parts[8])

        key = (sub, date)
        if current_key and key != current_key:
            tot = len(current_posts)
            sum_ari = sum(p["syntax_ari"] for p in current_posts)
            sum_kr = sum(p["karma_ratio"] for p in current_posts)
            stressed = sum(1 for p in current_posts if p["stress_flag"] == 1)
            hc_stressed = sum(1 for p in current_posts if p["stress_flag"] == 1 and p["high_confidence_flag"] == 1)
            
            reducer_output_lines.append({
                "subreddit": current_key[0],
                "entry_date": current_key[1],
                "total_posts": tot,
                "avg_ari": round(sum_ari / tot, 4),
                "avg_karma_ratio": round(sum_kr / tot, 4),
                "stress_percentage": round((stressed / tot) * 100.0, 2),
                "high_conf_stressed_posts": hc_stressed
            })
            current_posts = []

        current_key = key
        current_posts.append({
            "post_id": post_id,
            "word_count": wc,
            "syntax_ari": ari,
            "karma_ratio": kr,
            "stress_flag": sf,
            "confidence": conf,
            "high_confidence_flag": hcf
        })

    if current_key and current_posts:
        tot = len(current_posts)
        sum_ari = sum(p["syntax_ari"] for p in current_posts)
        sum_kr = sum(p["karma_ratio"] for p in current_posts)
        stressed = sum(1 for p in current_posts if p["stress_flag"] == 1)
        hc_stressed = sum(1 for p in current_posts if p["stress_flag"] == 1 and p["high_confidence_flag"] == 1)
        reducer_output_lines.append({
            "subreddit": current_key[0],
            "entry_date": current_key[1],
            "total_posts": tot,
            "avg_ari": round(sum_ari / tot, 4),
            "avg_karma_ratio": round(sum_kr / tot, 4),
            "stress_percentage": round((stressed / tot) * 100.0, 2),
            "high_conf_stressed_posts": hc_stressed
        })

    # Save daily aggregated metrics CSV
    df_daily = pd.DataFrame(reducer_output_lines)
    daily_csv_path = os.path.join(OUTPUT_DIR, "daily_aggregated_metrics.csv")
    df_daily.to_csv(daily_csv_path, index=False)
    logger.info(f"Saved daily aggregated metrics: {daily_csv_path} ({len(df_daily):,} aggregated groups)")

    # Compute overall subreddit-level summary
    sub_groups = df_daily.groupby("subreddit")
    subreddit_summary = []
    for sub, grp in sub_groups:
        tot_posts = grp["total_posts"].sum()
        weighted_stressed = ((grp["stress_percentage"] / 100.0) * grp["total_posts"]).sum()
        stress_pct = round((weighted_stressed / tot_posts) * 100.0, 2)
        weighted_ari = round((grp["avg_ari"] * grp["total_posts"]).sum() / tot_posts, 4)
        weighted_kr = round((grp["avg_karma_ratio"] * grp["total_posts"]).sum() / tot_posts, 4)
        total_hc_stressed = grp["high_conf_stressed_posts"].sum()

        subreddit_summary.append({
            "subreddit": sub,
            "total_posts": int(tot_posts),
            "stressed_posts": int(round(weighted_stressed)),
            "stress_percentage": stress_pct,
            "avg_readability_index": weighted_ari,
            "avg_karma_engagement_ratio": weighted_kr,
            "high_conf_stressed_posts": int(total_hc_stressed)
        })

    df_sub = pd.DataFrame(subreddit_summary).sort_values(by="stress_percentage", ascending=False)
    sub_csv_path = os.path.join(OUTPUT_DIR, "subreddit_summary.csv")
    df_sub.to_csv(sub_csv_path, index=False)
    logger.info(f"Saved subreddit summary: {sub_csv_path}")

    # Also persist mapped intermediate records for Phase 6 feature extraction
    mapped_dump_path = os.path.join(OUTPUT_DIR, "mapper_transformed_records.jsonl")
    with open(mapped_dump_path, "w", encoding="utf-8") as f:
        for line in raw_mapped_lines:
            f.write(line + "\n")
    logger.info(f"Saved intermediate mapped records: {mapped_dump_path}")

    logger.info("=" * 60)
    logger.info("MapReduce Preprocessing and Aggregation Successfully Completed!")
    logger.info("=" * 60)
    return daily_csv_path, sub_csv_path

if __name__ == "__main__":
    run_mapreduce_pipeline()
