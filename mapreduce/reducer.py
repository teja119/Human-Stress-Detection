#!/usr/bin/env python
"""
mapreduce/reducer.py - Phase 5: Reducer Aggregation
===================================================
Hadoop Streaming Reducer for Social Media Stress Analytics.

Input:
  Sorted tab-delimited streams from Mapper via standard input (sys.stdin).
  Format:
    subreddit \t entry_date \t post_id \t word_count \t syntax_ari \t karma_ratio \t stress_flag \t confidence \t high_confidence_flag

Grouping Key:
  (subreddit, entry_date)

Aggregations Computed:
  1. Average Readability Index:
     avg_readability_index = sum(syntax_ari) / total_posts
  2. Average Karma Engagement Ratio:
     avg_karma_ratio = sum(karma_ratio) / total_posts
  3. Stress Percentage:
     stress_percentage = (stressed_posts / total_posts) * 100.0
  4. Volume & High-Confidence Metrics:
     total_posts, stressed_posts, high_conf_stressed_posts

Emits (Standard Hadoop Streaming stdout):
  subreddit \t entry_date \t total_posts \t avg_ari \t avg_karma_ratio \t stress_percentage \t high_conf_stressed_posts
"""

import sys
import os

def emit_aggregation(current_key, posts_in_group):
    """Computes and outputs the aggregated metrics for a key group."""
    if not current_key or not posts_in_group:
        return

    subreddit, entry_date = current_key
    total_posts = len(posts_in_group)
    if total_posts == 0:
        return

    sum_ari = sum(p["syntax_ari"] for p in posts_in_group)
    sum_karma_ratio = sum(p["karma_ratio"] for p in posts_in_group)
    stressed_count = sum(1 for p in posts_in_group if p["stress_flag"] == 1)
    high_conf_stressed_count = sum(
        1 for p in posts_in_group if p["stress_flag"] == 1 and p["high_confidence_flag"] == 1
    )

    avg_ari = round(sum_ari / total_posts, 4)
    avg_karma_ratio = round(sum_karma_ratio / total_posts, 4)
    stress_percentage = round((stressed_count / total_posts) * 100.0, 2)

    # Output to stdout: tab-separated
    sys.stdout.write(
        f"{subreddit}\t{entry_date}\t{total_posts}\t{avg_ari}\t{avg_karma_ratio}\t{stress_percentage}\t{high_conf_stressed_count}\n"
    )


def reduce_stream():
    current_key = None
    current_posts = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) < 9:
            continue

        subreddit = parts[0]
        entry_date = parts[1]
        post_id = parts[2]
        
        try:
            word_count = int(parts[3])
            syntax_ari = float(parts[4])
            karma_ratio = float(parts[5])
            stress_flag = int(parts[6])
            confidence = float(parts[7])
            high_confidence_flag = int(parts[8])
        except (ValueError, TypeError):
            continue

        key = (subreddit, entry_date)

        if current_key and key != current_key:
            emit_aggregation(current_key, current_posts)
            current_posts = []

        current_key = key
        current_posts.append({
            "post_id": post_id,
            "word_count": word_count,
            "syntax_ari": syntax_ari,
            "karma_ratio": karma_ratio,
            "stress_flag": stress_flag,
            "confidence": confidence,
            "high_confidence_flag": high_confidence_flag
        })

    # Flush final key group
    if current_key and current_posts:
        emit_aggregation(current_key, current_posts)


if __name__ == "__main__":
    reduce_stream()
