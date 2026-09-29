#!/usr/bin/env python
"""
mapreduce/mapper.py - Phase 4: MapReduce Preprocessing (Mapper)
==============================================================
Hadoop Streaming Mapper for Social Media Stress Analysis.

Input:
  JSON records (from /data/raw/stress_logs/part-*) via standard input (sys.stdin).

Transformations:
  1. Timestamp conversion: Unix epoch timestamp -> YYYY-MM-DD (entry_date).
  2. Text cleaning:
     - Strips URLs (http://, https://, www.*)
     - Removes HTML tags and unescapes HTML entities
     - Removes special characters, markdown noise, non-alphanumeric symbols
     - Normalizes multiple spaces into a single space
  3. Word count:
     - Computes token count on the cleaned text.
  4. Karma engagement ratio:
     - karma_engagement_ratio = social_karma / social_num_comments
     - Explicit handling: if social_num_comments == 0 -> ratio = 0.0
  5. Stress flag:
     - Validates and maps Dreaddit label into stress_flag (1 = Stress, 0 = Non-stress).
  6. High confidence flag:
     - Sets high_confidence_flag = 1 if confidence >= 0.80 else 0.

Emits (Key \t Value):
  Key:   <subreddit>\t<entry_date>
  Value: <post_id>\t<word_count>\t<syntax_ari>\t<karma_engagement_ratio>\t<stress_flag>\t<confidence>\t<high_confidence_flag>
"""

import sys
import re
import json
import html
from datetime import datetime, timezone

# Compiled Regex patterns for optimal streaming performance
URL_PATTERN = re.compile(r'https?://\S+|www\.\S+', re.IGNORECASE)
HTML_TAG_PATTERN = re.compile(r'<[^>]+>')
SPECIAL_CHAR_PATTERN = re.compile(r'[^a-zA-Z0-9\s]')
WHITESPACE_PATTERN = re.compile(r'\s+')

def clean_text(raw_text: str) -> str:
    """
    Cleans raw Reddit post text:
      - Removes URLs
      - Unescapes & removes HTML tags/entities
      - Strips unwanted special characters & punctuation
      - Normalizes whitespace
    """
    if not raw_text:
        return ""
    # 1. Remove URLs
    text = URL_PATTERN.sub('', raw_text)
    # 2. Unescape and strip HTML tags
    text = html.unescape(text)
    text = HTML_TAG_PATTERN.sub('', text)
    # 3. Remove markdown / special characters, keeping alphanumeric and whitespace
    text = SPECIAL_CHAR_PATTERN.sub(' ', text)
    # 4. Collapse consecutive whitespace
    text = WHITESPACE_PATTERN.sub(' ', text).strip()
    return text

def convert_timestamp(epoch_seconds: float) -> str:
    """Converts a Unix epoch timestamp (seconds) into ISO 'YYYY-MM-DD'."""
    try:
        dt = datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "1970-01-01"

def calculate_karma_ratio(karma: float, comments: float) -> float:
    """
    Calculates karma engagement ratio.
    Explicitly handles comments == 0 by setting ratio = 0.0.
    """
    try:
        c = float(comments)
        if c <= 0.0:
            return 0.0
        return round(float(karma) / c, 4)
    except (ZeroDivisionError, ValueError):
        return 0.0

def process_record(line: str):
    """Parses a single raw JSON log record and outputs Key-Value pair."""
    line = line.strip()
    if not line:
        return

    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return

    post_id = str(data.get("post_id", "")).strip()
    subreddit = str(data.get("subreddit", "")).strip().lower()
    raw_timestamp = data.get("social_timestamp", 0)
    raw_text = data.get("text", "")
    raw_label = data.get("label", 0)
    raw_confidence = data.get("confidence", 0.0)
    raw_karma = data.get("social_karma", 0)
    raw_comments = data.get("social_num_comments", 0)
    raw_ari = data.get("syntax_ari", 0.0)

    # 1. Timestamp conversion
    entry_date = convert_timestamp(raw_timestamp)

    # 2. Text cleaning
    cleaned_txt = clean_text(raw_text)

    # 3. Word count calculation
    words = cleaned_txt.split()
    word_count = len(words)

    # 4. Karma engagement ratio
    karma_ratio = calculate_karma_ratio(raw_karma, raw_comments)

    # 5. Stress flag mapping (Dreaddit: 1=stress, 0=non-stress)
    try:
        stress_flag = int(raw_label)
        if stress_flag not in (0, 1):
            stress_flag = 1 if stress_flag > 0 else 0
    except (ValueError, TypeError):
        stress_flag = 0

    # Readability ARI
    try:
        syntax_ari = round(float(raw_ari), 4)
    except (ValueError, TypeError):
        syntax_ari = 0.0

    # Confidence and High-Confidence Flag (Threshold >= 0.80)
    try:
        confidence = round(float(raw_confidence), 4)
    except (ValueError, TypeError):
        confidence = 0.0
    high_confidence_flag = 1 if confidence >= 0.80 else 0

    # Key: (subreddit, entry_date)
    # Value: post_id, word_count, syntax_ari, karma_ratio, stress_flag, confidence, high_confidence_flag
    key = f"{subreddit}\t{entry_date}"
    value = f"{post_id}\t{word_count}\t{syntax_ari}\t{karma_ratio}\t{stress_flag}\t{confidence}\t{high_confidence_flag}"

    # Emit standard Hadoop Streaming key-value line
    sys.stdout.write(f"{key}\t{value}\n")

if __name__ == "__main__":
    for line in sys.stdin:
        process_record(line)
