"""
kafka/config.py
Configuration parameters for Kafka Producer and Consumer.
"""

import os

# Kafka Cluster Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_STRESS_LOGS = os.getenv("KAFKA_TOPIC_STRESS_LOGS", "stress_logs")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bda_stress_group")

# File and Directory Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

BASELINE_CSV_PATH = os.path.join(PROJECT_ROOT, "data", "baseline", "baseline_stress_logs.csv")
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
HDFS_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "stress_logs")
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
PROCESSED_FEATURES_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "stress_features")
FINAL_ANALYTICS_CSV_PATH = os.path.join(PROCESSED_DATA_DIR, "processed_stress_analytics.csv")

# Streaming Batch Settings
HDFS_BATCH_SIZE = 500  # Number of records per HDFS partition file
STREAMING_DELAY_SECONDS = 0.0005  # Slight delay per message to simulate real-time stream
