"""
kafka/consumer.py - Phase 3: Kafka to HDFS Consumer
===================================================
Consumes streaming messages from the Kafka topic `stress_logs`
and persists them into partitioned HDFS batches:
    /data/raw/stress_logs/part-00000
    /data/raw/stress_logs/part-00001
    ...

Architectural distinction:
  - Apache Kafka: Real-time distributed streaming transport (message broker)
  - Apache HDFS:  Fault-tolerant distributed persistent storage for Big Data batch analytics
"""

import os
import sys
import time
import json
import logging
import argparse
from typing import Optional

# Setup directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

sys.path.insert(0, SCRIPT_DIR)
import config as kcfg
from hdfs.hdfs_utils import HDFSStorageWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER->HDFS] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KafkaHDFSConsumer")

# Clean import of third-party kafka library avoiding folder name collision
def get_third_party_kafka():
    saved_path = list(sys.path)
    try:
        sys.path = [p for p in sys.path if os.path.abspath(p) not in (SCRIPT_DIR, PROJECT_ROOT, "")]
        import kafka
        return kafka
    except Exception as e:
        logger.debug(f"Third-party kafka import note: {e}")
        return None
    finally:
        sys.path = saved_path

kafka_lib = get_third_party_kafka()


def create_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str):
    """Attempts to connect to live Kafka broker as a consumer."""
    if not kafka_lib:
        return None

    try:
        consumer = kafka_lib.KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=3000,  # Stops polling if idle for 3s
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        logger.info(f"Connected to Kafka broker at: {bootstrap_servers}, subscribed to '{topic}'")
        return consumer
    except Exception as e:
        logger.info(f"Kafka broker offline or not reachable ({e}). Resilient HDFS pipeline active.")
        return None


def consume_to_hdfs(
    topic: str = kcfg.KAFKA_TOPIC_STRESS_LOGS,
    bootstrap_servers: str = kcfg.KAFKA_BOOTSTRAP_SERVERS,
    group_id: str = kcfg.KAFKA_CONSUMER_GROUP,
    hdfs_target_dir: str = kcfg.HDFS_RAW_DIR,
    batch_size: int = kcfg.HDFS_BATCH_SIZE,
    max_records: Optional[int] = None,
    stream_buffer: Optional[str] = None
):
    """
    Main ingestion engine: reads from Kafka (or stream buffer/baseline) and commits to partitioned HDFS.
    """
    logger.info("=" * 60)
    logger.info("Initializing Kafka -> HDFS Ingestion Service")
    logger.info(f"Target HDFS Directory: {hdfs_target_dir}")
    logger.info(f"Batch Partition Size: {batch_size} records per part-file")
    logger.info("=" * 60)

    # Clean previous partitions in HDFS raw dir for fresh idempotent run
    os.makedirs(hdfs_target_dir, exist_ok=True)
    writer = HDFSStorageWriter(target_dir=hdfs_target_dir, batch_size=batch_size, clean_target=True)
    consumer = create_kafka_consumer(bootstrap_servers, topic, group_id)
    consumed_count = 0
    start_time = time.time()

    if consumer is not None:
        logger.info("Polling streaming events from live Kafka topic...")
        try:
            for message in consumer:
                record = message.value
                writer.append(record)
                consumed_count += 1

                if consumed_count % 500 == 0:
                    logger.info(f"Consuming Kafka message #{consumed_count:,} (offset={message.offset})")

                if max_records and consumed_count >= max_records:
                    logger.info(f"Reached specified limit of {max_records:,} records.")
                    break
        except Exception as e:
            logger.error(f"Error during Kafka polling: {e}")
        finally:
            consumer.close()
    else:
        # Stream ingestion fallback
        source_file = stream_buffer
        if source_file and os.path.exists(source_file):
            logger.info(f"Streaming messages from buffer: {source_file}")
            with open(source_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rec = json.loads(line)
                        writer.append(rec)
                        consumed_count += 1
                        if max_records and consumed_count >= max_records:
                            break
        else:
            logger.info(f"Streaming messages from baseline CSV: {kcfg.BASELINE_CSV_PATH}")
            import pandas as pd
            if not os.path.exists(kcfg.BASELINE_CSV_PATH):
                raise FileNotFoundError(f"Baseline CSV not found at: {kcfg.BASELINE_CSV_PATH}")
            df = pd.read_csv(kcfg.BASELINE_CSV_PATH)
            if max_records:
                df = df.iloc[:max_records]
            for _, row in df.iterrows():
                rec = {
                    "post_id": str(row["post_id"]).strip(),
                    "subreddit": str(row["subreddit"]).strip().lower(),
                    "social_timestamp": float(row["social_timestamp"]),
                    "text": str(row["text"]),
                    "label": int(row["label"]),
                    "confidence": float(row["confidence"]),
                    "social_karma": int(row["social_karma"]),
                    "social_num_comments": int(row["social_num_comments"]),
                    "syntax_ari": float(row["syntax_ari"]),
                }
                writer.append(rec)
                consumed_count += 1

    # Flush remaining records
    writer.close()
    elapsed = time.time() - start_time

    # Report generated HDFS partition files
    generated_parts = sorted([
        f for f in os.listdir(hdfs_target_dir) if f.startswith("part-")
    ])

    logger.info("=" * 60)
    logger.info("Kafka -> HDFS Ingestion Complete!")
    logger.info(f"Total records stored in HDFS: {consumed_count:,}")
    logger.info(f"Total HDFS partitions created: {len(generated_parts)}")
    for part in generated_parts:
        part_path = os.path.join(hdfs_target_dir, part)
        part_size = os.path.getsize(part_path)
        with open(part_path, "r", encoding="utf-8") as pf:
            line_count = sum(1 for _ in pf)
        logger.info(f"  - {hdfs_target_dir}/{part} ({line_count:,} records, {part_size:,} bytes)")
    logger.info(f"Time taken: {elapsed:.2f}s | Throughput: {consumed_count / max(elapsed, 0.001):.1f} rec/s")
    logger.info("=" * 60)

    return consumed_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to HDFS Consumer Service")
    parser.add_argument("--topic", default=kcfg.KAFKA_TOPIC_STRESS_LOGS, help="Kafka topic name")
    parser.add_argument("--servers", default=kcfg.KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--group", default=kcfg.KAFKA_CONSUMER_GROUP, help="Consumer group id")
    parser.add_argument("--hdfs-dir", default=kcfg.HDFS_RAW_DIR, help="Target HDFS raw directory")
    parser.add_argument("--batch-size", type=int, default=kcfg.HDFS_BATCH_SIZE, help="Records per partition file")
    parser.add_argument("--max-records", type=int, default=None, help="Maximum records to ingest")
    parser.add_argument("--buffer", default=None, help="Optional streaming buffer input")
    args = parser.parse_args()

    consume_to_hdfs(
        topic=args.topic,
        bootstrap_servers=args.servers,
        group_id=args.group,
        hdfs_target_dir=args.hdfs_dir,
        batch_size=args.batch_size,
        max_records=args.max_records,
        stream_buffer=args.buffer
    )
