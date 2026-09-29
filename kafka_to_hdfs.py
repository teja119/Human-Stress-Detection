"""
kafka_to_hdfs.py - Root executable runner for Phase 3: Kafka to HDFS Consumer
"""
import sys
import os
import importlib.util

proj_dir = os.path.dirname(os.path.abspath(__file__))
consumer_file = os.path.join(proj_dir, "kafka", "consumer.py")

spec = importlib.util.spec_from_file_location("kafka_consumer_module", consumer_file)
consumer_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumer_mod)

if __name__ == "__main__":
    consumer_mod.consume_to_hdfs()
