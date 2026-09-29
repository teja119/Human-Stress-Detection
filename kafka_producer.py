"""
kafka_producer.py - Root executable runner for Phase 2: Kafka Producer
"""
import sys
import os
import importlib.util

# Load kafka/producer.py directly
proj_dir = os.path.dirname(os.path.abspath(__file__))
producer_file = os.path.join(proj_dir, "kafka", "producer.py")

spec = importlib.util.spec_from_file_location("kafka_producer_module", producer_file)
producer_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer_mod)

if __name__ == "__main__":
    producer_mod.stream_records()
