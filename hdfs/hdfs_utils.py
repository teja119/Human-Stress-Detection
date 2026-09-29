"""
hdfs/hdfs_utils.py
HDFS Integration & Storage Utilities.

Provides a unified interface for writing partitioned/batched records to:
    /data/raw/stress_logs/
Follows standard Hadoop partition naming conventions (e.g. part-00000, part-00001).
Supports both local HDFS directory simulation and remote WebHDFS / cluster environments.
"""

import os
import json
import logging
import subprocess
from typing import List, Dict, Any, Optional

logger = logging.getLogger("HDFSUtils")

class HDFSStorageWriter:
    """
    Manages batched ingestion into HDFS directory structure:
        /data/raw/stress_logs/part-00000
        /data/raw/stress_logs/part-00001
        ...
    """

    def __init__(self, target_dir: str, batch_size: int = 500, webhdfs_url: Optional[str] = None, clean_target: bool = False):
        self.target_dir = target_dir
        self.batch_size = batch_size
        self.webhdfs_url = webhdfs_url or os.getenv("HDFS_WEBHDFS_URL")
        self.partition_index = 0
        self.buffer: List[Dict[str, Any]] = []
        self.total_records_written = 0

        # Ensure directory exists on local mirror
        os.makedirs(self.target_dir, exist_ok=True)

        if clean_target:
            # Clear previous partitions for idempotent runs
            for f in os.listdir(self.target_dir):
                if f.startswith("part-") or f.endswith(".json"):
                    try:
                        os.remove(os.path.join(self.target_dir, f))
                    except OSError:
                        pass
            self.partition_index = 0
        else:
            # Scan existing parts to avoid overwrite if appending
            existing = [f for f in os.listdir(self.target_dir) if f.startswith("part-")]
            if existing:
                indices = []
                for f in existing:
                    try:
                        idx = int(f.replace("part-", "").split(".")[0])
                        indices.append(idx)
                    except ValueError:
                        pass
                if indices:
                    self.partition_index = max(indices) + 1

    def append(self, record: Dict[str, Any]):
        """Adds a record to the memory buffer and flushes if batch threshold reached."""
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        """Flushes buffered records to the next HDFS partition file."""
        if not self.buffer:
            return

        part_filename = f"part-{self.partition_index:05d}"
        part_filepath = os.path.join(self.target_dir, part_filename)

        with open(part_filepath, "w", encoding="utf-8") as f:
            for item in self.buffer:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        record_count = len(self.buffer)
        self.total_records_written += record_count
        logger.info(
            f"[HDFS WRITE] Flushed {record_count} records to partition: {part_filename} "
            f"(Total written: {self.total_records_written})"
        )

        self._sync_to_cluster_if_available(part_filepath, part_filename)

        self.partition_index += 1
        self.buffer.clear()

    def _sync_to_cluster_if_available(self, local_path: str, filename: str):
        """Optional upload to live Hadoop HDFS cluster via hadoop fs -put if available."""
        hdfs_target_path = f"/data/raw/stress_logs/{filename}"
        try:
            result = subprocess.run(
                ["hadoop", "fs", "-mkdir", "-p", "/data/raw/stress_logs"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2
            )
            if result.returncode == 0:
                subprocess.run(
                    ["hadoop", "fs", "-put", "-f", local_path, hdfs_target_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=5
                )
                logger.info(f"[HDFS CLUSTER] Synced {filename} to HDFS cluster at {hdfs_target_path}")
        except Exception:
            pass

    def close(self):
        """Ensures any remaining records in buffer are written to HDFS."""
        if self.buffer:
            self.flush()
        logger.info(f"[HDFS COMPLETE] Storage writer closed. Total ingested records: {self.total_records_written}")


def read_hdfs_partitions(target_dir: str):
    """
    Generator yielding records from all partition files in the HDFS directory.
    """
    if not os.path.exists(target_dir):
        raise FileNotFoundError(f"HDFS directory not found: {target_dir}")

    part_files = sorted([
        f for f in os.listdir(target_dir)
        if f.startswith("part-") or f.endswith(".json")
    ])

    for part_file in part_files:
        filepath = os.path.join(target_dir, part_file)
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
