#!/usr/bin/env bash
# ==============================================================================
# mapreduce/run_job.sh - Hadoop Streaming Job Execution Script
# ==============================================================================
# Executes Mapper and Reducer over HDFS /data/raw/stress_logs/
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_HDFS="/data/raw/stress_logs"
OUTPUT_HDFS="/data/processed/stress_features"

echo "============================================================"
echo "BDA-MH-K5: Submitting MapReduce Hadoop Streaming Job"
echo "============================================================"

# Check if Hadoop cluster is accessible
if command -v hadoop &> /dev/null; then
    echo "[MODE] Executing on Apache Hadoop Cluster..."
    
    # Clean previous output directory in HDFS
    hadoop fs -rm -r -f "${OUTPUT_HDFS}" || true

    # Locate hadoop-streaming jar
    STREAMING_JAR=$(find "${HADOOP_HOME}/share/hadoop/tools/lib/" -name "hadoop-streaming-*.jar" 2>/dev/null | head -n 1)

    if [ -z "${STREAMING_JAR}" ]; then
        STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar"
    fi

    echo "Using Hadoop Streaming Jar: ${STREAMING_JAR}"

    hadoop jar "${STREAMING_JAR}" \
        -D mapreduce.job.name="BDA_Stress_Log_Analytics" \
        -D mapreduce.job.reduces=2 \
        -input "${INPUT_HDFS}" \
        -output "${OUTPUT_HDFS}" \
        -mapper "python3 mapper.py" \
        -reducer "python3 reducer.py" \
        -file "${SCRIPT_DIR}/mapper.py" \
        -file "${SCRIPT_DIR}/reducer.py"

    echo "Hadoop job complete. Fetching results from HDFS..."
    hadoop fs -get "${OUTPUT_HDFS}" "${PROJECT_ROOT}/data/processed/"
else
    echo "[MODE] Hadoop CLI not detected in PATH. Executing Local Hadoop Streaming Simulation..."
    python3 "${SCRIPT_DIR}/run_mapreduce.py"
fi

echo "============================================================"
echo "MapReduce Execution Finished."
echo "============================================================"
