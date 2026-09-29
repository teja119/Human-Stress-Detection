# BDA-MH-K5: Social Media Mental Health & Stress Analytics Pipeline

A production-grade, end-to-end Big Data Analytics (BDA) engineering pipeline for ingesting, transforming, aggregating, and visualizing social media psychological stress indicators from the Reddit **Dreaddit** dataset.

---

## 🏗️ System Architecture & Workflow

```
[ dreaddit-train.csv ] ──┐
                         ├──► [ Phase 1: Schema Validation & Baseline ] ──► baseline_stress_logs.csv
[ dreaddit-test.csv  ] ──┘                                                          │
                                                                                    ▼
                                                                     [ Phase 2: Kafka Producer ]
                                                                                    │ (Record-by-record)
                                                                                    ▼
                                                                     [ Kafka Topic: stress_logs ]
                                                                                    │
                                                                                    ▼
                                                                     [ Phase 3: Kafka Consumer ]
                                                                                    │ (Batching & Partitioning)
                                                                                    ▼
                                                                  [ HDFS: /data/raw/stress_logs/ ]
                                                                      (part-00000 ... part-00007)
                                                                                    │
                                                                                    ▼
                                                                     [ Phase 4: MapReduce Mapper ]
                                                                        (Text Cleaning, Word Count,
                                                                         Karma Ratio, Timestamp Conv.)
                                                                                    │
                                                                                    ▼
                                                                     [ Shuffle & Sort by (Sub, Date) ]
                                                                                    │
                                                                                    ▼
                                                                    [ Phase 5: MapReduce Reducer ]
                                                                        (ARI Avg, Karma Ratio Avg,
                                                                         Stress Percentage Grouping)
                                                                                    │
                                           ┌────────────────────────────────────────┴────────────────────────────────────────┐
                                           ▼                                                                                 ▼
                         [ Phase 6: Final Feature Extraction ]                                            [ Daily & Subreddit Aggregations ]
                           processed_stress_analytics.csv                                                    daily_aggregated_metrics.csv
                                           │                                                                 subreddit_summary.csv
                                           └────────────────────────────────────────┬────────────────────────────────────────┘
                                                                                    ▼
                                                                     [ Phase 7: Automated QA & Validation ]
                                                                        (100% Record Preservation, 0 Nulls)
                                                                                    │
                                                                                    ▼
                                                                     [ Phase 8: Grafana Dashboard ]
                                                                        (5 Interactive Analytical Panels)
```

---

## 📁 Repository Directory Structure

```text
BDA-MH-K5/
│
├── data/
│   ├── raw/                                 # Raw input train/test sets and HDFS partitions
│   │   ├── train.csv                        # Dreaddit train partition (2,838 rows)
│   │   ├── test.csv                         # Dreaddit test partition (715 rows)
│   │   └── stress_logs/                     # Partitioned HDFS raw storage
│   │       ├── part-00000                   # HDFS partition batch 0 (500 records)
│   │       ├── part-00001                   # HDFS partition batch 1 (500 records)
│   │       └── ...                          # Up to part-00007
│   ├── baseline/
│   │   └── baseline_stress_logs.csv         # Standardized uncleaned baseline (3,553 rows)
│   └── processed/
│       ├── processed_stress_analytics.csv   # Final 8-column post-level analytics dataset
│       └── stress_features/
│           ├── daily_aggregated_metrics.csv # MapReduce reducer daily aggregated output
│           ├── subreddit_summary.csv        # Subreddit-level aggregated stress metrics
│           └── mapper_transformed_records.jsonl # Intermediate MapReduce stream dump
│
├── kafka/
│   ├── config.py                            # Cluster brokers, topic names, and batch constants
│   ├── producer.py                          # Phase 2: Record-by-record streaming producer
│   └── consumer.py                          # Phase 3: Partitioning consumer writing to HDFS
│
├── kafka_producer.py                        # Root alias runner for Kafka Producer
├── kafka_to_hdfs.py                         # Root alias runner for Kafka Consumer to HDFS
│
├── hdfs/
│   └── hdfs_utils.py                        # HDFS Storage writer & partition batch manager
│
├── mapreduce/
│   ├── mapper.py                            # Phase 4: Hadoop Streaming Mapper
│   ├── reducer.py                           # Phase 5: Hadoop Streaming Reducer
│   ├── run_mapreduce.py                     # MapReduce job execution orchestrator
│   ├── run_job.sh                           # Cluster Hadoop streaming bash submission script
│   └── run_job.ps1                          # Windows PowerShell MapReduce execution script
│
├── preprocessing/
│   ├── dataset_profile.py                   # Exploratory data profiling & distribution analysis
│   ├── create_baseline.py                   # Phase 1: Combine train/test & schema validation
│   ├── feature_extraction.py                # Phase 6: Post-level feature engineering
│   └── validate_pipeline.py                 # Phase 7: Automated QA, preservation & sanity checks
│
├── grafana/
│   ├── dashboard.json                       # Phase 8: 5-panel Grafana dashboard definition
│   └── README.md                            # Grafana datasource setup and import guide
│
├── requirements.txt                         # Project Python dependencies
└── README.md                                # Comprehensive documentation & Viva guide
```

---

## 🚀 Execution Guide (Phases 1 to 8)

### Prerequisites
Install the required Python dependencies:
```bash
pip install -r requirements.txt
```

---

### Phase 1 — Dataset Profiling & Standardized Baseline CSV

1. **Profile the raw datasets**:
   ```bash
   python preprocessing/dataset_profile.py
   ```
2. **Generate the standardized baseline CSV**:
   ```bash
   python preprocessing/create_baseline.py
   ```
   - **Input**: `data/raw/train.csv` (2,838 rows) and `data/raw/test.csv` (715 rows).
   - **Output**: `data/baseline/baseline_stress_logs.csv` (3,553 rows).
   - **Guaranteed schema**: `post_id`, `subreddit`, `social_timestamp`, `text`, `label`, `confidence`, `social_karma`, `social_num_comments`, `syntax_ari`.
   - **Crucial Rule**: Original text is preserved without modifications at this stage.

---

### Phase 2 — Real-time Kafka Streaming Producer

Streams the baseline records record-by-record into Kafka topic `stress_logs`:
```bash
python kafka_producer.py
# or
python kafka/producer.py --delay 0.0005
```
- Converts each row to an individual JSON event payload.
- Connects to Kafka cluster (`localhost:9092`) when active, with built-in streaming simulation fallback when offline.

---

### Phase 3 — Kafka Consumer → Partitioned HDFS Persistent Storage

Consumes from `stress_logs` and writes partitioned line-delimited JSON batches into HDFS:
```bash
python kafka_to_hdfs.py
# or
python kafka/consumer.py --batch-size 500
```
- **HDFS Hierarchy**: `/data/raw/stress_logs/part-00000` to `part-00007`.
- **Architectural Viva Note**:
  - **Kafka**: Real-time distributed streaming transport (ephemeral message buffer).
  - **HDFS**: Distributed, fault-tolerant persistent storage for big data batch analytics.

---

### Phase 4 & Phase 5 — MapReduce Preprocessing & Reducer Aggregations

Executes MapReduce preprocessing and aggregation:
```bash
python mapreduce/run_mapreduce.py
```
*(On an Apache Hadoop cluster, submit using `./mapreduce/run_job.sh`)*

#### Mapper Transformations (`mapreduce/mapper.py`):
1. **Timestamp Conversion**: Unix timestamp $\rightarrow$ ISO `YYYY-MM-DD` (`entry_date`).
2. **Text Cleaning**: Strips URLs (`https?://\S+`), unescapes and removes HTML entities/tags (`&amp;`, `<[^>]+>`), strips non-alphanumeric noise, and collapses whitespace.
3. **Word Count**: Calculates token count on the cleaned string.
4. **Karma Engagement Ratio**: $\text{karma\_engagement\_ratio} = \frac{\text{social\_karma}}{\text{social\_num\_comments}}$. Explicitly handles $\text{comments} = 0 \implies \text{ratio} = 0.0$.
5. **Stress Flag**: Maps dataset label $1 \rightarrow \text{Stress}$, $0 \rightarrow \text{Non-stress}$.
6. **High-Confidence Flag**: Sets flag to $1$ if annotator agreement $\ge 0.80$.

#### Reducer Aggregations (`mapreduce/reducer.py`):
- **Grouping Key**: `(subreddit, entry_date)`.
- **Aggregations Computed**:
  - `avg_readability_index` $= \frac{\sum \text{syntax\_ari}}{N}$
  - `avg_karma_ratio` $= \frac{\sum \text{karma\_ratio}}{N}$
  - `stress_percentage` $= \frac{\text{stressed\_posts}}{N} \times 100\%$
  - `high_conf_stressed_posts`
- **Outputs**:
  - `data/processed/stress_features/daily_aggregated_metrics.csv` (1,786 daily groups)
  - `data/processed/stress_features/subreddit_summary.csv` (Community-level ranking)

---

### Phase 6 — Final Feature Extraction

Generates the standardized analytical dataset:
```bash
python preprocessing/feature_extraction.py
```
- **Output File**: `data/processed/processed_stress_analytics.csv` (3,553 rows).
- **Exact Columns**:
  1. `post_id`
  2. `subreddit`
  3. `entry_date`
  4. `word_count`
  5. `avg_readability_index`
  6. `karma_engagement_ratio`
  7. `stress_flag`
  8. `high_confidence_flag`
- **High-Confidence Threshold Documentation**: In Dreaddit (Turcan & McKeown, EMNLP 2019), each post was evaluated by 5 MTurk annotators. A threshold of $\ge 0.80$ represents that at least 4 out of 5 annotators (80%+ consensus) unanimously classified the post as stressed.

---

### Phase 7 — Automated Pipeline Quality Assurance & Validation

Runs comprehensive verification checks:
```bash
python preprocessing/validate_pipeline.py
```
- **Audit 1 (Record Preservation)**: Confirms Raw (3,553) == Baseline (3,553) == HDFS (3,553) == MapReduce (3,553) == Final (3,553) with **Delta = 0**.
- **Audit 2 (Missing Values)**: Zero missing values across all 8 columns.
- **Audit 3 (Range Checks)**: `stress_flag` $\in \{0, 1\}$, `high_confidence_flag` $\in \{0, 1\}$, `word_count` $\ge 0$, `karma_engagement_ratio` $\ge 0.0$.
- **Audit 4 (Aggregation Sanity)**: Manual calculations match MapReduce Reducer outputs to 100% precision.

---

### Phase 8 — Grafana Dashboard Visualization

Import `grafana/dashboard.json` into Grafana to explore the 5 specialized panels:

1. **Panel 1 — Daily Stress Trend (Time Series)**:
   - X-axis: `entry_date`, Y-axis: `stress_percentage`, split by `subreddit`.
2. **Panel 2 — Subreddit Stress Comparison (Bar Chart)**:
   - Compares community stress levels: `r/domesticviolence` (64.18%), `r/anxiety` (64.00%), `r/almosthomeless` (59.60%), `r/ptsd` (58.23%) down to `r/assistance` (35.49%).
3. **Panel 3 — Readability Complexity Distribution (Bar Chart)**:
   - `subreddit` vs `avg_readability_index` (ARI reading grade level).
4. **Panel 4 — Social Engagement Heatmap (Heatmap)**:
   - `subreddit × entry_date` grid with cell intensity driven by `avg_karma_ratio`.
5. **Panel 5 — High-Confidence Stress Volume (Stacked Timeseries)**:
   - Tracks high-confidence stressed post volume over time and across subreddits.

---

## 📊 Summary of Pipeline Findings

| Subreddit | Total Posts | Stressed Posts | Stress Rate (%) | Avg Readability (ARI) | Avg Karma Ratio | High-Confidence Stressed |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **r/domesticviolence** | 388 | 249 | **64.18%** | 4.21 | 1.53 | 161 |
| **r/anxiety** | 650 | 416 | **64.00%** | 4.62 | 1.54 | 317 |
| **r/almosthomeless** | 99 | 59 | **59.60%** | 4.76 | 2.13 | 42 |
| **r/ptsd** | 711 | 414 | **58.23%** | 4.87 | 1.85 | 284 |
| **r/stress** | 78 | 45 | **57.69%** | 5.66 | 0.90 | 33 |
| **r/survivorsofabuse** | 315 | 143 | **45.40%** | 5.44 | 2.73 | 84 |
| **r/relationships** | 694 | 307 | **44.24%** | 4.37 | 1.51 | 179 |
| **r/food_pantry** | 43 | 17 | **39.53%** | 3.94 | 1.92 | 11 |
| **r/homeless** | 220 | 81 | **36.82%** | 4.48 | 1.64 | 53 |
| **r/assistance** | 355 | 126 | **35.49%** | 4.92 | 1.45 | 83 |
| **Total / Overall** | **3,553** | **1,857** | **52.27%** | **4.69** | **1.73** | **1,247** |

---

## 🎓 Key Viva Discussion Questions & Answers

1. **Why use Kafka and HDFS together instead of writing directly to HDFS?**
   - *Kafka* is an optimized, high-throughput distributed pub-sub streaming log designed for continuous event ingestion with minimal latency. Direct writes from thousands of concurrent clients to HDFS create an overwhelming number of small files, severely degrading HDFS NameNode metadata performance.
   - *HDFS* is a batch-oriented, append-only distributed storage system optimized for large sequential block reads ($128\text{MB}$ blocks) required by MapReduce. Ingesting via Kafka allows a streaming consumer to batch records into consolidated partitions before committing to HDFS.

2. **Why group by `(subreddit, entry_date)` in the Reducer instead of only `subreddit`?**
   - Grouping solely by `subreddit` loses the temporal dimension, making it impossible to analyze daily fluctuations, viral spikes, or temporal patterns.
   - Grouping by `(subreddit, entry_date)` provides fine-grained daily analytics while still enabling straightforward rollups to community-level summaries.

3. **How is the Karma Engagement Ratio defined and why is $0$ comments an edge case?**
   - Defined as $\frac{\text{social\_karma}}{\text{social\_num\_comments}}$. When a post receives karma (upvotes) but zero comments, dividing by zero causes an arithmetic fault.
   - In our pipeline, we explicitly check `if comments == 0: ratio = 0.0`, ensuring robust data normalization without crashing streaming workers.

4. **What is the justification for the $0.80$ High-Confidence threshold?**
   - Dreaddit crowd-sourced annotations utilized 5 independent MTurk annotators per post snippet.
   - A score of $0.80$ indicates that 4 out of 5 annotators ($\ge 80\%$) agreed on the psychological stress categorization, representing statistically significant consensus.
