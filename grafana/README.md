# Grafana Dashboard Setup Guide (BDA-MH-K5)

This directory contains the production-ready Grafana Dashboard definition for the **Big Data Analytics — Social Media Mental Health & Stress Analysis** pipeline.

## Dashboard Overview (`dashboard.json`)

The dashboard features **4 Executive KPI Stat Cards** and **5 Deep-Dive Analytical Panels**:

| Panel | Name | Visualization | Description & Schema Mapping |
| :--- | :--- | :--- | :--- |
| **KPI 1** | Total Posts Analyzed | Stat Badge | Count of total records ingested (3,553 posts). |
| **KPI 2** | Overall Stress Rate | Stat Gauge | Overall percentage of stressed posts across all domains (52.27%). |
| **KPI 3** | High-Confidence Stressed Posts | Stat Badge | Posts meeting the $\ge 0.80$ consensus agreement threshold (1,247 posts). |
| **KPI 4** | Mean Readability Index (ARI)| Stat Badge | Average Automated Readability Index (4.69, Grade 5 level). |
| **Panel 1** | **Daily Stress Trend** | Time Series | X-axis: `entry_date`, Y-axis: `stress_percentage`, split by `subreddit`. Tracks daily spikes and temporal fluctuations in stress levels. |
| **Panel 2** | **Subreddit Stress Comparison** | Bar Chart | `subreddit` vs `stress_percentage`. Compares stress rates across communities (e.g., `r/domesticviolence` 64.18%, `r/anxiety` 64.00%, `r/ptsd` 58.23% down to `r/assistance` 35.49%). |
| **Panel 3** | **Readability Distribution** | Bar Chart | `subreddit` vs `avg_readability_index`. Explores syntactic and structural text complexity (Grade level) across subreddits. |
| **Panel 4** | **Engagement Heatmap** | Heatmap | `subreddit × entry_date` grid with color intensity driven by `avg_karma_ratio`. Visualizes community engagement dynamics and viral discussions. |
| **Panel 5** | **High-Confidence Stress** | Stacked Bars / Time Series | High-confidence stressed posts (`high_confidence_flag = 1`) over time and subreddit. |

---

## How to Import into Grafana

### Option A: Using the Infinity Data Source (Recommended for CSVs)
1. In Grafana, navigate to **Configuration > Plugins** and install the **Infinity** plugin (`yesoreyeram-infinity-datasource`).
2. Add a new Infinity datasource named `bda-infinity-ds`.
3. Go to **Dashboards > Import**.
4. Upload `grafana/dashboard.json` or paste the JSON contents.
5. Select `bda-infinity-ds` as the datasource and click **Import**.

### Option B: Using SQLite / PostgreSQL / MySQL
If you store `processed_stress_analytics.csv` and `daily_aggregated_metrics.csv` in a relational database:
1. Import the CSV files into tables:
   ```sql
   CREATE TABLE processed_stress_analytics (
       post_id VARCHAR(50),
       subreddit VARCHAR(50),
       entry_date DATE,
       word_count INT,
       avg_readability_index DOUBLE PRECISION,
       karma_engagement_ratio DOUBLE PRECISION,
       stress_flag INT,
       high_confidence_flag INT
   );

   CREATE TABLE daily_aggregated_metrics (
       subreddit VARCHAR(50),
       entry_date DATE,
       total_posts INT,
       avg_ari DOUBLE PRECISION,
       avg_karma_ratio DOUBLE PRECISION,
       stress_percentage DOUBLE PRECISION,
       high_conf_stressed_posts INT
   );
   ```
2. In Grafana, configure your SQL datasource and point the panel queries to these tables.
