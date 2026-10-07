# Smart Maritime Analytics Platform

A Big Data pipeline that takes ship position signals (AIS), processes them with Kafka and Spark, stores them in HDFS and PostGIS, and shows the results on Superset dashboards. It also uses Spark MLlib to group vessels by behavior and flag the ones that look unusual.

Graduation project, NTI Big Data, 2026.

## 🎥 Demo Video

▶️ **[Watch the full project demo](https://drive.google.com/file/d/13VWCCYNCjUme78On0m5TT6-d7JqVPhTI/view?usp=sharing)**

## 🛠️ Technologies Used

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Spark MLlib](https://img.shields.io/badge/Spark%20MLlib-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-20A6C9?style=for-the-badge&logo=apachesuperset&logoColor=white)
![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![PostGIS](https://img.shields.io/badge/PostGIS-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## Why ship tracking is a Big Data problem

Every large ship carries an AIS radio device that broadcasts its position. The faster a ship moves, the more often it reports, so the data never stops and never slows down.

| Ship status | How often it reports |
|---|---|
| Anchored or docked | every 3 minutes |
| Sailing slowly (0–14 knots) | every 10 seconds |
| Sailing at moderate speed (14–23 knots) | every ~6 seconds |
| Sailing fast and turning | every ~2 seconds |

Worldwide that adds up to 100M+ position pings a day, and a busy port or strait can produce around 5,000 messages per second.

## The data

- **Source:** NOAA MarineCadastre historical AIS data
- **Period:** 7 days, from `AIS_2024_12_25.csv` to `AIS_2024_12_31.csv`
- **Size:** about 4.77 GB
- **Rows:** about 45.5 million
- **How it is used:** all the data comes from these 7 CSV files. No external API is used. A Python producer reads the files and sends the rows into Kafka, and Kafka and Spark take it from there.

## What it does

| Goal | What it does | Table |
|---|---|---|
| Fleet tracking | Shows the latest known position of every ship | `active_fleet_state` |
| Port dwell time | Detects when a ship enters and leaves a port, and how long it stayed | `port_dwell_times` |
| Speeding alerts | Flags ships going faster than the limit (20 knots) | `vessel_speed_alerts` |
| Behavior clustering | Groups ships by how they move and flags unusual ones | `vessel_behavior_clusters` |

The serving database has 14 tables in total. These four are the ones that matter most; the rest hold reference data and logs.

## How it works

```
7 NOAA AIS CSV files (Dec 25–31, 2024)
        ↓
Python producer
        ↓
Apache Kafka  (raw_ais_positions)
        ↓
Spark Structured Streaming
   ├─ PostGIS upsert    current position of each ship
   ├─ Kafka alerts      speeding events
   ├─ HDFS Parquet      raw archive, one folder per day
   └─ HDFS dead-letter  bad or malformed records
        ↓
Airflow daily batch (reads the HDFS archive)  →  Spark batch KPIs  +  Spark MLlib K-Means
        ↓
PostGIS analytical tables
        ↓
Apache Superset dashboards
```

The pipeline has two paths that read from the same raw data in HDFS (a Lambda architecture):

- **Speed path:** fleet tracking and speed alerts. Needs answers in seconds, so it runs on Spark Structured Streaming.
- **Batch path:** daily KPIs, dwell times and clustering. Needs to be exact, so it reprocesses full days once a day with Spark and Airflow.

Bad or malformed records are never dropped silently. They go to a dead-letter folder in HDFS so they can be checked later.

## ⚡ Loading 45.5M rows: from one CPU core to four

Loading the historical data into Kafka first took **3–4 hours**. Kafka was not the problem. The producer was a single Python process, and because of Python's GIL, CSV parsing, JSON serialization and Kafka sends all ran on one core while the other three sat idle.

The fix was one worker process per CSV file. Each worker has its own interpreter, its own GIL and its own Kafka producer, so the work spreads across all cores. It also uses `orjson` for serialization and lighter CSV parsing.

| | Before (single process) | After (multiprocessing) |
|---|---|---|
| CPU cores used | 1 of 4 | 4 of 4 |
| Throughput | ~5,000–7,000 rows/sec | **~28,464 rows/sec** |
| Load time | 3–4 hours | **under 30 minutes** |

> That is roughly **4–5x more throughput** (topic `raw_ais_positions`, 3 partitions, 1 broker). Nothing changed on the Kafka side. The whole gain came from using cores that were sitting idle.

## Daily batch pipeline

All the data comes from files, so the batch layer is where most of the analysis happens. Once the CSVs are loaded, they sit in HDFS as Parquet, one folder per day (`/raw/ais_historical/date=YYYY-MM-DD/`). Airflow processes one day at a time, so the 7 days of data mean 7 runs of the DAG `maritime_batch_kpi_pipeline`.

Each run has five tasks, in this order:

| # | Task | What it does | Output |
|---|---|---|---|
| 1 | `check_hdfs_partition_exists` | Looks for that day's folder in HDFS and stops the run right away if it is missing | none |
| 2 | `run_batch_kpi_job` | Spark job that calculates dwell time per port, speed stats per vessel type, traffic density per map cell, and speeding events | `port_dwell_times`, `fleet_daily_kpis`, `route_density_grid`, `vessel_speed_alerts` |
| 3 | `run_spark_ml_clustering_job` | Groups vessels with K-Means and flags anomalies (details below). The trained model is saved to HDFS | `vessel_behavior_clusters` |
| 4 | `verify_postgres_rows_written` | Fails the run if no rows were written to `fleet_daily_kpis` for that date | none |
| 5 | `pipeline_health_check` | Logs the status of Kafka, Spark, HDFS and PostGIS into the task log | task log |

A few details about the KPI job:
- A ship counts as being in a port when it is within 3 nautical miles of it (the default radius, it can be changed).
- Traffic density counts AIS pings inside 0.05° grid cells (also a default), which shows the busiest routes.
- It can also be run by hand with `spark-submit`, using `--exec-date`, `--port-radius-nm` and `--grid-cell-deg`.

**Safe to re-run.** Old rows for a date are deleted before the new ones are inserted, so running the same day twice never duplicates data. A failed task can simply be retried.

**Run it for one date**

```bash
docker compose exec airflow-scheduler airflow dags trigger -e 2024-12-25 maritime_batch_kpi_pipeline
```

Repeat with the other dates, up to `2024-12-31`. Follow the runs in the Airflow UI or with:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d maritime_batch_kpi_pipeline
```

**Check the results**

```bash
docker compose exec postgis psql -U maritime -d maritime -c "SELECT kpi_date, count(*) FROM fleet_daily_kpis GROUP BY kpi_date;"
```

This should list one row per processed date. Task logs are saved under `./airflow/logs/`.

## Vessel behavior clustering

The only machine learning in the project is Spark MLlib K-Means. It is unsupervised, so it doesn't need labeled data. For each day it:

1. Builds a feature vector (speed, course, heading, latitude, longitude) and scales it with `StandardScaler`
2. Trains K-Means with `k = 5`
3. Measures each vessel's distance to the center of its cluster
4. Flags a vessel as an anomaly when its distance is above the 95th percentile of its own cluster

The trained model is saved to HDFS and the results go to `vessel_behavior_clusters` in PostGIS.

## 📊 Dashboards

### Real-time Fleet Tracking

Vessel map (refreshes every 30 seconds), active vessel count, and the active vessels roster.

![Real-time Fleet Tracking](Dashboard/Real-time%20Fleet%20Tracking.png)

### Speed Alerts & Safety Violations

Total number of speeding vessels and a log of every event above 20 knots.

![Speed Alerts & Safety Violations](Dashboard/Speed%20Alerts%20%26%20Safety%20Violations.png)

### Port Congestion & Operational Metrics

Dwell time per harbor, plus daily speed and ping metrics by vessel type.

![Port Congestion & Operational Metrics](Dashboard/Port%20Congestion%20%26%20Operational%20Metrics.png)

## 🚀 Getting Started

**You need**
- Docker Engine 24.0+ and Docker Compose 2.20+
- Windows PowerShell 5.1+ (or an equivalent shell on Linux/macOS)
- 16 GB RAM and 4+ CPU cores

**Start everything**

```powershell
.\setup_and_run.ps1
```

The script checks Docker, starts the containers, waits for PostGIS and Kafka, takes HDFS out of safe mode, and installs the `pg8000` driver on the Spark nodes.

**Load the Superset dashboards**

```bash
docker compose exec superset python3 /tmp/setup_superset.py
```

**Run the daily pipeline**

See [Daily batch pipeline](#daily-batch-pipeline) above for how to run it for each date.

**Where to find things**

| Service | URL | Login |
|---|---|---|
| Superset | http://localhost:8089 | admin / admin |
| Airflow | http://localhost:8082 | admin / admin |
| Kafka UI | http://localhost:8090 | none |
| Spark Master | http://localhost:8080 | none |
| HDFS NameNode | http://localhost:9870 | none |
| Jupyter Lab | http://localhost:8888 | token: `lab` |

## 🔍 Try a query

Top 5 busiest ports by number of visits:

```bash
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT port_id,
       COUNT(*) AS total_visits,
       ROUND(AVG(dwell_minutes), 1) AS avg_dwell_minutes
FROM port_dwell_times
WHERE kpi_date = '2024-12-25'
GROUP BY port_id
ORDER BY total_visits DESC
LIMIT 5;"
```

## What could scale next

| Part | How it scales |
|---|---|
| Kafka | add partitions and brokers |
| Spark | add executors |
| HDFS | add DataNodes |
| PostGIS | partition tables, or move to Citus |
