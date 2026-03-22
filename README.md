# Data Engineering Portfolio – Batch Processing Pipeline
## DLMDSEDE02 | Task 1

## Overview
A batch-processing data architecture for a quarterly ML pipeline.
Processes NYC Taxi Trip data (112M+ records) using Apache Kafka,
Spark, PostgreSQL, Airflow, and FastAPI — all containerized with Docker.

## Architecture
Data Source → Kafka → PostgreSQL (raw) → Spark → PostgreSQL (aggregated) → FastAPI → ML App

## Project Structure
data-engineering-batch-pipeline/
│
├── 📄 docker-compose.yml           # IaC — defines all 7 microservices
├── 📄 README.md                    # This file
│
├── 📂 kafka-producer/
│   ├── 📄 Dockerfile               # python:3.11-slim image
│   └── 📄 producer.py              # Reads CSV → Kafka topic + PostgreSQL
│
├── 📂 spark-processor/
│   ├── 📄 Dockerfile               # apache/spark:3.5.0 image
│   └── 📄 batch_job.py             # GroupBy aggregations via JDBC
│
├── 📂 api/
│   ├── 📄 Dockerfile               # python:3.11-slim image
│   └── 📄 main.py                  # 5 REST endpoints (FastAPI)
│
├── 📂 airflow/
│   └── 📂 dags/
│       └── 📄 batch_dag.py         # Quarterly DAG scheduler
│
├── 📂 jars/
│   └── 📄 postgresql-42.7.3.jar    # PostgreSQL JDBC driver for Spark
│
└── 📂 data/
    └── 📄 yellow_tripdata_2020-01.csv   # Dataset (not tracked in Git)

## Quick Start (run the whole system)

# 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/data-engineering-batch-pipeline.git
cd data-engineering-batch-pipeline
```
# 2. Add dataset to /data folder (NYC Taxi CSV from Kaggle)
https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019?select=yellow_tripdata_2020-01.csv

# 3. Start all services
```bash
docker-compose up --build
```
# 4. Check the API
curl http://localhost:8000/health
curl http://localhost:8000/aggregated/summary
```

## Services
| Service | Port | Purpose |
|---|---|---|
| Kafka | 9092 | Message broker |
| PostgreSQL | 5432 | Data storage |
| Spark | 8080 | Batch processing |
| Airflow | 8081 | Job scheduling |
| FastAPI | 8000 | Data delivery |

## Run Spark batch job manually
```bash
docker exec spark spark-submit --master local[*] /app/batch_job.py
```

## API Endpoints
- `GET /` — Health check
- `GET /raw-data/count` — Count of ingested records
- `GET /aggregated` — Aggregated data (supports ?year=&month= filters)
- `GET /aggregated/summary` — Monthly summary for ML app
