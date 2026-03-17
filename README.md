# Data Engineering Portfolio – Batch Processing Pipeline
## DLMDSEDE02 | Task 1

## Overview
A batch-processing data architecture for a quarterly ML pipeline.
Processes NYC Taxi Trip data (112M+ records) using Apache Kafka,
Spark, PostgreSQL, Airflow, and FastAPI — all containerized with Docker.

## Architecture
Data Source → Kafka → PostgreSQL (raw) → Spark → PostgreSQL (aggregated) → FastAPI → ML App

## Quick Start (run the whole system)

# 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/data-engineering-batch-pipeline.git
cd data-engineering-batch-pipeline
```
# 2. Add dataset to /data folder (NYC Taxi CSV from Kaggle)

# 3. Start all services
docker-compose up --build

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
