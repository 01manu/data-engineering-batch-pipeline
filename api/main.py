import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from typing import Optional

app = FastAPI(
    title="NYC Taxi Data API",
    description="Serves aggregated taxi data for the ML application",
    version="1.0.0"
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST","postgres"),
        dbname=os.getenv("POSTGRES_DB","taxidb"),
        user=os.getenv("POSTGRES_USER","admin"),
        password=os.getenv("POSTGRES_PASSWORD","admin123")
    )

@app.get("/")
def root():
    return {"status": "ok", "message": "NYC Taxi Batch Pipeline API"}

@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/raw-data/count")
def raw_count():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_data")
    count = cur.fetchone()[0]
    cur.close(); conn.close()
    return {"total_raw_records": count}

@app.get("/aggregated")
def get_aggregated(year: Optional[int] = None, month: Optional[int] = None, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = "SELECT * FROM aggregated_data WHERE 1=1"
    params = []
    if year:
        query += " AND pickup_year = %s"; params.append(year)
    if month:
        query += " AND pickup_month = %s"; params.append(month)
    query += f" ORDER BY total_trips DESC LIMIT {limit}"
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return {"count": len(rows), "data": [dict(r) for r in rows]}

@app.get("/aggregated/summary")
def summary():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT pickup_year, pickup_month,
               SUM(total_trips) as total_trips,
               AVG(avg_fare) as avg_fare,
               AVG(avg_distance) as avg_distance,
               SUM(total_revenue) as total_revenue
        FROM aggregated_data
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return {"summary": [dict(r) for r in rows]}