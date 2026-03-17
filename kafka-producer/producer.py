import os, time, json, logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_HOST      = os.getenv("POSTGRES_HOST", "postgres")
PG_DB        = os.getenv("POSTGRES_DB",   "taxidb")
PG_USER      = os.getenv("POSTGRES_USER", "admin")
PG_PASS      = os.getenv("POSTGRES_PASSWORD", "admin123")

def wait_for_kafka():
    for i in range(20):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            p.close()
            log.info("Kafka is ready.")
            return
        except NoBrokersAvailable:
            log.info(f"Waiting for Kafka... attempt {i+1}")
            time.sleep(5)
    raise Exception("Kafka not available after 100 seconds")

def create_table():
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id SERIAL PRIMARY KEY,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Table raw_data created.")

def ingest_data():
    wait_for_kafka()
    create_table()
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )
    
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    log.info("Loading dataset...")
    df = pd.read_csv("/data/yellow_tripdata_2020-01.csv", nrows=50000)
    df = df.rename(columns={
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID":          "pickup_location_id",
        "DOLocationID":          "dropoff_location_id"
    })
    df = df[["pickup_datetime","dropoff_datetime","passenger_count",
             "trip_distance","fare_amount","tip_amount","total_amount",
             "pickup_location_id","dropoff_location_id"]].dropna()
    
    log.info(f"Ingesting {len(df)} records...")
    for i, row in df.iterrows():
        record = row.to_dict()
        # Send to Kafka topic
        producer.send("raw-data", record)
        # Also write directly to PostgreSQL
        cur.execute("""
            INSERT INTO raw_data (pickup_datetime, dropoff_datetime, passenger_count,
                trip_distance, fare_amount, tip_amount, total_amount,
                pickup_location_id, dropoff_location_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (record["pickup_datetime"], record["dropoff_datetime"],
              record.get("passenger_count",1), record["trip_distance"],
              record["fare_amount"], record["tip_amount"], record["total_amount"],
              record["pickup_location_id"], record["dropoff_location_id"]))
        
        if i % 1000 == 0:
            conn.commit()
            log.info(f"  {i} records processed...")
    
    conn.commit()
    producer.flush()
    cur.close()
    conn.close()
    log.info("Ingestion complete!")

if __name__ == "__main__":
    ingest_data()