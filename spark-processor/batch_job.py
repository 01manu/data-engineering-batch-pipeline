import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as fsum, month, year

PG_URL  = "jdbc:postgresql://postgres:5432/taxidb"
PG_PROPS = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}

spark = SparkSession.builder \
    .appName("TaxiBatchProcessor") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/extra/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/extra/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Reading raw data from PostgreSQL...")

df = spark.read.jdbc(url=PG_URL, table="raw_data", properties=PG_PROPS)
print(f"Total records: {df.count()}")

# Create date columns
df = df.withColumn("pickup_month", month(col("pickup_datetime"))) \
       .withColumn("pickup_year",  year(col("pickup_datetime")))

# Aggregate by month and pickup location
aggregated = df.groupBy("pickup_year", "pickup_month", "pickup_location_id").agg(
    count("*").alias("total_trips"),
    avg("fare_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance"),
    avg("tip_amount").alias("avg_tip"),
    fsum("total_amount").alias("total_revenue"),
    avg("passenger_count").alias("avg_passengers")
).orderBy("pickup_year", "pickup_month", "pickup_location_id")

print(f"Aggregated into {aggregated.count()} rows.")
aggregated.show(10)

# Create the aggregated table and write results
aggregated.write.jdbc(
    url=PG_URL,
    table="aggregated_data",
    mode="overwrite",
    properties=PG_PROPS
)

print("Batch job complete! Aggregated data written to PostgreSQL.")
spark.stop()