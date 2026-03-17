from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def check_data_quality():
    conn = psycopg2.connect(
        host="postgres", dbname="taxidb",
        user="admin", password="admin123"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_data")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"Data quality check: {count} records in raw_data")
    if count == 0:
        raise Exception("No data found in raw_data table!")
    return count

with DAG(
    dag_id="quarterly_batch_pipeline",
    default_args=default_args,
    description="Quarterly batch processing of NYC Taxi data",
    schedule_interval="0 0 1 1,4,7,10 *",  # quarterly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "taxi", "quarterly"],
) as dag:

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
    )

    run_spark_batch = BashOperator(
        task_id="run_spark_batch_job",
        bash_command="docker exec spark spark-submit --master local[*] /app/batch_job.py",
    )

    notify_complete = BashOperator(
        task_id="notify_completion",
        bash_command='echo "Batch pipeline complete at $(date). Data ready for ML application."',
    )

    data_quality_check >> run_spark_batch >> notify_complete