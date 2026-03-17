FROM apache/spark:3.5.0
USER root
RUN pip install psycopg2-binary
WORKDIR /app
COPY batch_job.py .