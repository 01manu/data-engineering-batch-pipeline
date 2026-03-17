FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python pandas psycopg2-binary sqlalchemy
COPY producer.py .
CMD ["python", "producer.py"]