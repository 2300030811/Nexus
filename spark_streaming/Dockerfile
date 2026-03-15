FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir pyspark==3.5.0 psycopg2-binary==2.9.9 prometheus-client==0.21.0

COPY common/ /app/common/
COPY scripts/ /app/scripts/
COPY spark_streaming/stream_processor.py /app/stream_processor.py
COPY spark_streaming/batch_health.py /app/batch_health.py

RUN python /app/scripts/validate_secrets.py || exit 1

WORKDIR /app

# Create non-root user and setup directories
RUN useradd -m appuser && \
    mkdir -p /opt/spark-checkpoints && \
    chown -R appuser:appuser /app /opt/spark-checkpoints /opt/bitnami/spark
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import pyspark; print('healthy')" || exit 1

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1", \
     "--master", "local[*]", \
     "stream_processor.py"]
