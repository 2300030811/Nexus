FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libsnappy-dev g++ && rm -rf /var/lib/apt/lists/*
COPY kafka_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-snappy

COPY common/ /app/common/
COPY kafka_producer/producer.py .
COPY scripts/ /app/scripts/

RUN python /app/scripts/validate_secrets.py || exit 1

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import psycopg2; print('healthy')" || exit 1

CMD ["python", "-u", "producer.py"]
