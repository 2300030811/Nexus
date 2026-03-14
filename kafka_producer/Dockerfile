FROM python:3.11-slim

WORKDIR /app

# No longer need libsnappy-dev/g++ as we use lz4 wheel
COPY kafka_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY kafka_producer/producer.py .
COPY kafka_producer/dlq.py .
COPY scripts/ /app/scripts/

RUN python /app/scripts/validate_secrets.py || exit 1

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9090/metrics')" || exit 1

CMD ["python", "-u", "producer.py"]
