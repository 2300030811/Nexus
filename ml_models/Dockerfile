FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_training_data.py .
COPY train_model.py .
COPY detect_anomalies.py .
COPY constants.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Directories for data and model artifacts
RUN mkdir -p /app/data /app/model

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD test -f /app/model/model.json && echo 'healthy' || echo 'initializing'

CMD ["./entrypoint.sh"]
