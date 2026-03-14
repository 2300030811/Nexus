FROM python:3.11-slim

WORKDIR /app

COPY ml_models/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY ml_models/generate_training_data.py .
COPY ml_models/train_model.py .
COPY ml_models/detect_anomalies.py .
COPY ml_models/retrain_production_model.py .
COPY ml_models/entrypoint.sh .
COPY scripts/ /app/scripts/

RUN python /app/scripts/validate_secrets.py || exit 1
RUN chmod +x entrypoint.sh

# Directories for data and model artifacts
RUN mkdir -p /app/data /app/model

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD test -f /app/model/model.json && echo 'healthy' || echo 'initializing'

CMD ["./entrypoint.sh"]
