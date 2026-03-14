FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY api_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy common module and API code
COPY common/ /app/common/
COPY scripts/ /app/scripts/
COPY api_service/main.py /app/main.py
COPY api_service/rate_limit.py /app/rate_limit.py

RUN python /app/scripts/validate_secrets.py || exit 1

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run Uvicorn worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
