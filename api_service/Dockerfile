FROM python:3.11-slim

WORKDIR /app

COPY api_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy common module and API code
COPY common/ /app/common/
COPY scripts/ /app/scripts/
COPY api_service/main.py /app/main.py
COPY api_service/rate_limit.py /app/rate_limit.py

RUN python /app/scripts/validate_secrets.py || exit 1

EXPOSE 8000

# Run Uvicorn worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
