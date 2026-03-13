FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    psycopg2 \
    pydantic

# Copy common module and API code
COPY common/ /app/common/
COPY api_service/main.py /app/main.py

EXPOSE 8000

# Run Uvicorn worker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
