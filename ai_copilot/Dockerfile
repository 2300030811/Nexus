FROM python:3.11-slim

WORKDIR /app

COPY ai_copilot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY ai_copilot/tools.py .
COPY ai_copilot/copilot.py .
COPY scripts/ /app/scripts/

RUN python /app/scripts/validate_secrets.py || exit 1

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import psycopg2; print('healthy')" || exit 1

CMD ["python", "-u", "copilot.py"]
