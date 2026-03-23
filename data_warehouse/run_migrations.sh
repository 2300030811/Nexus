#!/bin/sh
set -e

echo "Starting database migrations..."

# Wait for postgres to be ready
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  echo "Waiting for PostgreSQL..."
  sleep 1
done

# Run all migration files in order
for f in /docker-entrypoint-initdb.d/migrations/V*.sql; do
  if [ -f "$f" ]; then
    echo "Applying: $f"
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f" || true
  fi
done

echo "Migrations complete."
