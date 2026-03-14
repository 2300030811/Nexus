#!/bin/bash
set -e

echo "Starting database migrations..."
# The migrations are in /docker-entrypoint-initdb.d/migrations
# We can run the python script from there if we have python installed in the postgres image.
# The alpine image has python? Usually not.

# Let's check if the alpine postgres image has python. 
# Usually it doesn't. 
# But we can use psql to apply them in a loop if we want, 
# or use the provided python runner.

# If we want to use the python runner, we might need a separate container or 
# install python in the postgres container.

# Alternative: run them via psql in this shell script.

export PGHOST=localhost
export PGUSER=${POSTGRES_USER:-nexus}
export PGDATABASE=${POSTGRES_DB:-nexus}
export PGPASSWORD=${POSTGRES_PASSWORD:-nexus_password}

# Function to run a SQL file and log it
run_sql_file() {
    local file=$1
    local version=$(basename "$file" | cut -d'_' -f1)
    
    # Check if already applied
    if psql -t -c "SELECT 1 FROM schema_migrations WHERE version='$version'" | grep -q 1; then
        echo "Migration $version already applied."
    else
        echo "Applying migration $file..."
        psql -f "$file"
        psql -c "INSERT INTO schema_migrations (version, filename) VALUES ('$version', '$(basename "$file")')"
    fi
}

# Create tracking table if not exists
psql -c "CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(10) PRIMARY KEY, filename TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT NOW())"

# Find and run all V*.sql files in order
# They are mounted at /docker-entrypoint-initdb.d/migrations
for f in $(ls /docker-entrypoint-initdb.d/migrations/V*.sql | sort); do
    run_sql_file "$f"
done

echo "All migrations applied."
