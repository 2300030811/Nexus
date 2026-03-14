# tests/integration/conftest.py
import pytest
import psycopg2
import time
import os

TEST_DB = {
    "host": "localhost",
    "port": 5433,
    "dbname": "nexus_test",
    "user": "nexus",
    "password": "nexus_test_pass",
}

@pytest.fixture(scope="session")
def db_conn():
    """Provides a real PostgreSQL connection for integration tests."""
    # Wait for test-postgres to be ready
    for attempt in range(30):
        try:
            conn = psycopg2.connect(**TEST_DB)
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        pytest.fail("test-postgres did not become ready in time")

    # Apply schema
    # Note: we use V001_initial_schema.sql as the base
    schema_path = os.path.join("data_warehouse", "migrations", "V001__initial_schema.sql")
    if not os.path.exists(schema_path):
        # Fallback for if it's running from a different level
        schema_path = "../../data_warehouse/migrations/V001__initial_schema.sql"
        
    schema = open(schema_path).read()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()

    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def clean_tables(db_conn):
    """Truncate all tables between tests."""
    yield
    with db_conn.cursor() as cur:
        cur.execute("""
            TRUNCATE order_events, revenue_metrics, anomalies,
                     copilot_reports, feature_store, app_config
            RESTART IDENTITY CASCADE
        """)
        cur.execute("INSERT INTO app_config (key, value) VALUES ('simulate_stockout', 'false')")
    db_conn.commit()
