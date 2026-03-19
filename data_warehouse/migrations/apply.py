"""Minimal migration runner — applies new SQL files in version order."""
import os
import sys
import glob
import psycopg2
from pathlib import Path

# Allow running directly from the data_warehouse/migrations/ directory
for _rel in ("..", "../.."):
    _p = os.path.normpath(os.path.join(os.path.dirname(__file__), _rel))
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.db_utils import get_db_config  # noqa: E402

MIGRATIONS_DIR = Path(__file__).parent

def run_migrations(conn):
    # Create migrations tracking table
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     VARCHAR(10) PRIMARY KEY,
                filename    TEXT NOT NULL,
                applied_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()

    # Find all migration files
    files = sorted(glob.glob(str(MIGRATIONS_DIR / "V*.sql")))

    with conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_migrations")
        applied = {row[0] for row in cur.fetchall()}

    # Validate unique versions in files
    versions_found = {}
    for path in files:
        filename = os.path.basename(path)
        version = filename.split("__")[0]
        if version in versions_found:
            raise RuntimeError(f"Duplicate migration version found: {version} ({versions_found[version]} and {filename})")
        versions_found[version] = filename

    for path in files:
        filename = os.path.basename(path)
        version = filename.split("__")[0]   # e.g. "V001"
        if version in applied:
            continue

        print(f"Applying migration {filename}...")
        sql = Path(path).read_text()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (version, filename) VALUES (%s, %s)",
                (version, filename)
            )
        conn.commit()
        print(f"  Applied {version}")

    print("All migrations up to date.")

if __name__ == "__main__":
    conn = psycopg2.connect(**get_db_config())
    run_migrations(conn)
    conn.close()
