# kafka_producer/dlq.py
"""
Dead letter queue: failed events go to a PostgreSQL table,
with a background thread that periodically retries them.
"""

import json
import threading
import time
import queue
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import execute_values

from common.logging_utils import get_logger
from common.db_utils import get_connection_pool

logger = get_logger("nexus.dlq")


class DeadLetterQueue:
    """Optimized DLQ that batches writes and retries events in parallel."""

    def __init__(self, kafka_producer, topic: str,
                 retry_interval: int = 120, max_retries: int = 5,
                 batch_flush_interval: int = 5):
        self._producer = kafka_producer
        self._topic    = topic
        self._interval = retry_interval
        self._max_retries = max_retries
        
        # Buffer for fast intake without DB pressure
        self._queue = queue.Queue()
        self._batch_interval = batch_flush_interval
        
        self._pool = get_connection_pool(minconn=1, maxconn=5)
        self._ensure_table()
        
        # Threads
        self._stop_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._retry_thread  = threading.Thread(target=self._retry_loop, daemon=True)
        
        self._writer_thread.start()
        self._retry_thread.start()

    def _ensure_table(self):
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dlq_events (
                        id           SERIAL PRIMARY KEY,
                        event_data   JSONB NOT NULL,
                        error        TEXT,
                        retry_count  INTEGER NOT NULL DEFAULT 0,
                        first_failed TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_retried TIMESTAMPTZ,
                        resolved     BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                # Partial index for fast lookups of pending work
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dlq_pending 
                    ON dlq_events (first_failed) WHERE resolved = FALSE AND retry_count < 5
                """)
            conn.commit()
        finally:
            self._pool.putconn(conn)

    def put(self, event: dict, error: str):
        """Quickly buffer a failed event for batch insertion."""
        self._queue.put((event, str(error)))

    def _writer_loop(self):
        """Periodically flushes the internal queue to the database."""
        while not self._stop_event.is_set():
            batch = []
            try:
                # Collect as much as possible with a short timeout
                while len(batch) < 100:
                    try:
                        batch.append(self._queue.get(timeout=self._batch_interval))
                    except queue.Empty:
                        break
                
                if not batch:
                    continue

                conn = self._pool.getconn()
                try:
                    with conn.cursor() as cur:
                        execute_values(
                            cur,
                            "INSERT INTO dlq_events (event_data, error) VALUES %s",
                            [(json.dumps(e), err) for e, err in batch]
                        )
                    conn.commit()
                    logger.info("DLQ: Persisted batch of %d failures", len(batch))
                finally:
                    self._pool.putconn(conn)
                    
            except Exception as e:
                logger.error("DLQ writer error: %s", e)
                time.sleep(1)

    def _retry_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self._interval)
            self._retry_pending()

    def _retry_pending(self):
        """Retry pending events using parallel workers for I/O efficiency."""
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, event_data FROM dlq_events
                    WHERE resolved = FALSE AND retry_count < %s
                    ORDER BY first_failed LIMIT 100
                """, (self._max_retries,))
                rows = cur.fetchall()
            
            if not rows:
                return

            logger.info("DLQ retry: Processing %d events in parallel", len(rows))
            
            # Sub-process to avoid blocking the main producer
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self._retry_one, r[1]): r[0] for r in rows}
                
                resolved = []
                failed = [] # list of (id, error)
                
                for future in futures:
                    row_id = futures[future]
                    try:
                        future.result()
                        resolved.append(row_id)
                    except Exception as e:
                        failed.append((row_id, str(e)))

            # Update status in batches
            with conn.cursor() as cur:
                if resolved:
                    cur.execute("UPDATE dlq_events SET resolved = TRUE WHERE id = ANY(%s)", (resolved,))
                
                if failed:
                    for row_id, err in failed:
                        cur.execute("""
                            UPDATE dlq_events 
                            SET retry_count = retry_count + 1, 
                                last_retried = NOW(),
                                error = %s
                            WHERE id = %s
                        """, (err, row_id))
            conn.commit()
            
            if resolved:
                logger.info("DLQ: Successfully re-ingested %d events", len(resolved))
                
        except Exception as e:
            conn.rollback()
            logger.error("DLQ retry logic failed: %s", e)
        finally:
            self._pool.putconn(conn)

    def _retry_one(self, event_data):
        """Synchronously send one event to Kafka (called by executor)."""
        event = event_data if isinstance(event_data, dict) else json.loads(event_data)
        key = event.get("region", "").encode("utf-8")
        # Block here for the ack to ensure we don't mark as resolved too early
        self._producer.send(self._topic, key=key, value=event).get(timeout=10)

    def stop(self):
        self._stop_event.set()
        # Drain any remaining items to the database directly
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
                if len(batch) >= 100:
                    self._flush_batch(batch)
                    batch = []
            except queue.Empty:
                break
        if batch:
            self._flush_batch(batch)
            
        # Ensure connection pool is cleaned up
        if hasattr(self, '_pool'):
            from common.db_utils import close_connection_pool
            close_connection_pool()
        
        self._executor.shutdown(wait=True)

    def _flush_batch(self, batch):
        """Helper to flush a batch to the DB outside the main loop."""
        if not batch: return
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO dlq_events (event_data, error) VALUES %s",
                    [(json.dumps(e), err) for e, err in batch]
                )
            conn.commit()
            logger.info("DLQ shutdown: Flushed final batch of %d failures", len(batch))
        finally:
            self._pool.putconn(conn)
