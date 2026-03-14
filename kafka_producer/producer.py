"""
Nexus – Kafka Order Event Producer

Simulates a stream of retail order events and publishes them to the
'order_events' Kafka topic.  Each event represents a single line-item
purchase with realistic variance in products, quantities, and pricing.
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from common.logging_utils import get_logger
from common.metrics import (
    EVENTS_PRODUCED, PRODUCE_ERRORS, PRODUCE_LATENCY,
    SIMULATION_ACTIVE as SIMULATION_GAUGE,
    start_metrics_server,
)
from common.constants import PRODUCTS, REGIONS, PAYMENT_METHODS
from common.db_utils import get_db_config, get_single_connection, close_connection
from dlq import DeadLetterQueue

# ---------------------------------------------------------------------------
# Structured logging (via shared utility)
# ---------------------------------------------------------------------------
logger = get_logger("nexus.producer")

# ---------------------------------------------------------------------------
# Global state for graceful shutdown
# ---------------------------------------------------------------------------
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global shutdown_flag
    logger.info("Received signal %s, initiating graceful shutdown", sig)
    shutdown_flag = True

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_db_config = get_db_config()
PG_HOST = _db_config['host']
PG_PORT = _db_config['port']
PG_DB = _db_config['dbname']
PG_USER = _db_config['user']
PG_PASSWORD = _db_config['password']

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2.0"))


def create_producer(broker: str, retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a KafkaProducer with retry logic and optimized batching."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1, # Wait for leader to acknowledge
                retries=5,
                batch_size=32768, # 32KB batches
                linger_ms=20, # Higher linger for better batching efficiency
                compression_type="lz4", # Efficient compression for JSON telemetry
            )
            logger.info("Connected to Kafka broker at %s", broker)
            return producer
        except NoBrokersAvailable:
            logger.warning("Broker not available (attempt %d/%d), retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka broker at {broker}")


from common.db_utils import get_single_connection, close_connection

class SimulationState:
    def __init__(self):
        self._conn = None

    def get_conn(self):
        """Return a persistent connection, reconnecting if necessary."""
        if self._conn is None or self._conn.closed:
            try:
                self._conn = get_single_connection()
            except Exception:
                self._conn = None
        return self._conn

    def check_mode(self) -> bool:
        """Check simulate_stockout flag using a persistent connection."""
        conn = self.get_conn()
        if conn is None:
            return False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM app_config WHERE key = 'simulate_stockout'"
                )
                res = cur.fetchone()
                return res[0].lower() == "true" if res else False
        except Exception as e:
            logger.warning("Simulation mode check failed: %s", e)
            if conn:
                close_connection(conn)
            self._conn = None
            return False

_sim_state = SimulationState()


def generate_order_event(simulate_stockout: bool = False) -> dict:
    """Return a single randomised order event, potentially filtering for simulation."""
    # Slightly boost Delhi's normal order frequency to make stockouts more dramatic
    region = "Delhi" if random.random() < 0.35 else random.choice(REGIONS)
    
    eligible_products = PRODUCTS

    if simulate_stockout and region == "Delhi":
        # Complete stockout of all Electronics in North India (Delhi)
        eligible_products = [p for p in PRODUCTS if p["category"] != "Electronics"]

    product = random.choice(eligible_products)
    
    # Normally 1-5, but reduce quantity for Delhi during stockouts for ALL items (supply chain issue)
    if simulate_stockout and region == "Delhi":
        quantity = 1
    else:
        # Sell more electronics normally for higher revenue baselines
        quantity = random.randint(2, 6) if product["category"] == "Electronics" else random.randint(1, 5)
        
    unit_price = round(product["base_price"] * random.uniform(0.95, 1.05), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_placed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(unit_price * quantity, 2),
        "region": region,
        "payment_method": random.choice(PAYMENT_METHODS),
    }


def make_error_handler(captured_event, dlq):
    """Factory to create an error handler that captures the current event."""
    def on_error(excp):
        logger.error("Kafka send failed: %s", excp)
        PRODUCE_ERRORS.inc()
        dlq.put(captured_event, str(excp))
    return on_error


def main() -> None:
    # Start Prometheus metrics endpoint
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_metrics_server(metrics_port)
    logger.info("Prometheus metrics available on port %d", metrics_port)

    producer = create_producer(KAFKA_BROKER)

    dlq = DeadLetterQueue(producer, KAFKA_TOPIC)

    logger.info("Producing events to '%s' at ~%.1f events/sec", KAFKA_TOPIC, EVENTS_PER_SECOND)

    event_count = 0
    simulate_stockout = False
    
    try:
        while not shutdown_flag:
            # Refresh simulation mode every 20 events (~10 seconds)
            if event_count % 20 == 0:
                simulate_stockout = _sim_state.check_mode()
                SIMULATION_GAUGE.set(1 if simulate_stockout else 0)
                if simulate_stockout:
                    logger.info("Simulation active: High-value Electronics stockout")

            event = generate_order_event(simulate_stockout)
            # Set message key to region for partition ordering
            event_key = event["region"].encode("utf-8")
            
            start_time = time.monotonic()
            
            def make_success_handler(start):
                def on_success(record_metadata):
                    PRODUCE_LATENCY.observe(time.monotonic() - start)
                return on_success
            
            producer.send(KAFKA_TOPIC, key=event_key, value=event) \
                .add_callback(make_success_handler(start_time)) \
                .add_errback(make_error_handler(event, dlq))
            EVENTS_PRODUCED.labels(topic=KAFKA_TOPIC).inc()
            event_count += 1

            if event_count % 50 == 0:
                logger.info("%d events sent | latest: %s – %s x%d = ₹%.2f",
                            event_count, event['order_id'], event['product_name'],
                            event['quantity'], event['total_amount'])
                producer.flush() # Periodic flush to catch send errors

            time.sleep(1 / EVENTS_PER_SECOND)
    except KeyboardInterrupt:
        logger.info("Producer stopped after %d events", event_count)
    finally:
        logger.info("Stopping DLQ writer/retry threads")
        dlq.stop()
        logger.info("Flushing remaining messages")
        producer.flush()
        logger.info("Closing producer connection")
        producer.close()
        logger.info("Producer finished. Total events sent: %d", event_count)


if __name__ == "__main__":
    main()
