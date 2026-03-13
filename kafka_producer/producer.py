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
import logging
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
EVENTS_PRODUCED = Counter("nexus_events_produced_total", "Total events produced", ["topic"])
PRODUCE_ERRORS = Counter("nexus_produce_errors_total", "Total produce errors")
SIMULATION_GAUGE = Gauge("nexus_simulation_mode", "Stockout simulation active")

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts": self.formatTime(record),
            "level": record.levelname,
            "service": "producer",
            "msg": record.getMessage(),
        })

logger = logging.getLogger("nexus.producer")
logger.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(_JSONFormatter())
logger.addHandler(_h)
logger.propagate = False

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
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "nexus")
PG_USER = os.getenv("PG_USER", "nexus")
PG_PASSWORD = os.getenv("PG_PASSWORD", "nexus_password")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2.0"))

# ---------------------------------------------------------------------------
# Product catalog
# ---------------------------------------------------------------------------
PRODUCTS = [
    {"product_id": "SKU-1001", "name": "iPhone 15 Pro",       "category": "Electronics", "base_price": 999.00},
    {"product_id": "SKU-1002", "name": "MacBook Air M3",      "category": "Electronics", "base_price": 1199.00},
    {"product_id": "SKU-1003", "name": "AirPods Pro",         "category": "Electronics", "base_price": 249.00},
    {"product_id": "SKU-1004", "name": "Nike Air Max 90",     "category": "Footwear",    "base_price": 130.00},
    {"product_id": "SKU-1005", "name": "Levi's 501 Jeans",    "category": "Apparel",     "base_price": 69.50},
    {"product_id": "SKU-1006", "name": "Instant Pot Duo 7-in-1", "category": "Home",     "base_price": 89.99},
    {"product_id": "SKU-1007", "name": "Kindle Paperwhite",   "category": "Electronics", "base_price": 139.99},
    {"product_id": "SKU-1008", "name": "Dyson V15 Vacuum",    "category": "Home",        "base_price": 749.99},
    {"product_id": "SKU-1009", "name": "Yeti Rambler 26oz",   "category": "Accessories", "base_price": 35.00},
    {"product_id": "SKU-1010", "name": "Sony WH-1000XM5",     "category": "Electronics", "base_price": 348.00},
]

REGIONS = ["Delhi", "Maharashtra", "Karnataka", "Tamil Nadu", "West Bengal"]
PAYMENT_METHODS = ["credit_card", "debit_card", "digital_wallet", "buy_now_pay_later"]


def create_producer(broker: str, retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a KafkaProducer with retry logic and optimized batching."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1, # Wait for leader to acknowledge
                retries=3,
                batch_size=16384, # Standard batch
                linger_ms=10, # Lower linger for faster debugging
                compression_type=None, # Disable compression for compatibility check
            )
            logger.info("Connected to Kafka broker at %s", broker)
            return producer
        except NoBrokersAvailable:
            logger.warning("Broker not available (attempt %d/%d), retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka broker at {broker}")


def check_simulation_mode() -> bool:
    """Check if 'simulate_stockout' is enabled in the database."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_config WHERE key = 'simulate_stockout'")
            res = cur.fetchone()
            return res[0].lower() == 'true' if res else False
    except Exception as e:
        logger.warning("Could not check simulation mode: %s", e)
        return False
    finally:
        if 'conn' in locals():
            conn.close()


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


def main() -> None:
    # Start Prometheus metrics endpoint
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    logger.info("Prometheus metrics available on port %d", metrics_port)

    producer = create_producer(KAFKA_BROKER)
    logger.info("Producing events to '%s' at ~%.1f events/sec", KAFKA_TOPIC, EVENTS_PER_SECOND)

    event_count = 0
    simulate_stockout = False
    
    try:
        while not shutdown_flag:
            # Refresh simulation mode every 20 events (~10 seconds)
            if event_count % 20 == 0:
                simulate_stockout = check_simulation_mode()
                SIMULATION_GAUGE.set(1 if simulate_stockout else 0)
                if simulate_stockout:
                    logger.info("Simulation active: High-value Electronics stockout")

            event = generate_order_event(simulate_stockout)
            # Set message key to region for partition ordering
            event_key = event["region"].encode("utf-8")
            def on_success(record_metadata):
                pass # Already printing periodic updates
            
            def on_error(excp):
                logger.error("Kafka send failed: %s", excp)
                PRODUCE_ERRORS.inc()

            producer.send(KAFKA_TOPIC, key=event_key, value=event).add_callback(on_success).add_errback(on_error)
            EVENTS_PRODUCED.labels(topic=KAFKA_TOPIC).inc()
            event_count += 1

            if event_count % 10 == 0:
                logger.info("%d events sent | latest: %s – %s x%d = ₹%.2f",
                            event_count, event['order_id'], event['product_name'],
                            event['quantity'], event['total_amount'])
                producer.flush() # Force flush to catch errors faster during debugging

            time.sleep(1 / EVENTS_PER_SECOND)
    except KeyboardInterrupt:
        logger.info("Producer stopped after %d events", event_count)
    finally:
        logger.info("Flushing remaining messages")
        producer.flush()
        logger.info("Closing producer connection")
        producer.close()
        logger.info("Producer finished. Total events sent: %d", event_count)


if __name__ == "__main__":
    main()
