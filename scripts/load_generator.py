import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def generate_event(category=None, region=None, anomaly=False):
    categories = ["Electronics", "Clothing", "Home & Garden", "Groceries", "Beauty"]
    regions = ["North", "South", "East", "West", "Central"]
    
    cat = category or random.choice(categories)
    reg = region or random.choice(regions)
    
    # Baseline total amount between 10 and 500
    amount = random.uniform(10, 500)
    
    if anomaly:
        # Create a revenue drop anomaly (typical for retail monitoring)
        amount = amount * 0.1
    
    return {
        "event_id": f"evt_{random.getrandbits(32)}",
        "event_type": "order_placed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ord_{random.getrandbits(32)}",
        "product_id": f"prod_{random.getrandbits(16)}",
        "product_name": "Generic Product",
        "category": cat,
        "quantity": random.randint(1, 5),
        "unit_price": float(amount),
        "total_amount": float(amount),
        "region": reg,
        "payment_method": random.choice(["Credit Card", "UPI", "Cash"]),
        "schema_version": 1
    }

def main():
    parser = argparse.ArgumentParser(description="Nexus Load Generator")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="order_events", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (0 for infinite)")
    parser.add_argument("--anomaly-prob", type=float, default=0.01, help="Probability of an anomaly event")
    
    args = parser.parse_args()
    
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        enable_idempotence=True
    )
    
    print(f"Starting load generation at {args.rate} events/sec targeting {args.topic}...")
    
    start_time = time.time()
    count = 0
    
    try:
        while True:
            if args.duration > 0 and (time.time() - start_time) > args.duration:
                break
                
            is_anomaly = random.random() < args.anomaly_prob
            event = generate_event(anomaly=is_anomaly)
            
            producer.send(args.topic, value=event)
            count += 1
            
            if count % 100 == 0:
                print(f"Sent {count} events...")
                
            # Sleep to maintain rate
            time.sleep(1.0 / args.rate)
            
    except KeyboardInterrupt:
        print("Stopping load generator...")
    finally:
        producer.flush()
        producer.close()
        print(f"Load generation complete. Sent {count} total events.")

if __name__ == "__main__":
    main()
