import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# ------------------------
# Kafka configuration
# ------------------------
KAFKA_SERVERS = "localhost:29092"
TOPIC_NAME = "brut_events"

producer_client = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ------------------------
# Event types
# ------------------------
VALID_EVENTS = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENTS = ["CLICK", "VIEW", "PAY"]

# ------------------------
# Generate random timestamp within last 6 days
# ------------------------
def random_recent_timestamp():
    now = datetime.now(timezone.utc)  # UTC-aware datetime
    six_days_ago = now - timedelta(days=6)
    random_seconds = random.uniform(0, (now - six_days_ago).total_seconds())
    return six_days_ago + timedelta(seconds=random_seconds)

# ------------------------
# Generate one random event
# ------------------------
def create_event():
    # 25% chance the event will be invalid
    invalid_flag = random.random() < 0.25

    user_id = f"USER_{random.randint(1,5)}"
    event_type = random.choice(VALID_EVENTS)
    transaction_amount = round(random.uniform(10,500), 2)
    currency_code = "USD"

    invalid_field = None
    if invalid_flag:
        invalid_field = random.choice(["user_id", "event_type", "transaction_amount", "currency_code"])

    event_record = {
        "event_id": str(uuid.uuid4()),
        "user_id": None if invalid_field == "user_id" else user_id,
        "event_type": (
            random.choice(INVALID_EVENTS)
            if invalid_field == "event_type"
            else event_type
        ),
        "transaction_amount": (
            random.uniform(-500, -10)
            if invalid_field == "transaction_amount"
            else transaction_amount
        ),
        "currency_code": None if invalid_field == "currency_code" else currency_code,
        "event_timestamp": random_recent_timestamp().isoformat(),
        "is_valid": not invalid_flag,
        "invalid_field": invalid_field
    }

    return event_record["user_id"], event_record

# ------------------------
# Main producer loop
# ------------------------
print("Starting Kafka producer...")

while True:
    event_key, event_payload = create_event()

    producer_client.send(
        topic=TOPIC_NAME,
        key=event_key,
        value=event_payload
    )

    print(f"Produced event | key={event_key} | valid={event_payload['is_valid']}")

    time.sleep(1)
