import json
from kafka import KafkaConsumer, KafkaProducer

# ------------------------
# Kafka configuration
# ------------------------
KAFKA_SERVERS = "localhost:29092"
SOURCE_TOPIC = "brut_events"
TARGET_TOPIC = "proper_events"
PROCESSOR_GROUP_ID = "silver_stream_processor"

# ------------------------
# Validation rules
# ------------------------
ALLOWED_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

# ------------------------
# Kafka consumer
# ------------------------
event_consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    group_id=PROCESSOR_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ------------------------
# Kafka producer
# ------------------------
event_producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ------------------------
# Event validation function
# ------------------------
def check_event_validity(event_data):
    if not event_data.get("user_id"):
        return False
    if event_data.get("event_type") not in ALLOWED_EVENT_TYPES:
        return False
    if event_data.get("transaction_amount") is None or event_data.get("transaction_amount") <= 0:
        return False
    if not event_data.get("currency_code"):
        return False
    if event_data.get("is_valid") is not True:
        return False
    return True

# ------------------------
# Main processing loop
# ------------------------
print("Starting Silver Stream Processor...")

for msg in event_consumer:
    msg_key = msg.key
    msg_value = msg.value

    if check_event_validity(msg_value):
        event_producer.send(
            topic=TARGET_TOPIC,
            key=msg_key,
            value=msg_value
        )
        print(f"FORWARDED | key={msg_key} | event_type={msg_value['event_type']}")
    else:
        print(f"DROPPED | key={msg_key} | reason=invalid")

    event_consumer.commit()
    