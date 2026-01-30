import json
import pandas as pd
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ------------------------
# Kafka configuration
# ------------------------
BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "proper_events"
GROUP_ID = "snowflake-loader"

# ------------------------
# Snowflake connection settings (INSERT SNOWFLAKE CREDENTIALS)
# ------------------------
SNOWFLAKE_CONFIG = {
    "user": " ",
    "password": " ",
    "account": " ",
    "warehouse": " ",
    "database": " ",
    "schema": " "
}

BATCH_SIZE = 10
TARGET_TABLE = "KAFKA_EVENTS_SILVER"

# ------------------------
# Initialize Kafka consumer
# ------------------------
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ------------------------
# Connect to Snowflake
# ------------------------
sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
print("Connected to Snowflake")
print("Starting Kafka → Snowflake Loader...")

# ------------------------
# Buffer & helper function
# ------------------------
event_buffer = []

def flush_to_snowflake(batch):
    df = pd.DataFrame(batch)
    df.columns = [col.upper() for col in df.columns]

    success, *_ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name=TARGET_TABLE
    )

    if not success:
        raise RuntimeError("Snowflake insert failed")
    
    print(f"Inserted {len(batch)} rows into Snowflake")

# ------------------------
# Main loop
# ------------------------
for msg in consumer:
    payload = msg.value

    # Add record to buffer
    event_buffer.append({
        "event_id": payload["event_id"],
        "user_id": payload["user_id"],
        "event_type": payload["event_type"],
        "transaction_amount": payload["transaction_amount"],
        "currency_code": payload["currency_code"],
        "event_timestamp": payload["event_timestamp"]
    })

    # Flush buffer if batch size reached
    if len(event_buffer) >= BATCH_SIZE:
        try:
            flush_to_snowflake(event_buffer)
            consumer.commit()
            event_buffer.clear()
        except Exception as e:
            print(f"ERROR inserting batch: {e}")
