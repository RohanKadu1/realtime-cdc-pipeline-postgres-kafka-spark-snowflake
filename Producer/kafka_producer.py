import os
import json
import re
import logging
import signal
import sys
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from psycopg2 import errors
from kafka import KafkaProducer
load_dotenv()
# ----------------------------
# 1. Logging Configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# 2. Environment Variables
# ----------------------------
PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "database": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "connection_factory": psycopg2.extras.LogicalReplicationConnection
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
REPLICATION_SLOT = os.getenv("REPLICATION_SLOT")
TARGET_TABLE = os.getenv("TARGET_TABLE")

# ----------------------------
# 3. Kafka Producer (Reliable)
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all',
    retries=5
)

# ----------------------------
# 4. Payload Parser
# ----------------------------
def parse_postgres_payload(payload: str):
    """
    Example Input:
    table public.users: INSERT: id[integer]:5 name[text]:'Rohan'

    Output:
    {
        "action": "INSERT",
        "id": 5,
        "name": "Rohan"
    }
    """
    action_match = re.search(r': (INSERT|UPDATE|DELETE):', payload)
    if not action_match:
        return None

    action = action_match.group(1)

    pattern = r"(\w+)\[.*?\]:(?:'([^']*)'|(null)|(\S+))"
    matches = re.findall(pattern, payload)

    data = {"action": action}

    for match in matches:
        key = match[0]

        if match[1]:  # quoted string
            value = match[1]
        elif match[2]:  # null
            value = None
        else:
            value = match[3]

            # Try converting numeric values
            if value.isdigit():
                value = int(value)

        data[key] = value

    return data

# ----------------------------
# 5. Message Processor
# ----------------------------
def process_message(msg):
    try:
        payload = msg.payload

        # Ignore transaction boundaries
        if payload.startswith(("BEGIN", "COMMIT")):
            return

        # Only process specific table
        if not payload.startswith(f"table {TARGET_TABLE}:"):
            return

        structured_data = parse_postgres_payload(payload)

        if structured_data:
            structured_data["lsn_num"] = msg.data_start
            future = producer.send(KAFKA_TOPIC, value=structured_data)
            future.get(timeout=10)  # Ensure delivery
            logging.info(f"Sent to Kafka: {structured_data}")

    except Exception as e:
        logging.error(f"Error processing message: {e}")

    finally:
        # Always acknowledge
        msg.cursor.send_feedback(write_lsn=msg.data_start)

# ----------------------------
# 6. Graceful Shutdown
# ----------------------------
def shutdown_handler(signum, frame):
    logging.info("Shutting down gracefully...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ----------------------------
# 7. Main Execution
# ----------------------------
def main():
    conn = None

    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()

        # Create slot if not exists
        try:
            cur.create_replication_slot(
                REPLICATION_SLOT,
                output_plugin='test_decoding'
            )
            logging.info("Replication slot created")
        except errors.DuplicateObject:
            logging.info("Replication slot already exists")

        # Start replication
        cur.start_replication(slot_name=REPLICATION_SLOT, decode=True)
        logging.info("✅ CDC Pipeline Active: PostgreSQL → Kafka")

        cur.consume_stream(process_message)

    except Exception as e:
        logging.error(f"Fatal error: {e}")

    finally:
        if conn:
            conn.close()
        producer.flush()
        producer.close()

# ----------------------------
# 8. Entry Point
# ----------------------------
if __name__ == "__main__":
    main()