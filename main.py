import psycopg2
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor
import json
import os
from datetime import datetime
import select

# Configuration
DB_NAME = "mydb"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
OUTPUT_DIR = "/home/sampath/Downloads/postgresql"
SLOT_NAME = "datalake_slot"

# Ensure output directories exist
for subdir in ["inserts", "updates", "deletes"]:
    os.makedirs(os.path.join(OUTPUT_DIR, subdir), exist_ok=True)

# Connect to PostgreSQL with replication
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    connection_factory=LogicalReplicationConnection
)
cur = conn.cursor(cursor_factory=ReplicationCursor)

# Start replication using wal2json — NO publication_names!
cur.start_replication(
    slot_name=SLOT_NAME,
    options={
        "pretty-print": "1",  # Optional, makes JSON more readable
        "include-lsn": "1",   # Optional: LSN info
        "include-xids": "1",  # Optional: transaction IDs
        "include-timestamp": "1"  # Optional: WAL timestamps
    }
)


def process_wal_message(msg):
    """Process a WAL message and write to the appropriate directory."""
    try:
        payload_data = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"Error decoding payload: {e}")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    if isinstance(payload_data, dict) and "change" in payload_data:
        for change in payload_data["change"]:
            kind = change.get("kind")  # "insert", "update", "delete"
            if kind == "insert":
                subdir = "inserts"
                data = change.get("columnvalues", {})
            elif kind == "update":
                subdir = "updates"
                data = change.get("columnvalues", {})
            elif kind == "delete":
                subdir = "deletes"
                data = change.get("oldkeys", {}).get("keyvalues", {})
            else:
                continue  # Unknown operation, skip

            change_data = {
                "operation": kind,
                "schema": change.get("schema"),
                "table": change.get("table"),
                "data": data,
                "timestamp": timestamp
            }

            filename = f"{timestamp}.json"
            filepath = os.path.join(OUTPUT_DIR, subdir, filename)
            with open(filepath, "w") as f:
                json.dump(change_data, f, indent=2)

            print(f"Wrote {kind} to {filepath}")

    cur.send_feedback(flush_lsn=msg.data_start)


def main():
    """Main loop to consume replication stream."""
    print("Starting WAL streaming...")
    try:
        while True:
            msg = cur.read_message()
            if msg:
                process_wal_message(msg)
            else:
                select.select([conn], [], [], 10)
    except KeyboardInterrupt:
        print("Stopping WAL streaming...")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
