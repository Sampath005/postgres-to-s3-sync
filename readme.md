# PostgreSQL to S3 Logical Replication Sync

This project provides a simple mechanism to capture logical replication changes from a PostgreSQL database and save them as structured JSON files. These files can then be uploaded or synced to an S3 bucket, forming the basis for a lightweight change data capture (CDC) pipeline or data lake ingestion tool.

---

## 📌 Project Overview

- Uses PostgreSQL's logical replication and the `wal2json` plugin.
- Listens for `INSERT`, `UPDATE`, and `DELETE` changes.
- Writes changes as JSON to local directories (organized by operation type).
- Can be extended to sync the generated JSON files to Amazon S3.

---

## 📁 Directory Structure

├── inserts/ # Contains JSON files for INSERT operations

├── updates/ # Contains JSON files for UPDATE operations

├── deletes/ # Contains JSON files for DELETE operations

🚀 Usage
1. Enable logical replication in your PostgreSQL config:
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

Restart PostgreSQL after making these changes.

2. Create replication slot:
SELECT * FROM pg_create_logical_replication_slot('datalake_slot', 'wal2json');

3. Run the WAL streaming script:
python main.py

The script will begin listening to WAL changes and saving them as JSON in the respective subdirectories.
