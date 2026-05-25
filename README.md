# Real-Time CDC Pipeline: PostgreSQL → Kafka → Spark → Snowflake

A production-style, end-to-end **Change Data Capture (CDC)** pipeline that streams live database events from PostgreSQL into Snowflake using Apache Kafka and Apache Spark Structured Streaming — all containerized with Docker.

---

## Architecture

```
PostgreSQL (logical replication)
        │
        ▼
  kafka_producer.py       ← captures INSERT/UPDATE/DELETE via replication slot
        │
        ▼
  Apache Kafka (broker)   ← decouples ingestion from processing
        │
        ▼
  spark_consumer.py       ← Spark Structured Streaming, micro-batches to Snowflake
        │
        ▼
  Snowflake Staging Table
        │
        ▼
  Snowflake Task (SQL)    ← MERGE + dedup → Curated layer, then truncates staging
        │
        ▼
  Snowflake Curated Table (exact-once, analytics-ready)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source DB | PostgreSQL (logical replication + `test_decoding`) |
| Message Broker | Apache Kafka (KRaft mode, no Zookeeper) |
| Stream Processing | Apache Spark Structured Streaming (PySpark) |
| Cloud DWH | Snowflake (Staging → Curated via Tasks + MERGE) |
| Containerization | Docker Compose |
| Language | Python 3.x, SQL |

---

## Project Structure

```
├── Producer/
│   └── kafka_producer.py       # PostgreSQL CDC → Kafka
├── Consumer/
│   └── spark_consumer.py       # Kafka → Snowflake staging
├── Snowflake/
│   └── Snowflake_Task.sql      # MERGE + truncate tasks (curated layer)
├── docker-compose.yaml         # Kafka broker + Kafka UI + Spark container
├── .env.example                # Environment variable template
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose installed
- PostgreSQL instance with logical replication enabled (`wal_level = logical`)
- Snowflake account with a database, schema, and warehouse

### 1. Clone the repo
```bash
git clone https://github.com/rohankadu1/cdc-pipeline-postgres-kafka-spark-snowflake.git
cd cdc-pipeline-postgres-kafka-spark-snowflake
```

### 2. Configure environment variables
```bash
cp .env.example .env
# Fill in your credentials in .env
```

### 3. Set up Snowflake tables
Run the following in your Snowflake worksheet before starting the pipeline:
```sql
-- Staging table (raw CDC events)
CREATE OR REPLACE TABLE USERS_CDC_STAGING (
    action    VARCHAR,
    id        VARCHAR,
    name      VARCHAR,
    email     VARCHAR,
    updated_at VARCHAR,
    lsn_num   NUMBER
);

-- Curated table (deduplicated, analytics-ready)
CREATE OR REPLACE TABLE USERS_CDC_CURATED (
    action    VARCHAR,
    id        VARCHAR,
    name      VARCHAR,
    email     VARCHAR,
    updated_at VARCHAR,
    lsn_num   NUMBER
);
```

Then create and start the Snowflake Tasks from `Snowflake/Snowflake_Task.sql`.

### 4. Start the Docker stack
```bash
docker-compose up -d
```
This starts the Kafka broker, Kafka UI (port 8080), and Spark streaming container.

### 5. Run the Kafka producer
```bash
python Producer/kafka_producer.py
```

### 6. Verify
- **Kafka UI**: http://localhost:8080 — check messages flowing into your topic
- **Spark UI**: http://localhost:4040 — check streaming query status
- **Snowflake**: query `USERS_CDC_STAGING` and `USERS_CDC_CURATED` to confirm data landing

---

## Key Design Decisions

**Exact-once processing in Snowflake**
The Snowflake Task uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY lsn_num ...)` to deduplicate events before merging into the curated layer, ensuring no duplicate rows even if Spark retries a micro-batch.

**Graceful shutdown**
The Kafka producer handles `SIGINT` and `SIGTERM` signals — it flushes and closes cleanly rather than dropping in-flight messages.

**LSN-based acknowledgement**
Every message sends feedback via `msg.cursor.send_feedback(write_lsn=...)`, keeping PostgreSQL's replication slot from accumulating unbounded WAL files.

**KRaft mode Kafka**
No Zookeeper dependency — the broker runs in KRaft mode, simplifying the Docker setup to a single container.

---

## Kafka UI Preview

Once running, navigate to `http://localhost:8080` to inspect topics, messages, and consumer group lag in real time.

---

## Future Improvements
- [ ] Add support for UPDATE and DELETE operations in the Snowflake curated layer (currently inserts only)
- [ ] Replace `test_decoding` with `pgoutput` or `wal2json` for richer payload structure
- [ ] Add dbt models on top of the curated layer for transformation
- [ ] Implement schema registry for Kafka message validation
- [ ] Add Great Expectations data quality checks post-merge

---

## Author
**Rohan Kadu** — [LinkedIn](https://linkedin.com/in/rohankadu) · [GitHub](https://github.com/rohankadu1)
