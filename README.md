# CDC Data Pipeline

A Change Data Capture pipeline built to stream row-level changes out of an operational Postgres database and land them in a queryable, layered warehouse — without batch polling or nightly loads.

## About This Project

This project simulates a real-world CDC use case: capturing inserts, updates, and deletes from a Postgres source in near real-time and propagating them downstream through a medallion (Bronze/Silver/Gold) architecture. Debezium reads the Postgres write-ahead log and publishes change events to Kafka. A Kafka Connect JDBC Sink Connector consumes those events and writes them into a MySQL warehouse database (`dw_db`), which forms the Bronze layer. From there, PySpark Structured Streaming jobs incrementally clean and dedupe Bronze into Silver, then aggregate Silver into Gold. The whole stack — Kafka (KRaft mode, no Zookeeper), Kafka Connect, and MySQL — runs in Docker, while Postgres runs natively on Windows and is reached from containers via `host.docker.internal`. The project was built to demonstrate an end-to-end streaming ELT pattern: source DB → CDC → message broker → sink → incremental transformation layers.

## Architecture

```
Postgres ──▶ Debezium ──▶ Kafka ──▶ Spark Structured Streaming 
                                              │ 
                                              ▼
                                            Bronze 
                                              │ Spark Structured Streaming
                                              ▼
                                           Silver
                                              │ Spark Structured Streaming
                                              ▼
                                            Gold
```

- **Source**: Postgres 18 (runs natively on Windows, reached from Docker via `host.docker.internal`)
- **CDC**: Debezium Postgres connector, publishing change events to Kafka
- **Streaming broker**: Kafka in KRaft mode (no Zookeeper)
- **Sink**: Kafka Connect JDBC Sink Connector writes change events into MySQL (`dw_db`) — this becomes the Bronze layer
- **Processing**: PySpark 4.2.0 (Java 17, Python 3.11.9) Structured Streaming jobs move data Bronze → Silver → Gold within `dw_db`
- **Layers**: Bronze (raw) → Silver (cleaned/deduped) → Gold (aggregated)

## Files
- `bronze_streaming.py` — Spark Structured Streaming job populating/refreshing the Bronze layer
- `silver_streaming.py` — cleans/dedupes Bronze into Silver
- `gold_streaming.py` — aggregates Silver into Gold
- `docker-compose.yml` — Kafka, Kafka Connect, and MySQL stack
- `postgres_setup.sql` — Postgres source setup (e.g. publication/replication slot prerequisites for Debezium)
- `dw_setup.sql` — MySQL `dw_db` warehouse schema setup (Bronze/Silver/Gold tables)
- `postgres_source.template.json` — Debezium connector config template (copy to `postgres_source.json`, fill in real credentials, do not commit)

## Setup
1. Copy `postgres_source.template.json` → `postgres_source.json` and fill in your real `database.password`.
2. Run `postgres_setup.sql` against Postgres, and `dw_setup.sql` against MySQL, to prepare both databases.
3. Bring up the stack: `docker compose up -d`
4. Register the Debezium source connector and the JDBC sink connector against the Kafka Connect REST API.
5. Run the streaming jobs in order: `bronze_streaming.py` → `silver_streaming.py` → `gold_streaming.py`

`data/`, `checkpoints/`, and `*.log` are gitignored — they're regenerated locally when you run the pipeline.
