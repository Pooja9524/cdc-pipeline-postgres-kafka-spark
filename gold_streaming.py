"""
gold_streaming.py
------------------
Spark Structured STREAMING job (spark.readStream -- never spark.read batch).

SOURCE: Silver's Parquet folder (./data/silver/emp) -- NOT Kafka, NOT Bronze,
per architecture constraint #2.

Because Silver's Parquet folder is append-only (one row per emp_id per
event, not a running "current state" file), Gold cannot aggregate just the
incoming micro-batch in isolation -- a batch with only 2 changed employees
would otherwise wipe out headcounts for every other department.

So each trigger, Gold:
    1. Uses the streaming read only as a TRIGGER (to know new data arrived)
    2. Re-reads Silver's FULL Parquet history via a batch read inside
       foreachBatch
    3. Deduplicates to the latest event per emp_id (same row_number()
       pattern as Silver), ordered by cdc_ts_ms (Debezium's own event
       timestamp -- always increasing, unlike the source table's
       updated_at column, which can tie across events)
    4. Filters out soft-deleted employees (is_deleted = TRUE)
    5. Aggregates headcount / avg / min / max salary per department
    6. OVERWRITES (not appends) both Gold's Parquet folder and MySQL
       dept_summary_gold -- Gold is the final layer, nothing streams
       from it, so "current totals only" is the correct shape here.

This is a reasonable, common pattern for aggregation layers on top of
small-to-moderate data volumes. Processing time grows with total Silver
data size over time, since each trigger re-scans everything -- acceptable
here given the project's scale.

Credentials are loaded from a local .env file (see .env.example) --
never hardcode real credentials in this file.

Run with (from the project folder):

    spark-submit \
      --packages mysql:mysql-connector-java:8.0.33 \
      gold_streaming.py

Stop with Ctrl+C. Requires silver_streaming.py to be running (or to have
already written Silver Parquet data) since this reads FROM Silver's
Parquet folder.
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, desc, count, avg, min as spark_min, max as spark_max, round as spark_round

load_dotenv()

# ----------------------------------------------------------------------
# Configuration -- loaded from environment variables (.env file, not
# committed to git). See .env.example for the required variable names.
# ----------------------------------------------------------------------
SILVER_PARQUET_PATH = "./data/silver/emp"

GOLD_PARQUET_PATH = "./data/gold/dept_summary"
GOLD_CHECKPOINT_PATH = "./checkpoints/gold_dept_summary"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3307"))
MYSQL_DB = os.environ.get("MYSQL_DB", "dw_db")
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
GOLD_TABLE = "dept_summary_gold"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

TRIGGER_INTERVAL = "20 seconds"
# Why 20s (longer than Silver's 15s): Gold depends on Silver's Parquet
# files already existing, same head-start reasoning as Bronze->Silver.
# Also, since Gold re-scans ALL of Silver's history every trigger (not
# just new files), a slightly longer interval reduces redundant full
# rescans when changes are infrequent.

# ----------------------------------------------------------------------
# Spark session
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GoldStreaming")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------
# STREAMING read from Silver's Parquet folder.
# This is used ONLY as a trigger mechanism (to detect new data arrived).
# The actual aggregation logic re-reads the full folder via batch read
# inside foreachBatch, since we need the complete current state, not
# just the incoming micro-batch.
# ----------------------------------------------------------------------
silver_df_for_schema = spark.read.parquet(SILVER_PARQUET_PATH)
silver_schema = silver_df_for_schema.schema

silver_stream_df = (
    spark.readStream
    .schema(silver_schema)
    .parquet(SILVER_PARQUET_PATH)
)

# ----------------------------------------------------------------------
# foreachBatch: ignore the micro-batch content itself, re-scan full
# Silver Parquet history, dedup, filter deletes, aggregate, overwrite.
# ----------------------------------------------------------------------
def write_gold_batch(batch_df, batch_id):
    trigger_row_count = batch_df.count()
    if trigger_row_count == 0:
        print(f"[Gold] Batch {batch_id}: 0 new rows in trigger, skipping.")
        return

    print(f"[Gold] Batch {batch_id}: triggered by {trigger_row_count} new Silver rows -> re-scanning full Silver history")

    # Full re-read of Silver's Parquet folder (batch read, not the
    # streaming DataFrame) -- this is intentional, see module docstring.
    full_silver_df = spark.read.parquet(SILVER_PARQUET_PATH)

    # Dedup to latest event per emp_id (Silver's Parquet is append-only,
    # so it can contain multiple historical rows per employee).
    # Ordered by cdc_ts_ms (Debezium's own event timestamp, always
    # increasing) rather than updated_at -- the source Postgres table's
    # updated_at only fires via DEFAULT on INSERT, not ON UPDATE, so it
    # can tie across multiple real events for the same employee and give
    # an unreliable "latest" pick.
    latest_window = Window.partitionBy("emp_id").orderBy(desc("cdc_ts_ms"))

    current_state_df = (
        full_silver_df
        .withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .filter(col("is_deleted") == False)  # noqa: E712 -- exclude soft-deleted employees
    )

    current_count = current_state_df.count()
    print(f"[Gold] Batch {batch_id}: {current_count} active employees after dedup + delete filter")

    # Department-level aggregates
    dept_summary_df = (
        current_state_df
        .groupBy("dept_name")
        .agg(
            count("emp_id").alias("headcount"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
            spark_min("salary").alias("min_salary"),
            spark_max("salary").alias("max_salary"),
        )
    )

    dept_count = dept_summary_df.count()
    print(f"[Gold] Batch {batch_id}: {dept_count} departments -> writing")

    dept_summary_df.cache()

    # Sink 1: Gold Parquet -- OVERWRITE (current totals only, nothing
    # downstream streams from this, so no append-only constraint here)
    dept_summary_df.write.mode("overwrite").parquet(GOLD_PARQUET_PATH)

    # Sink 2: MySQL dept_summary_gold -- OVERWRITE (truncate + reload)
    (
        dept_summary_df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", GOLD_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .option("truncate", "true")   # TRUNCATE instead of DROP+CREATE, preserves table schema/constraints
        .mode("overwrite")
        .save()
    )

    dept_summary_df.unpersist()
    print(f"[Gold] Batch {batch_id}: write complete.")


# ----------------------------------------------------------------------
# Start the streaming query
# ----------------------------------------------------------------------
query = (
    silver_stream_df.writeStream
    .foreachBatch(write_gold_batch)
    .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print(f"[Gold] Streaming query started. Trigger interval: {TRIGGER_INTERVAL}")
print(f"[Gold] Reading from Silver Parquet: {SILVER_PARQUET_PATH}")
print(f"[Gold] Parquet output: {GOLD_PARQUET_PATH}")
print(f"[Gold] Checkpoint: {GOLD_CHECKPOINT_PATH}")
print(f"[Gold] MySQL target: {MYSQL_URL} / {GOLD_TABLE}")
print("[Gold] Waiting for data... (Ctrl+C to stop)")

query.awaitTermination()
