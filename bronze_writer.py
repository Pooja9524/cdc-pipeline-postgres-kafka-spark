"""
bronze_writer.py
-----------------
Spark Structured STREAMING job (spark.readStream -- never spark.read batch).

Reads CDC events from Kafka topic cdc_project.public.emp, decodes the
Debezium envelope (including base64 Decimal salary, epoch-day hire_date,
and microsecond updated_at), and writes EVERY event (append-only, full
history) to BOTH:
    - a local Bronze Parquet folder (this becomes silver_writer.py's
      streaming SOURCE)
    - MySQL dw_db.emp_bronze (for direct querying/reporting)

Uses foreachBatch so a single streaming query can write to two sinks
in the same micro-batch.

Credentials are loaded from a local .env file (see .env.example) --
never hardcode real credentials in this file.

Run with (from the project folder):

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,mysql:mysql-connector-java:8.0.33 \
      bronze_writer.py

Stop with Ctrl+C -- this runs continuously until stopped, re-checking
Kafka for new data every TRIGGER_INTERVAL.
"""

import base64
import os
from decimal import Decimal

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, expr, to_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DecimalType
)

load_dotenv()

# ----------------------------------------------------------------------
# Configuration -- loaded from environment variables (.env file, not
# committed to git). See .env.example for the required variable names.
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cdc_project.public.emp")

BRONZE_PARQUET_PATH = "./data/bronze/emp"
BRONZE_CHECKPOINT_PATH = "./checkpoints/bronze_emp"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3307")
MYSQL_DB = os.environ.get("MYSQL_DB", "dw_db")
MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
MYSQL_TABLE = "emp_bronze"
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

TRIGGER_INTERVAL = "10 seconds"
# Why 10s: Bronze is the first stage and closest to the source. A short
# interval keeps end-to-end latency low without hammering Kafka/MySQL
# with excessive tiny transactions. Tune down for lower latency, up to
# reduce MySQL connection overhead if write volume grows.

SALARY_SCALE = 2

# ----------------------------------------------------------------------
# Spark session
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("BronzeWriter")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------
# Debezium envelope schema
# ----------------------------------------------------------------------
emp_row_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("dept_name", StringType()),
    StructField("city_location", StringType()),
    StructField("designation", StringType()),
    StructField("salary", StringType()),      # base64 -- decoded below
    StructField("hire_date", IntegerType()),  # days since epoch
    StructField("status", StringType()),
    StructField("email", StringType()),
    StructField("updated_at", LongType()),    # microseconds since epoch
])

source_schema = StructType([
    StructField("version", StringType()),
    StructField("connector", StringType()),
    StructField("name", StringType()),
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("db", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
    StructField("txId", LongType()),
    StructField("lsn", LongType()),
    StructField("xmin", LongType()),
])

envelope_schema = StructType([
    StructField("before", emp_row_schema),
    StructField("after", emp_row_schema),
    StructField("source", source_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

payload_schema = StructType([
    StructField("payload", envelope_schema)
])

# ----------------------------------------------------------------------
# UDF: decode base64 Decimal salary
# ----------------------------------------------------------------------
def decode_decimal(b64_str):
    if b64_str is None:
        return None
    raw_bytes = base64.b64decode(b64_str)
    unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    return Decimal(unscaled) / (Decimal(10) ** SALARY_SCALE)

decode_decimal_udf = udf(decode_decimal, DecimalType(10, 2))

# ----------------------------------------------------------------------
# STREAMING read from Kafka
# ----------------------------------------------------------------------
raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")   # first run only; checkpoint takes over after
    .option("failOnDataLoss", "false")       # tolerate topic compaction/retention during dev
    .load()
)

# Filter tombstones (null value, follow deletes -- no payload to parse)
non_tombstone_df = raw_stream_df.filter(col("value").isNotNull())

parsed_df = non_tombstone_df.select(
    from_json(col("value").cast("string"), payload_schema).alias("data")
).select("data.payload.*")

decoded_df = parsed_df.select(
    col("op").alias("cdc_op"),
    col("ts_ms").alias("cdc_ts_ms"),
    col("source.lsn").alias("cdc_lsn"),

    expr("coalesce(after.emp_id, before.emp_id)").alias("emp_id"),
    expr("coalesce(after.first_name, before.first_name)").alias("first_name"),
    expr("coalesce(after.last_name, before.last_name)").alias("last_name"),
    expr("coalesce(after.dept_name, before.dept_name)").alias("dept_name"),
    expr("coalesce(after.city_location, before.city_location)").alias("city_location"),
    expr("coalesce(after.designation, before.designation)").alias("designation"),

    decode_decimal_udf(expr("coalesce(after.salary, before.salary)")).alias("salary"),

    expr("date_add(to_date('1970-01-01'), coalesce(after.hire_date, before.hire_date))").alias("hire_date"),

    expr("coalesce(after.status, before.status)").alias("status"),
    expr("coalesce(after.email, before.email)").alias("email"),

    to_timestamp(
        (expr("coalesce(after.updated_at, before.updated_at)") / 1000000)
    ).alias("updated_at"),
)

# ----------------------------------------------------------------------
# foreachBatch: write each micro-batch to BOTH Parquet and MySQL
# ----------------------------------------------------------------------
def write_bronze_batch(batch_df, batch_id):
    # Empty-batch guard -- Kafka can produce empty polls; skip writes entirely.
    row_count = batch_df.count()
    if row_count == 0:
        print(f"[Bronze] Batch {batch_id}: 0 rows, skipping.")
        return

    print(f"[Bronze] Batch {batch_id}: {row_count} rows -> writing to Parquet + MySQL")

    # Cache since we write this same micro-batch DataFrame to two sinks
    batch_df.cache()

    # Sink 1: Parquet (append-only, becomes Silver's streaming source)
    batch_df.write.mode("append").parquet(BRONZE_PARQUET_PATH)

    # Sink 2: MySQL emp_bronze (append-only, matches Bronze semantics)
    (
        batch_df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", MYSQL_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .mode("append")
        .save()
    )

    batch_df.unpersist()
    print(f"[Bronze] Batch {batch_id}: write complete.")


# ----------------------------------------------------------------------
# Start the streaming query
# ----------------------------------------------------------------------
query = (
    decoded_df.writeStream
    .foreachBatch(write_bronze_batch)
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print(f"[Bronze] Streaming query started. Trigger interval: {TRIGGER_INTERVAL}")
print(f"[Bronze] Parquet output: {BRONZE_PARQUET_PATH}")
print(f"[Bronze] Checkpoint: {BRONZE_CHECKPOINT_PATH}")
print(f"[Bronze] MySQL target: {MYSQL_URL} / {MYSQL_TABLE}")
print("[Bronze] Waiting for data... (Ctrl+C to stop)")

query.awaitTermination()
