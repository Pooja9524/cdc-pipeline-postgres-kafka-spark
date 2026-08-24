"""
silver_streaming.py
--------------------
Spark Structured STREAMING job (spark.readStream -- never spark.read batch).

SOURCE: Bronze's Parquet folder (./data/bronze/emp) -- NOT Kafka directly,
per architecture constraint #2.

Per micro-batch:
    1. Dedup to keep only the LATEST event per emp_id (highest cdc_ts_ms)
    2. Mark is_deleted = TRUE for rows whose latest event was a delete (op='d')
    3. Write the deduped batch (append) to Silver's Parquet folder
       -- this becomes gold_streaming.py's streaming source
    4. Upsert into MySQL dw_db.emp_silver via a staging-table + merge pattern:
         a. TRUNCATE emp_silver_staging
         b. Spark JDBC appends the deduped batch into emp_silver_staging
         c. pymysql runs INSERT ... ON DUPLICATE KEY UPDATE ... SELECT
            FROM emp_silver_staging into emp_silver
       (Spark's native JDBC writer has no upsert mode, so steps (b)+(c)
        are how we work around that limitation.)

Credentials are loaded from a local .env file (see .env.example) --
never hardcode real credentials in this file.

Run with (from the project folder):

    spark-submit \
      --packages mysql:mysql-connector-java:8.0.33 \
      silver_streaming.py

Stop with Ctrl+C. Requires bronze_writer.py to be running (or to have
already written Bronze Parquet data) since this reads FROM Bronze's
Parquet folder.
"""

import os

import pymysql
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, when, lit, desc

load_dotenv()

# ----------------------------------------------------------------------
# Configuration -- loaded from environment variables (.env file, not
# committed to git). See .env.example for the required variable names.
# ----------------------------------------------------------------------
BRONZE_PARQUET_PATH = "./data/bronze/emp"

SILVER_PARQUET_PATH = "./data/silver/emp"
SILVER_CHECKPOINT_PATH = "./checkpoints/silver_emp"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3307"))
MYSQL_DB = os.environ.get("MYSQL_DB", "dw_db")
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
STAGING_TABLE = "emp_silver_staging"
SILVER_TABLE = "emp_silver"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

TRIGGER_INTERVAL = "15 seconds"
# Why 15s (vs Bronze's 10s): Silver depends on Bronze's Parquet files
# already existing on disk. A slightly longer interval than Bronze gives
# Bronze's writes a head start each cycle, reducing (though not fully
# eliminating -- Spark's file source is still eventually-consistent
# across stages) the chance Silver reads a half-written batch.

# ----------------------------------------------------------------------
# Spark session
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SilverStreaming")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------
# STREAMING read from Bronze's Parquet folder.
# We must supply the schema explicitly for a streaming Parquet source
# on first run (Spark can infer it from existing files, but being
# explicit avoids surprises if Bronze's folder is briefly empty).
# ----------------------------------------------------------------------
bronze_df = spark.read.parquet(BRONZE_PARQUET_PATH)
bronze_schema = bronze_df.schema

bronze_stream_df = (
    spark.readStream
    .schema(bronze_schema)
    .parquet(BRONZE_PARQUET_PATH)
)

# ----------------------------------------------------------------------
# foreachBatch: dedup, soft-delete flag, write to Parquet + MySQL
# ----------------------------------------------------------------------
def write_silver_batch(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count == 0:
        print(f"[Silver] Batch {batch_id}: 0 rows, skipping.")
        return

    print(f"[Silver] Batch {batch_id}: {row_count} raw events -> deduping")

    # Keep only the latest event per emp_id within this micro-batch,
    # using cdc_ts_ms (event time) to break ties -- highest wins.
    dedup_window = Window.partitionBy("emp_id").orderBy(desc("cdc_ts_ms"))

    deduped_df = (
        batch_df
        .withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("is_deleted", when(col("cdc_op") == "d", lit(True)).otherwise(lit(False)))
        .withColumn("last_cdc_op", col("cdc_op"))
        .select(
            "emp_id", "first_name", "last_name", "dept_name", "city_location",
            "designation", "salary", "hire_date", "status", "email", "updated_at",
            "is_deleted", "last_cdc_op", "cdc_ts_ms"
        )
    )

    deduped_count = deduped_df.count()
    print(f"[Silver] Batch {batch_id}: {deduped_count} rows after dedup -> writing")

    deduped_df.cache()

    # Sink 1: Silver Parquet (append-only, becomes Gold's streaming source).
    # Includes cdc_ts_ms -- Gold needs a reliable, always-increasing event
    # timestamp to dedup by (source-table updated_at columns aren't
    # guaranteed to change on every UPDATE, so they can tie).
    deduped_df.write.mode("append").parquet(SILVER_PARQUET_PATH)

    # MySQL-bound frame: emp_silver / emp_silver_staging don't have a
    # cdc_ts_ms column, so drop it before the JDBC writes (matches the
    # Bronze lesson: DataFrame columns must match the target table exactly).
    mysql_df = deduped_df.drop("cdc_ts_ms")

    # Sink 2a: Spark JDBC bulk-load into staging (fast, parallel, plain append)
    (
        mysql_df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", STAGING_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .mode("overwrite")   # staging is truncated & refilled fresh each batch
        .save()
    )

    # Sink 2b: pymysql runs the actual upsert merge (staging -> emp_silver)
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB
    )
    try:
        with conn.cursor() as cursor:
            merge_sql = f"""
                INSERT INTO {SILVER_TABLE}
                    (emp_id, first_name, last_name, dept_name, city_location,
                     designation, salary, hire_date, status, email, updated_at,
                     is_deleted, last_cdc_op)
                SELECT
                    emp_id, first_name, last_name, dept_name, city_location,
                    designation, salary, hire_date, status, email, updated_at,
                    is_deleted, last_cdc_op
                FROM {STAGING_TABLE}
                ON DUPLICATE KEY UPDATE
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    dept_name = VALUES(dept_name),
                    city_location = VALUES(city_location),
                    designation = VALUES(designation),
                    salary = VALUES(salary),
                    hire_date = VALUES(hire_date),
                    status = VALUES(status),
                    email = VALUES(email),
                    updated_at = VALUES(updated_at),
                    is_deleted = VALUES(is_deleted),
                    last_cdc_op = VALUES(last_cdc_op)
            """
            cursor.execute(merge_sql)
            conn.commit()
            print(f"[Silver] Batch {batch_id}: merged {cursor.rowcount} row-effects into {SILVER_TABLE}")
    finally:
        conn.close()

    deduped_df.unpersist()
    print(f"[Silver] Batch {batch_id}: write complete.")


# ----------------------------------------------------------------------
# Start the streaming query
# ----------------------------------------------------------------------
query = (
    bronze_stream_df.writeStream
    .foreachBatch(write_silver_batch)
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print(f"[Silver] Streaming query started. Trigger interval: {TRIGGER_INTERVAL}")
print(f"[Silver] Reading from Bronze Parquet: {BRONZE_PARQUET_PATH}")
print(f"[Silver] Parquet output: {SILVER_PARQUET_PATH}")
print(f"[Silver] Checkpoint: {SILVER_CHECKPOINT_PATH}")
print(f"[Silver] MySQL target: {MYSQL_URL} / {SILVER_TABLE} (via {STAGING_TABLE})")
print("[Silver] Waiting for data... (Ctrl+C to stop)")

query.awaitTermination()
