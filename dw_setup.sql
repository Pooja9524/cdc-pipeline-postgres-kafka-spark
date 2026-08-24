-- =====================================================================
-- dw_setup.sql
-- Run against MySQL (host: localhost, port: 3307, db: dw_db)
-- e.g.  mysql -h 127.0.0.1 -P 3307 -u dw_user -p dw_db < dw_setup.sql
-- =====================================================================

USE dw_db;

-- =====================================================================
-- BRONZE: append-only raw CDC history.
-- One row per Kafka message (snapshot read, insert, update, delete).
-- Fields are decoded to proper types (not raw Debezium encodings),
-- but nothing is deduplicated or filtered -- full history preserved.
-- =====================================================================
DROP TABLE IF EXISTS emp_bronze;
CREATE TABLE emp_bronze (
    bronze_id       BIGINT AUTO_INCREMENT PRIMARY KEY,  -- surrogate key, one per CDC event

    -- CDC / Debezium metadata
    cdc_op          VARCHAR(1)     NOT NULL,   -- 'r' snapshot, 'c' insert, 'u' update, 'd' delete
    cdc_ts_ms       BIGINT         NOT NULL,   -- event timestamp from Debezium (epoch millis)
    cdc_lsn         BIGINT,                    -- Postgres WAL LSN, useful for ordering/debugging

    -- Employee data (decoded to proper types)
    emp_id          INT            NOT NULL,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    dept_name       VARCHAR(50),
    city_location   VARCHAR(50),
    designation     VARCHAR(50),
    salary          DECIMAL(10, 2),
    hire_date       DATE,
    status          VARCHAR(20),
    email           VARCHAR(100),
    updated_at      TIMESTAMP NULL,

    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- when Spark wrote this row

    INDEX idx_bronze_emp_id (emp_id),
    INDEX idx_bronze_cdc_ts (cdc_ts_ms)
);

-- =====================================================================
-- SILVER: current state, one row per emp_id (upserted).
-- Deduplicated per micro-batch to keep only the latest event per employee.
-- Soft-delete via is_deleted flag (deletes don't remove rows, they flag them).
-- =====================================================================
DROP TABLE IF EXISTS emp_silver;
CREATE TABLE emp_silver (
    emp_id          INT PRIMARY KEY,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    dept_name       VARCHAR(50),
    city_location   VARCHAR(50),
    designation     VARCHAR(50),
    salary          DECIMAL(10, 2),
    hire_date       DATE,
    status          VARCHAR(20),
    email           VARCHAR(100),
    updated_at      TIMESTAMP NULL,             -- source-side updated_at (from Postgres)

    is_deleted      BOOLEAN DEFAULT FALSE,       -- soft-delete flag
    last_cdc_op     VARCHAR(1),                  -- last operation applied ('c','u','d')
    last_synced_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_silver_dept (dept_name),
    INDEX idx_silver_is_deleted (is_deleted)
);

-- =====================================================================
-- SILVER STAGING: temporary landing zone for each micro-batch before
-- merging into emp_silver. Spark's JDBC writer has no native upsert
-- mode, so each batch is (1) loaded here via a fast parallel append,
-- then (2) merged into emp_silver via a single
-- INSERT ... ON DUPLICATE KEY UPDATE statement (see silver_writer.py).
-- No primary key here -- it's truncated and refilled every batch.
-- =====================================================================
DROP TABLE IF EXISTS emp_silver_staging;
CREATE TABLE emp_silver_staging (
    emp_id          INT,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    dept_name       VARCHAR(50),
    city_location   VARCHAR(50),
    designation     VARCHAR(50),
    salary          DECIMAL(10, 2),
    hire_date       DATE,
    status          VARCHAR(20),
    email           VARCHAR(100),
    updated_at      TIMESTAMP NULL,
    is_deleted      BOOLEAN DEFAULT FALSE,
    last_cdc_op     VARCHAR(1)
);

-- =====================================================================
-- GOLD: department-level aggregates, recomputed from Silver.
-- One row per department.
-- =====================================================================
DROP TABLE IF EXISTS dept_summary_gold;
CREATE TABLE dept_summary_gold (
    dept_name       VARCHAR(50) PRIMARY KEY,
    headcount       INT             NOT NULL,
    avg_salary      DECIMAL(10, 2)  NOT NULL,
    min_salary      DECIMAL(10, 2)  NOT NULL,
    max_salary      DECIMAL(10, 2)  NOT NULL,
    recomputed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Sanity check
SHOW TABLES;
