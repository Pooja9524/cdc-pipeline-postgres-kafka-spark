-- =====================================================================
-- 1. CREATE THE SOURCE EMPLOYEE TABLE
-- =====================================================================
-- This is the original/source table.
--
-- Changes made to this table (INSERT, UPDATE, DELETE) will be captured
-- by PostgreSQL logical replication and then consumed by Debezium.
-- =====================================================================

CREATE TABLE IF NOT EXISTS emp (
    emp_id         SERIAL PRIMARY KEY,
    first_name     VARCHAR(50)  NOT NULL,
    last_name      VARCHAR(50)  NOT NULL,
    dept_name      VARCHAR(100) NOT NULL,
    city_location  VARCHAR(100) NOT NULL,
    designation    VARCHAR(100) NOT NULL,
    salary         NUMERIC(10, 2) NOT NULL,
    hire_date      DATE DEFAULT CURRENT_DATE,
    status         VARCHAR(20)  NOT NULL,
    email          VARCHAR(255) NOT NULL,

    -- Stores the time when the row was last updated.
    -- The trigger defined later automatically updates this value
    -- whenever the row is modified.
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =====================================================================
-- 2. ENABLE REPLICA IDENTITY FULL
-- =====================================================================
-- PostgreSQL needs to know what information should be included in
-- logical replication messages for UPDATE and DELETE operations.
--
-- REPLICA IDENTITY FULL means PostgreSQL records the COMPLETE OLD ROW.
--
-- Example:
--
-- Before UPDATE:
--   emp_id = 1, salary = 50000
--
-- After UPDATE:
--   emp_id = 1, salary = 60000
--
-- Debezium can therefore receive:
--
--   before = { emp_id: 1, salary: 50000, ... }
--   after  = { emp_id: 1, salary: 60000, ... }
--
-- This is important for our CDC pipeline because the Bronze layer
-- expects both "before" and "after" information.
-- =====================================================================

ALTER TABLE emp REPLICA IDENTITY FULL;


-- =====================================================================
-- 3. CREATE A DEDICATED DEBEZIUM REPLICATION USER
-- =====================================================================
-- Debezium needs a PostgreSQL user to connect to the database and
-- consume logical replication changes.
--
-- We create a dedicated user instead of using the PostgreSQL
-- superuser. This is better practice because the CDC connector
-- should have only the permissions it needs.
--
-- NOTE:
--   Replace 'CHANGE_ME' with your own real password. Never commit a
--   real password to version control -- keep it in a .env file or
--   equivalent local secret store instead.
-- =====================================================================

DO $$
BEGIN
    -- Create the user only if it does not already exist.
    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_roles
        WHERE rolname = 'debezium'
    ) THEN

        CREATE ROLE debezium
        WITH
            LOGIN
            PASSWORD 'CHANGE_ME'
            REPLICATION;

    END IF;
END
$$;


-- =====================================================================
-- 4. GRANT DATABASE AND SCHEMA ACCESS
-- =====================================================================
-- Allow the Debezium user to connect to the database.
-- =====================================================================

GRANT CONNECT ON DATABASE postgres TO debezium;


-- Allow Debezium to access the public schema.
-- =====================================================================

GRANT USAGE ON SCHEMA public TO debezium;


-- Allow Debezium to read the source table.
--
-- Debezium needs SELECT permission to read the table's data and
-- create the CDC events correctly.
-- =====================================================================

GRANT SELECT ON emp TO debezium;


-- =====================================================================
-- 5. CREATE A POSTGRESQL PUBLICATION
-- =====================================================================
-- A PUBLICATION is a PostgreSQL logical replication object.
--
-- Think of it as:
--
--   "Publish changes made to this table so that a CDC consumer
--    such as Debezium can receive them."
--
-- Here we publish changes only for the "emp" table.
-- =====================================================================

-- Remove the publication if it already exists.
-- This makes the script easier to re-run during development.

DROP PUBLICATION IF EXISTS cdc_publication;


-- Create a new publication containing the emp table.

CREATE PUBLICATION cdc_publication
FOR TABLE emp;


-- =====================================================================
-- 6. CREATE FUNCTION + TRIGGER TO AUTOMATICALLY UPDATE updated_at
-- =====================================================================
-- PostgreSQL does NOT automatically update a column defined as:
--
--   DEFAULT CURRENT_TIMESTAMP
--
-- when the row is updated. The DEFAULT only fires on INSERT.
--
-- Therefore, we create a trigger function so that updated_at changes
-- automatically whenever an existing employee row is updated. This
-- matters downstream: Silver/Gold's dedup logic can rely on updated_at
-- (or Debezium's own cdc_ts_ms) to determine the latest row per
-- employee -- without this trigger, updated_at would stay frozen at
-- INSERT time and give unreliable ordering.
-- =====================================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_emp_updated_at ON emp;

CREATE TRIGGER trg_emp_updated_at
    BEFORE UPDATE ON emp
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();


-- =====================================================================
-- 7. SANITY CHECKS
-- =====================================================================
-- These queries allow us to verify that PostgreSQL was configured
-- correctly before starting Debezium.
-- =====================================================================

-- Check the replica identity of the emp table.
-- Expected: relreplident = 'f' (FULL)
SELECT
    relname,
    relreplident
FROM pg_class
WHERE relname = 'emp';

-- Check whether the emp table belongs to our publication.
SELECT *
FROM pg_publication_tables
WHERE pubname = 'cdc_publication';

-- Check how many employees currently exist in the source table.
SELECT COUNT(*)
FROM emp;
