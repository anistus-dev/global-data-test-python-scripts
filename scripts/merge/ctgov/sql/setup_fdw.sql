-- ==============================================================================
-- CTGov Foreign Data Wrapper (FDW) Setup
-- ==============================================================================
-- This script links the unified clinical database to the AACT (CTGov) database 
-- so we can query the raw tables directly from within PostgreSQL.
-- 
-- Note: Python injects the connection credentials dynamically at runtime using 
--       string formatting (e.g. {host}, {dbname}) so secrets are not hardcoded.
-- ==============================================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create the foreign server definition
CREATE SERVER IF NOT EXISTS ctgov_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '{host}', dbname '{dbname}', port '{port}', fetch_size '50000', use_remote_estimate 'true');

-- Create user mapping for authentication
DO $$ 
BEGIN
    DROP USER MAPPING IF EXISTS FOR current_user SERVER ctgov_server;
    CREATE USER MAPPING FOR current_user
        SERVER ctgov_server
        OPTIONS (user '{user}', password '{password}');
END $$;

-- Drop and recreate the foreign schema to ensure we get a fresh snapshot 
-- of all 71 tables in case the AACT schema was updated.
DROP SCHEMA IF EXISTS aact_foreign CASCADE;
CREATE SCHEMA aact_foreign;

-- Import all tables from the source 'ctgov' schema into our virtual schema
IMPORT FOREIGN SCHEMA ctgov 
    FROM SERVER ctgov_server 
    INTO aact_foreign;
