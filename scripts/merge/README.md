# Unified Clinical Data Ingestion Pipelines

This directory contains the scripts and pipelines responsible for migrating and standardizing raw, source-specific clinical trial data into the Unified Clinical Database schema.

## Supported Sources

Currently supported databases and their corresponding subdirectories:

1. **[CTGov (ClinicalTrials.gov)](./ctgov/README.md)**: Parses and migrates data from the AACT PostgreSQL database into the unified schema.

*(Additional sources like ISRCTN or EudraCT will be added as new subdirectories as the project expands).*

## Initialization

Before running any source-specific ingestion, the Unified Clinical Database schema must be initialized. The `init_db.py` script sets up all the required schemas (`clinical`, `scientific`, `company`, `ref`, `ingest`), creates the core tables with strict constraints, and inserts critical initial seed data (such as the `CTGOV` source record in `ref.source`).

```bash
uv run python -m scripts.merge.init_db
```

This step ensures the destination database is completely prepared to receive data from the ingestion pipelines.

## Architecture Strategy

Our ingestion pipelines follow a strict, high-performance design pattern regardless of the source database:

1. **Seed Jobs:** An initial script reads raw identifiers (e.g., `nct_id`s) from the source database and seeds them into the unified `ingest.sync_log` tracking table, marking them as `pending`.
2. **Batch Processing:** Data is migrated in bulk rather than row-by-row. We leverage memory aggregation and `psycopg2.extras.execute_values` to perform massive set-based `INSERT` operations. This maximizes throughput by preventing database lock contention and drastically minimizing network I/O overhead.
3. **Graceful Fallbacks:** If a massive bulk insert fails (which is common when dealing with messy source data and strict `NOT NULL` constraints), the pipeline automatically catches the database error, safely rolls back the transaction, and temporarily drops into a row-by-row "fallback mode" for that specific batch. This ensures that one bad record doesn't bring down the entire pipeline, while simultaneously isolating the specific database error to the `sync_log`.
4. **Resiliency:** The `sync_log` continuously tracks the state of every single record (`pending`, `processing`, `completed`, `error`). The pipeline is designed to be stopped or interrupted at any time and safely resumed without duplicate processing. Failed records can be re-attempted using the `--retry` flag.
