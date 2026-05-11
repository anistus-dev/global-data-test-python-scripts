# CTGov Bulk Ingestion Pipeline

This directory contains the Python-based bulk ingestion pipeline for migrating clinical trial data from the AACT (CTGov) database into the Unified Clinical Database schema.

## Architecture

This pipeline is built for high-throughput data extraction and loading using pure Python. It utilizes `psycopg2.extras.execute_values` to aggregate entire batches of studies into memory, cleanses them, and executes massive set-based `INSERT` operations to achieve ingestion rates exceeding ~400 trials per second.

## Core Scripts

1. **`seed_ids.py`**: Initializes the ingestion tracking table (`ingest.sync_log`). It reads the `nct_id`s from the AACT database and marks them as `pending` so the ingest scripts know what to process.
2. **`ingest_bulk.py`**: The primary, high-performance orchestrator. Uses multi-threading and an in-memory batching engine to perform bulk-upserts into the relational unified schema. Includes a fallback mechanism to handle row-by-row errors gracefully.
3. **`ingest.py`**: The original row-by-row ingestion script. Much slower than `ingest_bulk.py` but provides safe, sequential processing.

## Usage

### 1. Seeding IDs

Before running any ingestion, you must seed the `sync_log` tracking table:

```bash
uv run python -m scripts.merge.ctgov.seed_ids
```

### 2. Ingesting Data (Bulk - Recommended)

```bash
uv run python -m scripts.merge.ctgov.ingest_bulk [OPTIONS]
```

**Options:**
* `--batch-size <INT>`: The number of trials to process per worker batch (default: `500`).
* `--workers <INT>`: Number of parallel thread pool workers (default: `1`).
* `--limit <INT>`: Maximum total studies to process in the current run.
* `--retry`: If set, the script will re-process studies currently marked as `error` in the `sync_log`.

> **Performance Tip:** Extensive benchmarking indicates that a large batch size (e.g., 500) combined with a low worker count (e.g., 1 or 2) provides the highest throughput while safely avoiding database locking and resource contention.

### 3. Ingesting Data (Row-by-Row - Legacy)

```bash
uv run python -m scripts.merge.ctgov.ingest [OPTIONS]
```
*(Accepts similar options like `--limit` and `--retry` but runs sequentially.)*

## Known Limitations & Design Exclusions

To streamline the sprawling 71-table AACT schema into our highly performant, source-agnostic model, a few intentional design exclusions were made:

* **Intentional Exclusions:** Complex longitudinal tables such as `baseline_counts`, `participant_flows`, `milestones`, and `drop_withdrawals` have been omitted from the relational mapping to keep the database lightweight.
* **MeSH Term Loss:** Standardized Medical Subject Headings (`browse_conditions` and `browse_interventions` tables) are currently dropped in favor of extracting the raw, free-text investigator strings.
* **Orphaned Publications:** References in the source database that lack an explicit PubMed ID (`pmid`) are dropped, as the pipeline relies on PMIDs as the unique identifier.
* **Missing Fallbacks:** Because AACT data can be extremely messy, some required constraints (like `intervention.intervention_name` or `outcome.measure`) are completely blank in the source. The pipeline automatically catches these and injects `SERVERGENERATED: Unnamed Intervention` or `SERVERGENERATED: Unnamed Outcome` to preserve data integrity while passing Postgres constraints.
