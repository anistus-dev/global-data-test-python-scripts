# ClinicalTrials.gov (AACT) Database Management

This directory contains tools for managing and initializing the ClinicalTrials.gov (AACT) database repository.

## Configuration

The scripts in this directory use the `CTGOV_DB_CONFIG` settings from `scripts/config.py`. You can configure these in your root `.env` file:

```bash
CTGOV_DB_HOST=localhost
CTGOV_DB_PORT=5432
CTGOV_DB_NAME=aact
CTGOV_DB_USER=your_user
CTGOV_DB_PASSWORD=your_password
```

## Tools

### Data Download (`download_aact.py`)

Downloads the latest daily static database copy from the AACT website.

**Usage:**
```bash
# Download today's dump
uv run python -m scripts.ctgov.download_aact

# Download a specific date's dump
uv run python -m scripts.ctgov.download_aact --date 2026-04-20
```

### Database Initialization (`init_db.py`)

This script restores the AACT database from a PostgreSQL custom format dump file (`.dmp`). It uses `pg_restore` under the hood with optimized flags for a clean setup.

**Usage:**
```bash
uv run python -m scripts.ctgov.init_db path/to/your/dump_file.dmp
```

**What it does:**
- Connects to the database specified in `CTGOV_DB_CONFIG`.
- Uses `--clean` and `--if-exists` to drop existing objects before restoring (unless disabled).
- Uses `--no-owner` and `--no-privileges` to avoid permission errors during restoration.
- Supports password-less execution by pulling credentials from your environment.

## Exploration & Querying

For scripts related to querying and exploring the AACT data (once initialized), see the [aact_dump_explore](../aact_dump_explore/) directory.
