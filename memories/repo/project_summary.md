# AACT Clinical Trials Data Project - Project Summary

## Project Goal
This project provides a set of Python scripts to interact with and explore the AACT (ClinicalTrials.gov API Aggregator) database. It is designed to simplify schema discovery and trial-specific data extraction from the complex `ctgov` schema (71+ tables).

## Key Components

### Database
- **Engine**: PostgreSQL
- **Schema**: `ctgov`
- **Tables**: ~71 tables containing clinical trial metadata, results, and administrative data.
- **Key Trial Identifier**: `nct_id` (used across most tables).

### Core Scripts (`scripts/`)
- `explore_ctgov.py`: Generates a full schema overview, including column types and sample data for all tables.
- `query_trial_by_nct_id.py`: Fetches all data related to a single NCT ID and outputs a human-readable text report.
- `query_trial_to_csv.py`: Exports all related data for a single NCT ID into a multi-table CSV file.
- `check_schema.py`: Perform basic schema validation.

### Data Model Highlights
- `studies`: The central table for trial records.
- `conditions`, `interventions`, `outcomes`: Primary clinical data points.
- `reported_events`: Large volume table (~11.5M rows) containing adverse events.

## For AI Agents
- **Context**: This is a data-heavy project. When querying, always check for the existence of `nct_id` in the target table.
- **Performance**: Some tables are very large. Use `LIMIT` when performing exploratory queries.
- **Execution**: Use `uv run python -m scripts.<script_name>` for execution.

## Evolution
This is currently a "test space" or precursor to a larger project. The focus is on robust data extraction and understanding the AACT schema before building more complex features like comparative analysis or time-series tracking.
