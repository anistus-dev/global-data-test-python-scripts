# Clinical trial Data Ingestion & Exploration

A unified repository for fetching, storing, and exploring clinical trial data from multiple sources.

## Project Structure
- `scripts/isrctn/`: ISRCTN data ingestion pipeline and database management.
- `scripts/ctgov/`: AACT (ClinicalTrials.gov) database management.
- `database/`: SQL schemas for integrated trial repositories.

## Installation
Ensure you have `uv` installed, then set up the environment:
```bash
uv sync
```

## AACT (ClinicalTrials.gov) Setup
For detailed setup and usage instructions for the ClinicalTrials.gov database creation and management, see the [CTGov README](scripts/ctgov/README.md).

## ISRCTN Pipeline
For detailed setup and usage instructions for the ISRCTN ingestion pipeline, see the [ISRCTN README](scripts/isrctn/README.md).