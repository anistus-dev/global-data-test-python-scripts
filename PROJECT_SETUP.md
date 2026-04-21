# AACT Clinical Trials Data Project

## Project Overview
Working with AACT (ClinicalTrials.gov API Aggregator) PostgreSQL database containing clinical trials data in the `ctgov` schema. The database contains 71 tables with clinical trial information.

## Database Details
- **Database Name**: aact
- **Schema**: ctgov
- **Total Tables**: 71
- **Host**: localhost:5432
- **Credentials**: postgres/password (in DB_CONFIG in scripts)

## Project Structure
```
python/
├── scripts/                    # Main scripts directory
│   ├── explore_ctgov.py       # Explore all tables, columns, sample data
│   ├── query_trial_by_nct_id.py    # Query specific trial - text output
│   ├── query_trial_to_csv.py   # Query specific trial - CSV output
│   └── check_schema.py        # Schema validation script
├── output/                     # Generated output directory (auto-created)
│   ├── ctgov_exploration_output.txt
│   ├── trial_data_<nct_id>.txt
│   └── trial_data_<nct_id>.csv
├── PROJECT_SETUP.md           # This file
├── pyproject.toml             # Project config (uses UV package manager)
└── README.md
```

## Scripts Built

### 1. **explore_ctgov.py**
Explores the entire ctgov schema for discovery.
- Lists all 71 tables
- Shows columns, data types, nullable status for each table
- Displays sample data (first 5 rows) from each table
- **Output**: `output/ctgov_exploration_output.txt`
- **Usage**: `uv run python -m scripts.explore_ctgov`

### 2. **query_trial_by_nct_id.py**
Retrieves all data for a specific trial across all related tables.
- Queries tables containing `nct_id` column
- Shows both tables WITH data and empty tables (no records for that trial)
- Displays column definitions and actual data rows
- **Output**: `output/trial_data_<nct_id>.txt`
- **Usage**: `uv run python -m scripts.query_trial_by_nct_id <NCT_ID>`
- **Example**: `uv run python -m scripts.query_trial_by_nct_id NCT00842166`

### 3. **query_trial_to_csv.py**
Exports trial data to CSV format for Excel/analysis.
- Format: Table name (row 1) → Column names (row 2) → Data rows
- Includes empty tables with headers
- Blank row separates each table
- **Output**: `output/trial_data_<nct_id>.csv`
- **Usage**: `uv run python -m scripts.query_trial_to_csv <NCT_ID>`
- **Example**: `uv run python -m scripts.query_trial_to_csv NCT00842166`

### 4. **check_schema.py**
Schema validation script for basic checks.

## Key Facts About Data

### Table Coverage
- **71 total tables** in ctgov schema
- Almost all tables have `nct_id` column for trial identification
- When querying a specific trial:
  - Tables with records for that trial: displayed with data
  - Tables without records for that trial: displayed with empty/no data message
  - Total records for NCT00842166: 40 records across 31 tables with data

### Key Tables (Most Important)
- `studies`: Main trial information (579,194 rows)
- `conditions`: Trial conditions/diseases (1,032,410 rows)
- `interventions`: Trial interventions/treatments (979,228 rows)
- `outcomes`: Trial outcome measures (641,506 rows)
- `reported_events`: Adverse events data (11,558,043 rows)
- `facilities`: Trial sites (3,423,678 rows)
- `participants_flow`: Participant information
- `design_outcomes`: Study design outcomes
- And 63 more tables...

## Database Configuration
Database credentials in each script (can be modified as needed):
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'aact',
    'user': 'postgres',
    'password': 'password',
}
```

## Running Scripts

### Prerequisites
- UV package manager (or pip)
- PostgreSQL with AACT dump imported into ctgov schema
- Python 3.7+

### Quick Start
```bash
# Explore schema
uv run python -m scripts.explore_ctgov

# Query specific trial (text output)
uv run python -m scripts.query_trial_by_nct_id NCT00842166

# Query specific trial (CSV output)
uv run python -m scripts.query_trial_to_csv NCT00842166
```

All outputs go to `output/` directory (auto-created if missing).

## Recent Updates
- Scripts now display ALL tables with nct_id column (including empty ones)
- Outputs organized in `output/` directory
- Progress indicators in CSV export: ✓ (data), ⊘ (no records), ✗ (error)
- Text output shows "(No data available for this trial)" for empty tables

## Common Queries

### Get all conditions for a trial
```bash
uv run python -m scripts.query_trial_by_nct_id NCT00842166
# Look for: conditions, browse_conditions, all_conditions tables
```

### Export trial data for analysis
```bash
uv run python -m scripts.query_trial_to_csv NCT00842166
# Open output/trial_data_NCT00842166.csv in Excel/spreadsheet
```

### Understand table structure
```bash
uv run python -m scripts.explore_ctgov
# Outputs full schema to output/ctgov_exploration_output.txt
```

## For AI Agents / Other Team Members
This project also has agent documentation in `/memories/repo/project_summary.md` (accessible to GitHub Copilot agents and AI assistants). When assigning tasks to other agents:
1. Direct them to this PROJECT_SETUP.md file
2. Or mention the `/memories/repo/project_summary.md` resource
3. All scripts are in `scripts/` directory with clear naming

## Troubleshooting

### Connection Error
- Verify PostgreSQL is running
- Check DB_CONFIG credentials in scripts
- Ensure AACT data is imported in ctgov schema

### No Output
- Check that `output/` directory has write permissions
- Verify the nct_id exists in database
- Run `explore_ctgov.py` to check schema

### Memory Issues
- Reduce LIMIT in query functions if dealing with very large trials
- Process tables individually if needed

## Future Enhancements
- Filter/search capabilities
- JSON export format
- Comparative analysis across multiple trials
- Time-series analysis
- Data validation/quality checks
- API wrapper for scripted access
