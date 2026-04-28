# Clinical Data Pipeline — Architecture Document

> **Purpose**: This document captures the full system architecture, codebase structure, implementation decisions, and roadmap for the clinical data ingestion and unification platform. It is intended to serve as the single source of truth for any developer or AI agent working on this project.

---

## 1. Project Vision

Build an automated, multi-source clinical trial data platform that:
1. **Ingests** data from multiple registries (ISRCTN, ClinicalTrials.gov/AACT, EU Clinical Trials Register, etc.)
2. **Normalizes** each source into its own structured PostgreSQL database.
3. **Unifies** all sources into a single, source-agnostic repository (similar to GlobalData).
4. **Automates** the entire pipeline on a schedule, with monitoring and configuration via a web UI.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Dashboard (React + Vite)             │
│    Overview  |  Source Cards  |  Runs  |  Errors     │
└────────────────────────┬────────────────────────────┘
                         │ HTTP
┌────────────────────────▼────────────────────────────┐
│              FastAPI (Control Plane)                  │
│    /sources    /pipelines    /stats    /merge         │
└────────┬──────────────────────────┬─────────────────┘
         │ Prefect API              │ SQL Queries
┌────────▼────────────┐     ┌───────▼─────────────────┐
│   Prefect Server    │     │       PostgreSQL         │
│   (Orchestrator)    │     │                          │
│                     │     │   isrctn_repository      │
│   Schedules,        │     │   ctgov_repository       │
│   Runs, Logs,       │     │   unified_repository     │
│   Retries           │     │                          │
└────────┬────────────┘     └─────────────────────────┘
         │ Executes
┌────────▼────────────────────────────────────────────┐
│            Pipelines (Prefect Flows)                 │
│                                                      │
│   isrctn_flow  ──→  scripts/isrctn/*                 │
│   ctgov_flow   ──→  scripts/ctgov/*                  │
│   merge_flow   ──→  unified DB merge                 │
└──────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Technology | Responsibility |
|:---|:---|:---|
| **Scripts** | Pure Python | Core business logic: downloading, parsing, DB operations. Standalone and runnable via CLI. |
| **Pipelines** | Prefect | Thin wrappers around scripts. Handles orchestration, scheduling, retries, and dependency ordering. |
| **API** | FastAPI | Control plane for source configuration, manual pipeline triggers, and statistics endpoints. |
| **Dashboard** | React + Vite | Web UI for monitoring pipeline status, viewing errors, and configuring sources. |
| **Database** | PostgreSQL | One database per source + one unified database for the backend. |

---

## 3. Codebase Structure

```
clinical-data-pipeline/
│
├── scripts/                        # Core business logic (standalone, CLI-runnable)
│   ├── __init__.py
│   ├── config.py                   # Shared DB config (reads from .env)
│   │
│   ├── isrctn/                     # ISRCTN Registry source
│   │   ├── __init__.py
│   │   ├── download_csv.py         # Downloads trial ID list via Headless Playwright
│   │   ├── populate_queue.py       # Loads CSV IDs into trial_queue table
│   │   ├── fetch_isrctn_data.py    # Fetches XML per trial, parses & stores in 14 tables
│   │   └── init_db.py              # Creates/drops schema from SQL file
│   │
│   └── ctgov/                      # ClinicalTrials.gov (AACT) source
│       ├── __init__.py
│       ├── download_aact.py        # Downloads daily dump ZIP, extracts .dmp file
│       └── init_db.py              # Restores .dmp via pg_restore
│
├── pipelines/                      # Prefect orchestration layer
│   ├── __init__.py
│   ├── isrctn_flow.py              # @flow wrapping ISRCTN download → populate → fetch
│   ├── ctgov_flow.py               # @flow wrapping AACT download → restore
│   ├── merge_flow.py               # @flow for merging sources into unified DB (future)
│   └── master_flow.py              # Top-level @flow that orchestrates all source flows
│
├── api/                            # FastAPI control plane (future)
│   ├── __init__.py
│   ├── main.py                     # App entry point, CORS, lifespan
│   ├── models.py                   # Pydantic schemas (SourceConfig, RunStatus, etc.)
│   ├── routes/
│   │   ├── sources.py              # CRUD for data source configuration
│   │   ├── pipelines.py            # Trigger runs, get run history from Prefect
│   │   └── dashboard.py            # Stats & error endpoints
│   └── services/
│       ├── prefect_client.py       # Proxies requests to Prefect's REST API
│       └── db_stats.py             # Queries source DBs for row counts, last updated
│
├── dashboard/                      # React frontend (future)
│   ├── src/
│   │   ├── App.jsx
│   │   ├── components/
│   │   │   ├── OverviewPanel.jsx   # Total trials, source breakdown, last run times
│   │   │   ├── SourceCard.jsx      # Per-source card: status, toggle, trigger button
│   │   │   ├── RunHistory.jsx      # Timeline/table of recent pipeline executions
│   │   │   └── ErrorViewer.jsx     # Expandable list of failures from error_log
│   │   └── api/
│   │       └── client.js           # Fetch wrapper for FastAPI endpoints
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
│
├── database/                       # SQL schema files
│   ├── isrctn_schema.sql           # 14-table normalized schema for ISRCTN
│   ├── ctgov_schema.sql            # Created by pg_restore (71-table AACT schema)
│   └── unified_schema.sql          # Future: merged/normalized schema
│
├── data/                           # Downloaded data files (gitignored)
│   ├── isrctn_ids.csv              # Latest ISRCTN trial ID list
│   └── aact_dumps/                 # AACT daily dumps organized by date
│       └── YYYY-MM-DD/
│           └── postgres_data.dmp
│
├── .env                            # Environment variables (DB credentials, etc.)
├── .gitignore
├── pyproject.toml                  # Python dependencies (uv-managed)
├── ARCHITECTURE.md                 # This file
└── README.md                       # Quick-start guide
```

---

## 4. Data Sources — Current State

### 4.1 ISRCTN (International Standard Randomised Controlled Trial Number)

| Aspect | Detail |
|:---|:---|
| **Data Format** | XML (per-trial API endpoint) |
| **API Endpoint** | `https://www.isrctn.com/api/trial/{id}/format/default` |
| **ID Source** | CSV download from `https://www.isrctn.com/searchCsv?q=&columns=ISRCTN` |
| **ID Download Method** | Headless Playwright (bypasses JS/cookie protection) |
| **Database** | `isrctn_repository` (PostgreSQL) |
| **Schema** | 14 normalized tables (see `database/isrctn_schema.sql`) |
| **Current Trial Count** | ~28,000 IDs, ~22,000+ successfully ingested |
| **Queue System** | `trial_queue` table with `pending/completed/failed` status |
| **Transaction Model** | Single atomic commit per trial (data + status update) |

**ISRCTN Pipeline Steps:**
1. `download_csv.py` → Downloads all trial IDs via headless browser
2. `populate_queue.py` → Loads IDs into `trial_queue` (uses `ON CONFLICT DO NOTHING`)
3. `fetch_isrctn_data.py` → Fetches XML, parses, and inserts into 14 tables per trial

**ISRCTN Database Tables:**
| Table | Description |
|:---|:---|
| `trial_queue` | Staging queue with retrieval status tracking |
| `trials` | Core trial data (title, design, status, raw XML) |
| `participant_details` | Age, gender, enrolment numbers |
| `participant_types` | Type list (e.g., "Patient", "Healthy volunteer") |
| `external_identifiers` | DOI, EudraCT, ClinicalTrials.gov cross-references |
| `secondary_identifiers` | Additional registry numbers |
| `outcomes` | Primary and secondary outcome measures |
| `ethics_committees` | Ethics approval details |
| `trial_centres` | Recruitment site locations |
| `recruitment_countries` | Country list |
| `conditions` | Medical conditions and disease classifications |
| `interventions` | Treatment details, drug names, phases |
| `interventional_designs` | Allocation, masking, control, assignment |
| `trial_purposes` | Purpose list |
| `data_outputs` | Published outputs and attached files |
| `attached_files` | Trial file metadata |
| `organizations` | Sponsors and funders |
| `contacts` | Contact persons |
| `contact_types` | Contact role types |

### 4.2 ClinicalTrials.gov (AACT)

| Aspect | Detail |
|:---|:---|
| **Data Format** | PostgreSQL dump file (`.dmp`) |
| **Source** | AACT daily static database copies |
| **Download URL** | `https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/{date}` |
| **Database** | `ctgov_repository` (PostgreSQL, restored via `pg_restore`) |
| **Schema** | 71-table AACT schema (created automatically by `pg_restore`) |
| **Size** | ~500MB compressed, ~2GB+ restored |

**AACT Pipeline Steps:**
1. `download_aact.py` → Downloads daily ZIP, extracts `.dmp`, cleans up
2. `init_db.py` → Restores `.dmp` to PostgreSQL via `pg_restore`

### 4.3 Future Sources (Planned)
- **EU Clinical Trials Register** (EudraCT)
- Additional registries as needed

---

## 5. Configuration

### Environment Variables (`.env`)
```env
# Default fallback
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password

# ClinicalTrials.gov (AACT)
CTGOV_DB_HOST=localhost
CTGOV_DB_PORT=5432
CTGOV_DB_NAME=aact
CTGOV_DB_USER=postgres
CTGOV_DB_PASSWORD=your_password

# ISRCTN
ISRCTN_DB_HOST=localhost
ISRCTN_DB_PORT=5432
ISRCTN_DB_NAME=isrctn_repository
ISRCTN_DB_USER=postgres
ISRCTN_DB_PASSWORD=your_password

# Future: Unified DB
# UNIFIED_DB_NAME=clinical_repository
```

### Dependencies (`pyproject.toml`)
```toml
[project]
name = "clinical-data-pipeline"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "playwright>=1.58.0",       # Headless browser for ISRCTN CSV download
    "psycopg2-binary>=2.9.11",  # PostgreSQL driver
    "python-dotenv>=1.2.2",     # .env file loading
    "requests>=2.33.1",         # HTTP client for API calls
    "prefect>=3.0",             # Workflow orchestration (Phase 1)
    # "fastapi>=0.115.0",       # Control plane API (Phase 2)
    # "uvicorn>=0.32.0",        # ASGI server (Phase 2)
]
```

### One-Time Setup
```bash
# Install Playwright browser engine (required for ISRCTN CSV download)
uv run playwright install chromium
uv run playwright install-deps chromium
```

---

## 6. Key Design Decisions

### 6.1 Scripts Stay Pure
The `scripts/` layer contains **no Prefect or FastAPI dependencies**. Every script can be run standalone via `uv run python -m scripts.isrctn.fetch_isrctn_data`. This ensures:
- Easy debugging and testing
- No vendor lock-in to Prefect
- Scripts can be reused in other contexts

### 6.2 Atomic Transactions Per Trial (ISRCTN)
Each trial's data ingestion and queue status update are committed in a **single transaction**. If any part fails, everything is rolled back. The error is then logged to the queue in a separate transaction. This prevents "half-finished" trials in the database.

### 6.3 Duplicate Handling
- **`populate_queue.py`**: Uses `INSERT ... ON CONFLICT DO NOTHING` with `cur.rowcount` to accurately track new vs. duplicate IDs without breaking the transaction.
- **`fetch_isrctn_data.py`**: Uses `ON CONFLICT ... DO UPDATE` for upsert behavior on core tables, and `DELETE + INSERT` for child tables.

### 6.4 Schema Flexibility
Fields that ISRCTN researchers commonly use as free-text (despite being labeled as codes) are stored as `TEXT` to prevent truncation errors. Fields with predictable lengths use `VARCHAR(255)` or `VARCHAR(50)`. This was determined empirically by analyzing failing trials.

### 6.5 Headless Browser for ISRCTN
The ISRCTN website uses JavaScript challenges and cookies to protect the CSV download. We use **Headless Playwright** (Chromium) to bypass this transparently. This is fully automated and requires no visible browser window.

---

## 7. Implementation Roadmap

### Phase 1: Prefect Pipelines (Current Priority)
- [ ] Add `prefect` to dependencies
- [ ] Create `pipelines/isrctn_flow.py` — wraps ISRCTN scripts as `@task` and `@flow`
- [ ] Create `pipelines/ctgov_flow.py` — wraps AACT scripts as `@task` and `@flow`
- [ ] Create `pipelines/master_flow.py` — orchestrates all source flows
- [ ] Configure scheduling (e.g., ISRCTN weekly, AACT daily)
- [ ] Test with Prefect Server UI for monitoring
- [ ] Set up error notifications

### Phase 2: Unified Database
- [ ] Design `database/unified_schema.sql` — source-agnostic normalized schema
- [ ] Create `scripts/merge/` — ETL scripts to merge ISRCTN + AACT into unified DB
- [ ] Create `pipelines/merge_flow.py` — Prefect flow for the merge step
- [ ] Map ~40-50 critical data points across sources

### Phase 3: FastAPI Control Plane
- [ ] Create `api/` with FastAPI app
- [ ] Implement source configuration CRUD (`/sources`)
- [ ] Implement pipeline trigger/status endpoints (`/pipelines`)
- [ ] Implement dashboard stats endpoints (`/stats`)
- [ ] Proxy Prefect API for run history

### Phase 4: Dashboard UI
- [ ] Create `dashboard/` with React + Vite
- [ ] Build Overview Panel (trial counts, last runs)
- [ ] Build Source Cards (enable/disable, trigger, schedule)
- [ ] Build Run History timeline
- [ ] Build Error Viewer

### Phase 5: Additional Sources
- [ ] EU Clinical Trials Register integration
- [ ] Other registries as needed
- [ ] Each new source follows the pattern: `scripts/source/` → `pipelines/source_flow.py`

---

## 8. CLI Reference

### ISRCTN Commands
```bash
# Download trial IDs (requires Playwright setup)
uv run python -m scripts.isrctn.download_csv --out data/isrctn_ids.csv

# Initialize database schema
uv run python -m scripts.isrctn.init_db              # Create tables
uv run python -m scripts.isrctn.init_db --drop        # Drop and recreate

# Populate the processing queue
uv run python -m scripts.isrctn.populate_queue data/isrctn_ids.csv

# Fetch trial data from API
uv run python -m scripts.isrctn.fetch_isrctn_data
```

### AACT Commands
```bash
# Download today's dump
uv run python -m scripts.ctgov.download_aact

# Download a specific date's dump
uv run python -m scripts.ctgov.download_aact --date 2026-04-28

# Restore the dump to PostgreSQL
uv run python -m scripts.ctgov.init_db data/aact_dumps/2026-04-28/postgres_data.dmp
```

---

## 9. Known Issues & Lessons Learned

> [!WARNING]
> **ISRCTN Data Quality**: Researchers frequently paste long paragraphs into fields designed for short codes (e.g., `assignment` in `interventional_designs` had a 1,218-character entry). Always use `TEXT` for fields that might contain free-form data.

> [!NOTE]
> **Transaction Rollback Bug (Fixed)**: The original `populate_queue.py` used try/except with `UniqueViolation`, which caused `conn.rollback()` to wipe all previous insertions in the same transaction. Fixed by using `ON CONFLICT DO NOTHING`.

> [!NOTE]
> **Playwright System Dependencies**: On Linux/WSL, `uv run playwright install chromium` alone is not enough. You also need `uv run playwright install-deps chromium` to install system libraries like `libnspr4`.

---

## 10. Files to Migrate to New Project

When creating the new `clinical-data-pipeline` project, copy these files:

### Must Copy
| Source | Destination |
|:---|:---|
| `scripts/config.py` | `scripts/config.py` |
| `scripts/__init__.py` | `scripts/__init__.py` |
| `scripts/isrctn/*.py` | `scripts/isrctn/*.py` |
| `scripts/ctgov/download_aact.py` | `scripts/ctgov/download_aact.py` |
| `scripts/ctgov/init_db.py` | `scripts/ctgov/init_db.py` |
| `database/isrctn_schema.sql` | `database/isrctn_schema.sql` |
| `.env` | `.env` |
| `.gitignore` | `.gitignore` |

### Do NOT Copy
| File | Reason |
|:---|:---|
| `main.py` | Placeholder, not used |
| `memories/` | Test artifacts |
| `output/` | Test artifacts |
| `data/test_seripts/` | Diagnostic scripts (already deleted) |
| `data/*.csv` (except `isrctn_ids.csv`) | Test files |
| `data/*.pdf` | Test files |
| `scripts/aact_dump_explore/` | Exploratory scripts, not production |
| `PROJECT_SETUP.md` | Replaced by this document |
