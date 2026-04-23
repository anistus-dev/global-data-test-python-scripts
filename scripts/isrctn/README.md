# ISRCTN Data Ingestion Scripts

These scripts are used to fetch clinical trial data from the ISRCTN website via their public API and store it in a normalized PostgreSQL database.

## 1. Database Setup

### Create the Database
First, connect to your PostgreSQL instance as the superuser (usually `postgres`):

```bash
psql -U postgres -h localhost
```
*(If you are prompted for a password, enter the password for the `postgres` user)*

Once connected, create the dedicated database and assign an owner (the user you will use in your scripts):

```sql
CREATE DATABASE isrctn_repository OWNER your_username;
```

> [!TIP]
> If you get an error like `FATAL: database "your_username" does not exist` when trying to connect, it's because `psql` tries to connect to a database with the same name as your user by default. Use `-d postgres` to connect to the default system database first:
> `psql -U your_username -h localhost -d postgres`

### Configure Credentials
The scripts use the configuration defined in `scripts/config.py`. You can configure the database connection using the following environment variables in your `.env` file:

```env
ISRCTN_DB_HOST=localhost
ISRCTN_DB_PORT=5432
ISRCTN_DB_NAME=isrctn_repository
ISRCTN_DB_USER=postgres
ISRCTN_DB_PASSWORD=your_password
```

## 2. Usage Instructions

### Step 1: Initialize Database Schema
Apply the comprehensive SQL schema to your new database:
```bash
uv run python -m scripts.isrctn.init_db
```
*(Optionally, you can pass a custom schema path: `uv run python -m scripts.isrctn.init_db --schema database/my_schema.sql`)*

### Step 2: Populate the Processing Queue
Load trial IDs from your CSV file into the staging queue. The script automatically looks for an `ISRCTN` column:
```bash
uv run python -m scripts.isrctn.populate_queue path/to/your/input_file.csv
```

### Step 3: Fetch Data from API
Run the crawler to fetch XML data for all pending trials and populate the normalized tables:
```bash
uv run python -m scripts.isrctn.fetch_isrctn_data
```

## 3. Script Details

-   **`init_db.py`**: A generic script to execute SQL schema files against the database.
-   **`populate_queue.py`**: Reads a CSV, identifies trial IDs, and inserts them into the `trial_queue` table with a `pending` status.
-   **`fetch_isrctn_data.py`**: 
    -   Queries the queue for pending trials.
    -   Fetches trial data from `https://www.isrctn.com/api/trial/{id}/format/default`.
    -   Parses complex XML namespaces and maps them to normalized tables.
    -   Updates the queue status to `completed` or `failed` with error logs.

## 4. Monitoring Progress
You can check the status of your ingestion by querying the `trial_queue` table:
```sql
SELECT retrieval_status, COUNT(*) FROM trial_queue GROUP BY retrieval_status;
```
