# TAX_BI Scripts

Utility scripts for managing the NYC Green Taxi data pipeline.

## Clean Test Data

### Description
Removes all test data from both staging and data warehouse databases to prepare for production backfills.

### What It Does
1. **Staging Database**: Drops all `green_taxi_*` tables
2. **Data Warehouse**: Truncates the `fact_trips` table (preserves schema and dimensions)

### Usage

**Option 1: Run via bash wrapper (recommended)**
```bash
./scripts/clean_test_data.sh
```

**Option 2: Run Python script directly in container**
```bash
docker compose exec -T airflow-worker python - < scripts/clean_test_data.py
```

**Option 3: Interactive mode with confirmation**
```bash
docker compose exec airflow-worker python /opt/airflow/scripts/clean_test_data.py
```

### Output Example
```
============================================================
NYC GREEN TAXI - TEST DATA CLEANUP SCRIPT
============================================================

This script will:
  1. Drop all green_taxi_* tables from staging database
  2. Truncate fact_trips table in data warehouse

⚠️  WARNING: This action cannot be undone!

============================================================
CLEANING STAGING DATABASE
============================================================
Found 6 staging tables:
  • green_taxi_2024_01
  • green_taxi_2024_02
  ...

Dropping tables...
  ✓ Dropped: green_taxi_2024_01
  ✓ Dropped: green_taxi_2024_02
  ...

✅ Successfully dropped 6 staging tables

============================================================
CLEANING DATA WAREHOUSE
============================================================
Records before cleanup: 637,801
Date range: 20240101 → 20251031

Truncating fact_trips table...
Records after cleanup: 0

✅ Successfully removed 637,801 records from data warehouse

============================================================
CLEANUP SUMMARY
============================================================
Staging Database: ✅ SUCCESS
Data Warehouse:   ✅ SUCCESS
============================================================

🎉 All test data cleaned successfully!
```

### After Cleanup

Run a fresh backfill:
```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id nyc_green_taxi_pipeline \
  --from-date 2024-01-01 \
  --to-date 2025-10-31 \
  --max-active-runs 3
```

### Safety Features
- ✅ Interactive confirmation prompt (when run in TTY)
- ✅ Transaction-based operations (rollback on error)
- ✅ Detailed logging of all actions
- ✅ Pre-cleanup statistics displayed
- ✅ Post-cleanup verification

### Credentials
The script uses credentials embedded in `/dags/etl_pipeline.py`:
- **Staging**: Supabase (EU Central 2)
- **Data Warehouse**: Supabase (EU North 1)

### Notes
- ⚠️ Does NOT drop dimension tables (dim_date, dim_location, etc.)
- ⚠️ Does NOT affect Airflow metadata
- ✅ Preserves data warehouse schema structure
- ✅ Safe to run multiple times (idempotent)
