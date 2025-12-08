# Data Warehouse SQL Scripts

This directory contains SQL scripts for the NYC Taxi Trip OLAP Data Warehouse.

## Database Schema

The data warehouse implements a **Star Schema** with the following components:

### Dimension Tables

1. **dim_date** - Time dimension for temporal analysis
   - Date key (YYYYMMDD format)
   - Year, quarter, month, day attributes
   - Day of week, weekend flags
   - Season classification

2. **dim_location** - Geographic dimension for trip locations
   - Location zones and boroughs
   - Airport, downtown, tourist area flags
   - Latitude/longitude coordinates

3. **dim_vendor** - Vendor dimension
   - Taxi service providers
   - Active status tracking

4. **dim_payment_type** - Payment method dimension
   - Payment types (credit, cash, etc.)
   - Electronic payment indicator

5. **dim_rate_code** - Rate code dimension
   - Standard, JFK, Newark, etc.
   - Rate descriptions

### Fact Table

**fact_trips** - Trip transaction data
- Foreign keys to all dimension tables
- Trip metrics: distance, duration, passenger count
- Financial metrics: fares, tips, tolls, total amount
- Source tracking for data lineage

## Script Execution Order

The scripts are executed automatically when the `postgres-dwh` container starts:

1. **01_init_dwh.sh** - Initialization script (orchestrates other scripts)
2. **create_dwh_schema.sql** - Creates all dimension and fact tables
3. **populate_dimensions.sql** - Populates reference data in dimensions
4. **helper_functions.sql** - Creates helper functions and views

## Manual Execution

To manually run scripts against the data warehouse:

```bash
# Connect to the data warehouse
docker exec -it <container_name> psql -U dwh_admin -d taxi_dwh

# Run a specific script
docker exec -i <container_name> psql -U dwh_admin -d taxi_dwh < sql/create_dwh_schema.sql
```

## Data Warehouse Connection

**Host**: postgres-dwh (internal) or localhost:5433 (external)
**Database**: taxi_dwh
**User**: dwh_admin
**Password**: dwh_password

## Views Available

- `vw_trip_summary_by_date` - Daily trip statistics
- `vw_trip_summary_by_location` - Location-based analysis
- `vw_payment_analysis` - Payment method breakdown
- `vw_data_quality_orphans` - Data quality monitoring

## Functions Available

- `populate_dim_date(start_date, end_date)` - Generate date dimension records
- `get_or_create_location_key(loc_id)` - Upsert location dimension

## Data Quality

The data warehouse includes built-in data quality checks:
- Foreign key constraints ensure referential integrity
- Views for monitoring orphan records
- Validation of trip metrics (duration, distance, amounts)

## Performance Optimization

The schema includes:
- Indexes on all foreign keys
- Indexes on frequently queried columns (dates, locations)
- Partitioning considerations for large fact tables (future enhancement)

## Maintenance

Regular maintenance tasks:
```sql
-- Vacuum and analyze tables
VACUUM ANALYZE fact_trips;
VACUUM ANALYZE dim_date;
VACUUM ANALYZE dim_location;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```
