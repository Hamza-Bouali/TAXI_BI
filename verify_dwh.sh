#!/bin/bash

# Data Warehouse Verification Script
# This script checks if the data warehouse is properly set up and contains data

echo "================================================"
echo "NYC Taxi Data Warehouse Verification"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Connection details
DWH_HOST="localhost"
DWH_PORT="5433"
DWH_USER="dwh_admin"
DWH_DB="taxi_dwh"

echo "1. Checking PostgreSQL connection..."
if PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -c "SELECT version();" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Connection successful${NC}"
else
    echo -e "${RED}✗ Connection failed${NC}"
    echo "Please ensure the postgres-dwh container is running"
    exit 1
fi

echo ""
echo "2. Checking dimension tables..."

# Function to check table
check_table() {
    local table_name=$1
    local count=$(PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -t -c "SELECT COUNT(*) FROM $table_name;" 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        count=$(echo $count | xargs)  # Trim whitespace
        echo -e "   ${GREEN}✓${NC} $table_name: $count records"
    else
        echo -e "   ${RED}✗${NC} $table_name: Table not found or error"
    fi
}

check_table "dim_date"
check_table "dim_location"
check_table "dim_vendor"
check_table "dim_payment_type"
check_table "dim_rate_code"

echo ""
echo "3. Checking fact table..."
check_table "fact_trips"

echo ""
echo "4. Checking views..."

check_view() {
    local view_name=$1
    if PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -c "SELECT * FROM $view_name LIMIT 1;" > /dev/null 2>&1; then
        echo -e "   ${GREEN}✓${NC} $view_name exists"
    else
        echo -e "   ${YELLOW}⚠${NC} $view_name not found"
    fi
}

check_view "vw_trip_summary_by_date"
check_view "vw_trip_summary_by_location"
check_view "vw_payment_analysis"
check_view "vw_data_quality_orphans"

echo ""
echo "5. Checking functions..."

check_function() {
    local function_name=$1
    local exists=$(PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -t -c "SELECT COUNT(*) FROM pg_proc WHERE proname = '$function_name';" 2>/dev/null)
    exists=$(echo $exists | xargs)
    
    if [ "$exists" -gt 0 ]; then
        echo -e "   ${GREEN}✓${NC} $function_name exists"
    else
        echo -e "   ${YELLOW}⚠${NC} $function_name not found"
    fi
}

check_function "populate_dim_date"
check_function "get_or_create_location_key"

echo ""
echo "6. Data quality checks..."

# Check for data in fact table by month
echo "   Recent data loads:"
PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -c "
    SELECT 
        source_file,
        COUNT(*) as record_count,
        TO_CHAR(MIN(loaded_at), 'YYYY-MM-DD HH24:MI') as first_loaded,
        TO_CHAR(MAX(loaded_at), 'YYYY-MM-DD HH24:MI') as last_loaded
    FROM fact_trips
    GROUP BY source_file
    ORDER BY MAX(loaded_at) DESC
    LIMIT 5;
" 2>/dev/null

echo ""
echo "7. Index status..."
PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -c "
    SELECT 
        tablename,
        indexname,
        idx_scan as scans
    FROM pg_stat_user_indexes
    WHERE schemaname = 'public'
    ORDER BY idx_scan DESC
    LIMIT 10;
" 2>/dev/null

echo ""
echo "8. Database size..."
PGPASSWORD=dwh_password psql -h $DWH_HOST -p $DWH_PORT -U $DWH_USER -d $DWH_DB -c "
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
" 2>/dev/null

echo ""
echo "================================================"
echo "Verification Complete"
echo "================================================"
