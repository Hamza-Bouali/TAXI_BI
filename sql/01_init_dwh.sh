#!/bin/bash
set -e

echo "================================================"
echo "Initializing Taxi Data Warehouse"
echo "================================================"

# Run schema creation
echo "Creating data warehouse schema..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/create_dwh_schema.sql

# Populate dimension tables
echo "Populating dimension tables..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/populate_dimensions.sql

# Create helper functions and views
echo "Creating helper functions and views..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/helper_functions.sql

echo "================================================"
echo "Data Warehouse initialization complete!"
echo "================================================"
