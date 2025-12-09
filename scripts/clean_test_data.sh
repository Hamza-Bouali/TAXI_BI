#!/bin/bash
# Clean Test Data Wrapper Script
# Runs the Python cleanup script inside the Airflow worker container

echo "Running cleanup script in Airflow container..."
docker compose exec -T airflow-worker python - < scripts/clean_test_data.py
