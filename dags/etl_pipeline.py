"""
NYC Green Taxi Data Pipeline
Extract NYC Green Taxi trip data from CloudFront API and store it locally.
Supports backfilling with date-based parameterization.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_taxi_data(**context):
    """Extract NYC Green Taxi data from CloudFront API based on execution date"""
    
    # Get the execution date for backfilling
    execution_date = context['ds']  # Format: YYYY-MM-DD
    logical_date = context['logical_date']  # datetime object
    
    # Format the date as YYYY-MM for the API URL
    year_month = logical_date.strftime('%Y-%m')
    
    # Construct the URL
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year_month}.parquet"
    
    print(f"Extracting data for {year_month}")
    print(f"URL: {url}")
    
    # Create data directory if it doesn't exist
    data_dir = '/opt/airflow/dags/data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Download the file
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Save the parquet file
        file_path = f"{data_dir}/green_tripdata_{year_month}.parquet"
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Successfully downloaded {len(response.content)} bytes")
        print(f"Saved to: {file_path}")
        
        # Push metadata to XCom
        context['ti'].xcom_push(key='file_path', value=file_path)
        context['ti'].xcom_push(key='year_month', value=year_month)
        context['ti'].xcom_push(key='file_size', value=len(response.content))
        
        return file_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        raise


def validate_data(**context):
    """Validate the downloaded parquet file"""
    
    file_path = context['ti'].xcom_pull(key='file_path', task_ids='extract')
    year_month = context['ti'].xcom_pull(key='year_month', task_ids='extract')
    
    print(f"Validating data for {year_month}")
    
    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        print(f"Total records: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print(f"Date range: {df['lpep_pickup_datetime'].min()} to {df['lpep_pickup_datetime'].max()}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Push validation metrics to XCom
        context['ti'].xcom_push(key='record_count', value=len(df))
        context['ti'].xcom_push(key='columns', value=list(df.columns))
        
        return len(df)
        
    except Exception as e:
        print(f"Error validating file: {e}")
        raise


def load_data(**context):
    """Load data into PostgreSQL database"""
    
    file_path = context['ti'].xcom_pull(key='file_path', task_ids='extract')
    year_month = context['ti'].xcom_pull(key='year_month', task_ids='extract')
    
    print(f"Loading data for {year_month} into PostgreSQL")
    print(f"File: {file_path}")
    
    # Database connection string (using the local postgres service from docker-compose)
    # If you want to use Supabase, ensure network connectivity and IPv4 resolution
    stg_table_cred = {
    'USER':'postgres.fnsjmxoaclpgbrhsxwfr', 
    'PASSWORD':"admin",
    'HOST':"aws-1-eu-west-1.pooler.supabase.com" ,
    'PORT':'6543' ,
    'DBNAME':'postgres',}
    db_connection_string = f"postgresql+psycopg2://{stg_table_cred['USER']}:{stg_table_cred['PASSWORD']}@{stg_table_cred['HOST']}:{stg_table_cred['PORT']}/{stg_table_cred['DBNAME']}?sslmode=require"
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(db_connection_string)
        
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        print(f"Loading {len(df)} records to database...")
        
        # Table name based on year-month
        table_name = f'green_taxi_{year_month.replace("-", "_")}'
        
        # Load data to PostgreSQL
        # if_exists='replace' will drop and recreate the table
        # Use 'append' if you want to add to existing data
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10000,  # Insert in chunks for better performance
            method='multi'  # Use multi-row INSERT statements
        )
        
        print(f"Successfully loaded {len(df)} records to table '{table_name}'")
        
        # Verify the load
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"Verification: Table '{table_name}' contains {count} records")
        
        engine.dispose()
        
        return len(df)
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise


# Define the DAG
with DAG(
    'nyc_green_taxi_pipeline',
    default_args=default_args,
    description='Extract NYC Green Taxi data with date-based backfilling',
    
    schedule='@monthly',  # Run monthly to get each month's data
    start_date=datetime(2020, 1, 1),  # Start from January 2024
    catchup=True,  # Enable backfilling for historical data
    max_active_runs=3,  # Limit concurrent runs
    tags=['nyc', 'taxi', 'etl', 'green-taxi'],
) as dag:
    
    # Task 1: Start
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting NYC Green Taxi data extraction for {{ ds }}"',
    )
    
    # Task 2: Extract
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_taxi_data,
    )
    
    # Task 3: Validate
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data,
    )
    
    # Task 4: Load
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )
    
    # Task 5: End
    end = BashOperator(
        task_id='end',
        bash_command='echo "NYC Green Taxi pipeline completed for {{ ds }}!"',
    )
    
    # Define task dependencies
    start >> extract >> validate >> load >> end
