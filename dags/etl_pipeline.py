"""
NYC Green Taxi Data Pipeline
Extract NYC Green Taxi trip data from CloudFront API and store it locally.
Supports backfilling with date-based parameterization.
Loads data into both staging database and OLAP data warehouse.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text
import numpy as np

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


def check_and_create_dwh_schema(**context):
    """Check if DWH schema exists, create if not"""
    
    print("Checking Data Warehouse schema...")
    
    # Data Warehouse connection
    dwh_cred = {
        'USER': 'dwh_admin',
        'PASSWORD': 'dwh_password',
        'HOST': 'postgres-dwh',  # Docker service name
        'PORT': '5432',
        'DBNAME': 'taxi_dwh',
    }
    dwh_connection_string = f"postgresql+psycopg2://{dwh_cred['USER']}:{dwh_cred['PASSWORD']}@{dwh_cred['HOST']}:{dwh_cred['PORT']}/{dwh_cred['DBNAME']}"
    
    try:
        engine = create_engine(dwh_connection_string)
        
        with engine.connect() as conn:
            # Check if fact_trips table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'fact_trips'
                );
            """))
            schema_exists = result.scalar()
            
            if schema_exists:
                print("✓ Data warehouse schema already exists")
                
                # Check all required tables
                required_tables = ['dim_date', 'dim_location', 'dim_vendor', 
                                 'dim_payment_type', 'dim_rate_code', 'fact_trips']
                
                for table in required_tables:
                    result = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = '{table}'
                        );
                    """))
                    exists = result.scalar()
                    print(f"  {'✓' if exists else '✗'} {table}: {'exists' if exists else 'missing'}")
                
                # Check dimension data
                dim_counts = {}
                for table in ['dim_date', 'dim_vendor', 'dim_payment_type', 'dim_rate_code']:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    dim_counts[table] = count
                    print(f"  {table}: {count} records")
                
            else:
                print("✗ Data warehouse schema does not exist. Creating...")
                
                # Read and execute schema creation script
                schema_sql_path = '/opt/airflow/sql/create_dwh_schema.sql'
                print(f"Reading schema from: {schema_sql_path}")
                
                with open(schema_sql_path, 'r') as f:
                    schema_sql = f.read()
                
                # Execute schema creation
                conn.execute(text(schema_sql))
                conn.commit()
                print("✓ Schema created successfully")
                
                # Populate dimensions
                populate_sql_path = '/opt/airflow/sql/populate_dimensions.sql'
                print(f"Populating dimensions from: {populate_sql_path}")
                
                with open(populate_sql_path, 'r') as f:
                    populate_sql = f.read()
                
                conn.execute(text(populate_sql))
                conn.commit()
                print("✓ Dimensions populated successfully")
                
                # Create helper functions
                helpers_sql_path = '/opt/airflow/sql/helper_functions.sql'
                print(f"Creating helper functions from: {helpers_sql_path}")
                
                with open(helpers_sql_path, 'r') as f:
                    helpers_sql = f.read()
                
                conn.execute(text(helpers_sql))
                conn.commit()
                print("✓ Helper functions created successfully")
                
        engine.dispose()
        print("✓ Schema check complete")
        return True
        
    except Exception as e:
        print(f"Error checking/creating schema: {e}")
        raise


def load_to_dwh(**context):
    """Transform and load data into the OLAP Data Warehouse"""
    
    file_path = context['ti'].xcom_pull(key='file_path', task_ids='extract')
    year_month = context['ti'].xcom_pull(key='year_month', task_ids='extract')
    
    print(f"Loading data for {year_month} into Data Warehouse")
    print(f"File: {file_path}")
    
    # Data Warehouse connection
    dwh_cred = {
        'USER': 'postgres.xgoybmnqftaiismchzhj',
        'PASSWORD': 'nyc_data',
        'HOST': 'aws-1-eu-north-1.pooler.supabase.com',  # Docker service name
        'PORT': '6543',
        'DBNAME': 'postgres',
    }
    dwh_connection_string = f"postgresql+psycopg2://{dwh_cred['USER']}:{dwh_cred['PASSWORD']}@{dwh_cred['HOST']}:{dwh_cred['PORT']}/{dwh_cred['DBNAME']}"
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(dwh_connection_string)
        
        # Read the parquet file
        df = pd.read_parquet(file_path)
        
        print(f"Transforming {len(df)} records for data warehouse...")
        
        # Clean and prepare data
        df = df.copy()
        
        # Handle missing values and data types
        df['VendorID'] = df['VendorID'].fillna(-1).astype(int)
        df['RatecodeID'] = df['RatecodeID'].fillna(-1).astype(int)
        df['PULocationID'] = df['PULocationID'].fillna(-1).astype(int)
        df['DOLocationID'] = df['DOLocationID'].fillna(-1).astype(int)
        df['payment_type'] = df['payment_type'].fillna(-1).astype(int)
        df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)
        
        # Calculate trip duration in minutes
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df['trip_duration_minutes'] = (
            (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60
        ).round().astype(int)
        
        # Extract date key (YYYYMMDD format)
        df['pickup_date_key'] = df['lpep_pickup_datetime'].dt.strftime('%Y%m%d').astype(int)
        
        # Filter out invalid records
        df = df[
            (df['trip_distance'] > 0) & 
            (df['fare_amount'] > 0) & 
            (df['trip_duration_minutes'] > 0) &
            (df['trip_duration_minutes'] < 1440)  # Less than 24 hours
        ]
        
        print(f"After filtering: {len(df)} valid records")
        
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # 1. Populate dim_location for new locations
                print("Updating dim_location...")
                unique_pickup_locations = df['PULocationID'].unique()
                unique_dropoff_locations = df['DOLocationID'].unique()
                all_locations = np.unique(np.concatenate([unique_pickup_locations, unique_dropoff_locations]))
                
                for loc_id in all_locations:
                    if loc_id > 0:  # Skip invalid location IDs
                        # Check if location exists
                        result = conn.execute(text(
                            "SELECT location_key FROM dim_location WHERE location_id = :loc_id"
                        ), {"loc_id": int(loc_id)})
                        
                        if result.fetchone() is None:
                            # Get next location_key
                            max_key_result = conn.execute(text(
                                "SELECT COALESCE(MAX(location_key), 0) FROM dim_location WHERE location_key > 0"
                            ))
                            next_key = max_key_result.scalar() + 1
                            
                            # Insert new location
                            conn.execute(text("""
                                INSERT INTO dim_location (location_key, location_id, zone_name, borough)
                                VALUES (:key, :id, :name, :borough)
                            """), {
                                "key": next_key,
                                "id": int(loc_id),
                                "name": f"Zone {loc_id}",
                                "borough": "Unknown"
                            })
                
                # 2. Create lookup dictionaries for dimension keys
                print("Creating dimension key lookups...")
                
                # Vendor lookup
                vendor_lookup = pd.read_sql(
                    "SELECT vendor_id, vendor_key FROM dim_vendor",
                    conn
                ).set_index('vendor_id')['vendor_key'].to_dict()
                
                # Location lookup
                location_lookup = pd.read_sql(
                    "SELECT location_id, location_key FROM dim_location",
                    conn
                ).set_index('location_id')['location_key'].to_dict()
                
                # Payment type lookup
                payment_lookup = pd.read_sql(
                    "SELECT payment_type_id, payment_type_key FROM dim_payment_type",
                    conn
                ).set_index('payment_type_id')['payment_type_key'].to_dict()
                
                # Rate code lookup
                rate_code_lookup = pd.read_sql(
                    "SELECT rate_code_id, rate_code_key FROM dim_rate_code",
                    conn
                ).set_index('rate_code_id')['rate_code_key'].to_dict()
                
                # 3. Transform fact table data
                print("Transforming fact table data...")
                fact_df = pd.DataFrame({
                    'vendor_key': df['VendorID'].map(vendor_lookup).fillna(-1).astype(int),
                    'pickup_location_key': df['PULocationID'].map(location_lookup).fillna(-1).astype(int),
                    'dropoff_location_key': df['DOLocationID'].map(location_lookup).fillna(-1).astype(int),
                    'payment_type_key': df['payment_type'].map(payment_lookup).fillna(-1).astype(int),
                    'rate_code_key': df['RatecodeID'].map(rate_code_lookup).fillna(-1).astype(int),
                    'pickup_date_key': df['pickup_date_key'],
                    'passenger_count': df['passenger_count'].astype(int),
                    'trip_distance_miles': df['trip_distance'].round(2),
                    'trip_duration_minutes': df['trip_duration_minutes'],
                    'fare_amount': df['fare_amount'].round(2),
                    'extra_amount': df['extra'].fillna(0).round(2),
                    'mta_tax': df['mta_tax'].fillna(0).round(2),
                    'tip_amount': df['tip_amount'].fillna(0).round(2),
                    'tolls_amount': df['tolls_amount'].fillna(0).round(2),
                    'improvement_surcharge': df['improvement_surcharge'].fillna(0).round(2),
                    'total_amount': df['total_amount'].round(2),
                    'congestion_surcharge': df['congestion_surcharge'].fillna(0).round(2) if 'congestion_surcharge' in df.columns else 0,
                    'trip_type': df['trip_type'].fillna(0).astype(int) if 'trip_type' in df.columns else 0,
                    'source_file': f'green_tripdata_{year_month}.parquet'
                })
                
                # 4. Load fact table
                print(f"Loading {len(fact_df)} records into fact_trips...")
                fact_df.to_sql(
                    name='fact_trips',
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=5000,
                    method='multi'
                )
                
                # Commit transaction
                trans.commit()
                print(f"Successfully loaded {len(fact_df)} records into data warehouse")
                
                # Verify the load
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM fact_trips WHERE source_file = :source"
                ), {"source": f'green_tripdata_{year_month}.parquet'})
                count = result.scalar()
                print(f"Verification: fact_trips contains {count} records for {year_month}")
                
                # Push metrics to XCom
                context['ti'].xcom_push(key='dwh_record_count', value=len(fact_df))
                
            except Exception as e:
                trans.rollback()
                print(f"Transaction rolled back due to error: {e}")
                raise
        
        engine.dispose()
        return len(fact_df)
        
    except Exception as e:
        print(f"Error loading data to data warehouse: {e}")
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
    
    # Task 4: Load to Staging
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )
    
    # Task 5: Check/Create DWH Schema
    check_schema = PythonOperator(
        task_id='check_schema',
        python_callable=check_and_create_dwh_schema,
    )
    
    # Task 6: Load to Data Warehouse
    load_dwh = PythonOperator(
        task_id='load_dwh',
        python_callable=load_to_dwh,
    )
    
    # Task 7: End
    end = BashOperator(
        task_id='end',
        bash_command='echo "NYC Green Taxi pipeline completed for {{ ds }}!"',
    )
    
    # Define task dependencies
    start >> extract >> validate >> load >> check_schema >> load_dwh >> end
