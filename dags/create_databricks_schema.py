"""
Databricks Schema Creation Pipeline
Creates dimension and fact tables based on NYC Green Taxi data dictionary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.sdk import Variable
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_dimension_tables(**context):
    """Create all dimension tables in Databricks"""
    
    print("Creating dimension tables in Databricks...")
    
    # Get Databricks credentials
    dwh_cred = Variable.get("dwh_db_creds", deserialize_json=True)
    
    required_keys = ['ACCESS_TOKEN', 'SERVER_HOSTNAME', 'HTTP_PATH']
    missing_keys = [key for key in required_keys if key not in dwh_cred]
    if missing_keys:
        raise ValueError(f"Missing required Databricks credentials: {', '.join(missing_keys)}")
    
    dwh_connection_string = (
        f"databricks://token:{dwh_cred['ACCESS_TOKEN']}@"
        f"{dwh_cred['SERVER_HOSTNAME']}?http_path={dwh_cred['HTTP_PATH']}&"
        f"catalog={dwh_cred.get('CATALOG', 'hive_metastore')}&"
        f"schema={dwh_cred.get('SCHEMA', 'default')}"
    )
    
    try:
        engine = create_engine(dwh_connection_string)
        with engine.connect() as conn:
            
            # Create schema if not exists
            schema_name = dwh_cred.get('SCHEMA', 'default')
            print(f"Creating schema '{schema_name}'...")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            
            # 1. Create dim_vendor table
            print("Creating dim_vendor table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_vendor (
                    vendor_id INT NOT NULL,
                    vendor_name STRING,
                    description STRING,
                    is_active BOOLEAN,
                    created_date TIMESTAMP,
                    PRIMARY KEY (vendor_id)
                ) COMMENT 'Vendor dimension - TPEP providers'
            """))
            
            # Populate dim_vendor
            result = conn.execute(text("SELECT COUNT(*) FROM dim_vendor"))
            if result.scalar() == 0:
                print("Populating dim_vendor...")
                conn.execute(text("""
                    INSERT INTO dim_vendor (vendor_id, vendor_name, description, is_active, created_date) VALUES
                    (1, 'Creative Mobile Technologies, LLC', 'CMT - Creative Mobile Technologies', TRUE, CURRENT_TIMESTAMP()),
                    (2, 'VeriFone Inc.', 'VeriFone taxi technology provider', TRUE, CURRENT_TIMESTAMP())
                """))
                print("✓ Vendor dimension populated")
            
            # 2. Create dim_rate_code table
            print("Creating dim_rate_code table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_rate_code (
                    rate_code_id INT NOT NULL,
                    rate_code_name STRING,
                    description STRING,
                    is_active BOOLEAN,
                    PRIMARY KEY (rate_code_id)
                ) COMMENT 'Rate code dimension - Final rate code at end of trip'
            """))
            
            # Populate dim_rate_code
            result = conn.execute(text("SELECT COUNT(*) FROM dim_rate_code"))
            if result.scalar() == 0:
                print("Populating dim_rate_code...")
                conn.execute(text("""
                    INSERT INTO dim_rate_code (rate_code_id, rate_code_name, description, is_active) VALUES
                    (1, 'Standard rate', 'Standard metered rate', TRUE),
                    (2, 'JFK', 'JFK Airport flat fare', TRUE),
                    (3, 'Newark', 'Newark Airport flat fare', TRUE),
                    (4, 'Nassau or Westchester', 'Nassau or Westchester County rate', TRUE),
                    (5, 'Negotiated fare', 'Negotiated fare', TRUE),
                    (6, 'Group ride', 'Group ride', TRUE),
                    (99, 'Unknown', 'Unknown or missing rate code', FALSE)
                """))
                print("✓ Rate code dimension populated")
            
            # 3. Create dim_payment_type table
            print("Creating dim_payment_type table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_payment_type (
                    payment_type_id INT NOT NULL,
                    payment_type_name STRING,
                    description STRING,
                    is_electronic BOOLEAN,
                    is_active BOOLEAN,
                    PRIMARY KEY (payment_type_id)
                ) COMMENT 'Payment type dimension - How passenger paid'
            """))
            
            # Populate dim_payment_type
            result = conn.execute(text("SELECT COUNT(*) FROM dim_payment_type"))
            if result.scalar() == 0:
                print("Populating dim_payment_type...")
                conn.execute(text("""
                    INSERT INTO dim_payment_type (payment_type_id, payment_type_name, description, is_electronic, is_active) VALUES
                    (1, 'Credit card', 'Credit card payment', TRUE, TRUE),
                    (2, 'Cash', 'Cash payment', FALSE, TRUE),
                    (3, 'No charge', 'No charge trip', FALSE, TRUE),
                    (4, 'Dispute', 'Disputed trip', FALSE, TRUE),
                    (5, 'Unknown', 'Unknown payment type', FALSE, FALSE),
                    (6, 'Voided trip', 'Voided trip', FALSE, FALSE)
                """))
                print("✓ Payment type dimension populated")
            
            # 4. Create dim_trip_type table
            print("Creating dim_trip_type table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_trip_type (
                    trip_type_id INT NOT NULL,
                    trip_type_name STRING,
                    description STRING,
                    is_active BOOLEAN,
                    PRIMARY KEY (trip_type_id)
                ) COMMENT 'Trip type dimension - Street-hail or dispatch'
            """))
            
            # Populate dim_trip_type
            result = conn.execute(text("SELECT COUNT(*) FROM dim_trip_type"))
            if result.scalar() == 0:
                print("Populating dim_trip_type...")
                conn.execute(text("""
                    INSERT INTO dim_trip_type (trip_type_id, trip_type_name, description, is_active) VALUES
                    (1, 'Street-hail', 'Street-hail trip', TRUE),
                    (2, 'Dispatch', 'Dispatch trip', TRUE),
                    (99, 'Unknown', 'Unknown trip type', FALSE)
                """))
                print("✓ Trip type dimension populated")
            
            # 5. Create dim_location table
            print("Creating dim_location table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_location (
                    location_id INT NOT NULL,
                    borough STRING,
                    zone STRING,
                    service_zone STRING,
                    is_airport BOOLEAN,
                    is_boro_zone BOOLEAN,
                    is_yellow_zone BOOLEAN,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    PRIMARY KEY (location_id)
                ) COMMENT 'Location dimension - NYC Taxi Zones'
            """))
            
            print("✓ All dimension tables created")
            
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"Error creating dimension tables: {e}")
        raise


def load_location_dimension(**context):
    """Load location data from taxi zone lookup file"""
    
    print("Loading location dimension from zone lookup...")
    
    # Read the zone lookup CSV (use container path)
    zone_lookup_path = '/opt/airflow/taxi_zone_lookup.csv'
    
    try:
        # Read zone lookup data
        df = pd.read_csv(zone_lookup_path)
        
        print(f"Loaded {len(df)} location records")
        
        # Get Databricks credentials
        dwh_cred = Variable.get("dwh_db_creds", deserialize_json=True)
        
        dwh_connection_string = (
            f"databricks://token:{dwh_cred['ACCESS_TOKEN']}@"
            f"{dwh_cred['SERVER_HOSTNAME']}?http_path={dwh_cred['HTTP_PATH']}&"
            f"catalog={dwh_cred.get('CATALOG', 'hive_metastore')}&"
            f"schema={dwh_cred.get('SCHEMA', 'default')}"
        )
        
        engine = create_engine(dwh_connection_string)
        
        with engine.connect() as conn:
            # Check if already populated
            result = conn.execute(text("SELECT COUNT(*) FROM dim_location"))
            existing_count = result.scalar()
            
            if existing_count > 0:
                print(f"dim_location already has {existing_count} records. Skipping load.")
                return existing_count
            
            # Prepare data for insertion
            df['is_airport'] = df['service_zone'] == 'Airports'
            df['is_boro_zone'] = df['service_zone'] == 'Boro Zone'
            df['is_yellow_zone'] = df['service_zone'] == 'Yellow Zone'
            
            # Rename columns to match table schema
            df = df.rename(columns={
                'LocationID': 'location_id',
                'Borough': 'borough',
                'Zone': 'zone',
                'service_zone': 'service_zone'
            })
            
            # Select only needed columns
            df = df[['location_id', 'borough', 'zone', 'service_zone', 
                     'is_airport', 'is_boro_zone', 'is_yellow_zone']]
            
            # Insert data in batches
            print("Inserting location data...")
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                values = []
                
                for _, row in batch.iterrows():
                    values.append(
                        f"({row['location_id']}, '{row['borough']}', "
                        f"'{row['zone'].replace(chr(39), chr(39)+chr(39))}', "
                        f"'{row['service_zone']}', {row['is_airport']}, "
                        f"{row['is_boro_zone']}, {row['is_yellow_zone']}, NULL, NULL)"
                    )
                
                insert_sql = (
                    "INSERT INTO dim_location "
                    "(location_id, borough, zone, service_zone, is_airport, "
                    "is_boro_zone, is_yellow_zone, latitude, longitude) "
                    f"VALUES {', '.join(values)}"
                )
                
                conn.execute(text(insert_sql))
            
            print(f"✓ Loaded {len(df)} location records")
        
        engine.dispose()
        return len(df)
        
    except Exception as e:
        print(f"Error loading location dimension: {e}")
        raise


def create_date_time_dimensions(**context):
    """Create and populate date and time dimension tables"""
    
    print("Creating date and time dimensions...")
    
    dwh_cred = Variable.get("dwh_db_creds", deserialize_json=True)
    
    dwh_connection_string = (
        f"databricks://token:{dwh_cred['ACCESS_TOKEN']}@"
        f"{dwh_cred['SERVER_HOSTNAME']}?http_path={dwh_cred['HTTP_PATH']}&"
        f"catalog={dwh_cred.get('CATALOG', 'hive_metastore')}&"
        f"schema={dwh_cred.get('SCHEMA', 'default')}"
    )
    
    try:
        engine = create_engine(dwh_connection_string)
        
        with engine.connect() as conn:
            
            # Create dim_date table
            print("Creating dim_date table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INT NOT NULL,
                    full_date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    month_name STRING,
                    day INT,
                    day_of_week INT,
                    day_name STRING,
                    week_of_year INT,
                    is_weekend BOOLEAN,
                    is_holiday BOOLEAN,
                    season STRING,
                    PRIMARY KEY (date_key)
                ) COMMENT 'Date dimension for date-based analysis'
            """))
            
            # Check if dim_date needs population
            result = conn.execute(text("SELECT COUNT(*) FROM dim_date"))
            if result.scalar() == 0:
                print("Populating dim_date (2018-2030)...")
                
                date_range = pd.date_range(start='2018-01-01', end='2030-12-31', freq='D')
                
                day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                              'July', 'August', 'September', 'October', 'November', 'December']
                
                date_records = []
                for date in date_range:
                    date_key = int(date.strftime('%Y%m%d'))
                    year = date.year
                    quarter = (date.month - 1) // 3 + 1
                    month = date.month
                    day = date.day
                    day_of_week = date.weekday()
                    day_name = day_names[day_of_week]
                    month_name = month_names[month - 1]
                    week_of_year = date.isocalendar()[1]
                    is_weekend = day_of_week >= 5
                    
                    if month in [12, 1, 2]:
                        season = 'Winter'
                    elif month in [3, 4, 5]:
                        season = 'Spring'
                    elif month in [6, 7, 8]:
                        season = 'Summer'
                    else:
                        season = 'Fall'
                    
                    date_records.append(
                        f"({date_key}, '{date.strftime('%Y-%m-%d')}', {year}, {quarter}, {month}, "
                        f"'{month_name}', {day}, {day_of_week}, '{day_name}', {week_of_year}, "
                        f"{is_weekend}, FALSE, '{season}')"
                    )
                
                # Insert in batches
                batch_size = 1000
                for i in range(0, len(date_records), batch_size):
                    batch = date_records[i:i+batch_size]
                    insert_sql = (
                        "INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, "
                        "day, day_of_week, day_name, week_of_year, is_weekend, is_holiday, season) "
                        f"VALUES {', '.join(batch)}"
                    )
                    conn.execute(text(insert_sql))
                
                print(f"✓ Inserted {len(date_records)} date records")
            
            # Create dim_time table
            print("Creating dim_time table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_time (
                    time_key INT NOT NULL,
                    hour INT,
                    minute INT,
                    time_of_day STRING,
                    is_business_hours BOOLEAN,
                    is_rush_hour BOOLEAN,
                    hour_12 INT,
                    am_pm STRING,
                    PRIMARY KEY (time_key)
                ) COMMENT 'Time dimension for time-based analysis'
            """))
            
            # Check if dim_time needs population
            result = conn.execute(text("SELECT COUNT(*) FROM dim_time"))
            if result.scalar() == 0:
                print("Populating dim_time...")
                
                time_records = []
                for hour in range(24):
                    for minute in [0, 15, 30, 45]:
                        time_key = hour * 100 + minute
                        
                        if 5 <= hour < 12:
                            time_of_day = 'Morning'
                        elif 12 <= hour < 17:
                            time_of_day = 'Afternoon'
                        elif 17 <= hour < 21:
                            time_of_day = 'Evening'
                        else:
                            time_of_day = 'Night'
                        
                        is_business_hours = 9 <= hour < 17
                        is_rush_hour = (7 <= hour < 10) or (16 <= hour < 19)
                        
                        hour_12 = hour if hour <= 12 else hour - 12
                        if hour_12 == 0:
                            hour_12 = 12
                        am_pm = 'AM' if hour < 12 else 'PM'
                        
                        time_records.append(
                            f"({time_key}, {hour}, {minute}, '{time_of_day}', "
                            f"{is_business_hours}, {is_rush_hour}, {hour_12}, '{am_pm}')"
                        )
                
                # Insert in batches
                batch_size = 100
                for i in range(0, len(time_records), batch_size):
                    batch = time_records[i:i+batch_size]
                    insert_sql = (
                        "INSERT INTO dim_time (time_key, hour, minute, time_of_day, "
                        "is_business_hours, is_rush_hour, hour_12, am_pm) "
                        f"VALUES {', '.join(batch)}"
                    )
                    conn.execute(text(insert_sql))
                
                print(f"✓ Inserted {len(time_records)} time records")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"Error creating date/time dimensions: {e}")
        raise


def create_fact_table(**context):
    """Create the fact table for trip records"""
    
    print("Creating fact_trips table...")
    
    dwh_cred = Variable.get("dwh_db_creds", deserialize_json=True)
    
    dwh_connection_string = (
        f"databricks://token:{dwh_cred['ACCESS_TOKEN']}@"
        f"{dwh_cred['SERVER_HOSTNAME']}?http_path={dwh_cred['HTTP_PATH']}&"
        f"catalog={dwh_cred.get('CATALOG', 'hive_metastore')}&"
        f"schema={dwh_cred.get('SCHEMA', 'default')}"
    )
    
    try:
        engine = create_engine(dwh_connection_string)
        
        with engine.connect() as conn:
            print("Creating fact_trips table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_trips (
                    trip_id STRING NOT NULL,
                    vendor_id INT,
                    pickup_datetime TIMESTAMP,
                    dropoff_datetime TIMESTAMP,
                    pickup_location_id INT,
                    dropoff_location_id INT,
                    pickup_date_key INT,
                    pickup_time_key INT,
                    dropoff_date_key INT,
                    dropoff_time_key INT,
                    passenger_count INT,
                    trip_distance DOUBLE COMMENT 'Trip distance in miles',
                    rate_code_id INT,
                    store_and_fwd_flag STRING COMMENT 'Y=store and forward; N=not store and forward',
                    payment_type_id INT,
                    fare_amount DOUBLE COMMENT 'Time-and-distance fare',
                    extra DOUBLE COMMENT 'Miscellaneous extras and surcharges',
                    mta_tax DOUBLE COMMENT 'MTA tax automatically triggered',
                    tip_amount DOUBLE COMMENT 'Tip amount (credit cards only)',
                    tolls_amount DOUBLE COMMENT 'Total tolls paid',
                    ehail_fee DOUBLE COMMENT 'Ehail fee',
                    improvement_surcharge DOUBLE COMMENT 'Improvement surcharge',
                    total_amount DOUBLE COMMENT 'Total amount charged',
                    congestion_surcharge DOUBLE COMMENT 'Congestion surcharge',
                    trip_type_id INT,
                    trip_duration_minutes INT,
                    PRIMARY KEY (trip_id)
                ) COMMENT 'Fact table for NYC Green Taxi trips'
                PARTITIONED BY (pickup_date_key)
            """))
            
            print("✓ fact_trips table created")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"Error creating fact table: {e}")
        raise


# Define the DAG
with DAG(
    'databricks_schema_creation',
    default_args=default_args,
    description='Create Databricks schema for NYC Green Taxi data warehouse',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['databricks', 'schema', 'setup'],
) as dag:
    
    create_dims = PythonOperator(
        task_id='create_dimension_tables',
        python_callable=create_dimension_tables,
    )
    
    load_locations = PythonOperator(
        task_id='load_location_dimension',
        python_callable=load_location_dimension,
    )
    
    create_date_time = PythonOperator(
        task_id='create_date_time_dimensions',
        python_callable=create_date_time_dimensions,
    )
    
    create_fact = PythonOperator(
        task_id='create_fact_table',
        python_callable=create_fact_table,
    )
    
    # Define task dependencies
    create_dims >> [load_locations, create_date_time] >> create_fact
