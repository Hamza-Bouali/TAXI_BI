from sqlalchemy import create_engine, text
from 

stg_table_cred = {
    'USER': 'postgres.xgoybmnqftaiismchzhj',
    'PASSWORD': 'nyc_data',
    'HOST': 'aws-1-eu-north-1.pooler.supabase.com',
    'PORT': '6543',
    'DBNAME': 'postgres',
}
db_connection_string = f"postgresql+psycopg2://{stg_table_cred['USER']}:{stg_table_cred['PASSWORD']}@{stg_table_cred['HOST']}:{stg_table_cred['PORT']}/{stg_table_cred['DBNAME']}?sslmode=require&connect_timeout=30"

print(f"Connecting to {stg_table_cred['HOST']}...")
try:
    engine = create_engine(db_connection_string)
    with engine.connect() as conn:
        print("Connection successful!")
        result = conn.execute(text("SELECT 1"))
        print(f"Result: {result.scalar()}")
except Exception as e:
    print(f"Connection failed: {e}")
