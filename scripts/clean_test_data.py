#!/usr/bin/env python3
"""
Clean Test Data Script
Removes all staging tables and truncates the data warehouse fact table.
Use this before running production backfills.
"""

import sys
from sqlalchemy import create_engine, text

# Staging database credentials
STG_CRED = {
    'USER': 'postgres.wjkkavghitommkkbdzpm',
    'PASSWORD': 'admin',
    'HOST': 'aws-1-eu-central-2.pooler.supabase.com',
    'PORT': '6543',
    'DBNAME': 'postgres',
}

# Data Warehouse credentials
DWH_CRED = {
    'USER': 'postgres.xgoybmnqftaiismchzhj',
    'PASSWORD': 'nyc_data',
    'HOST': 'aws-1-eu-north-1.pooler.supabase.com',
    'PORT': '6543',
    'DBNAME': 'postgres',
}


def clean_staging_database():
    """Clean all green_taxi tables from staging database"""
    print("\n" + "=" * 60)
    print("CLEANING STAGING DATABASE")
    print("=" * 60)
    
    conn_string = (
        f"postgresql+psycopg2://{STG_CRED['USER']}:{STG_CRED['PASSWORD']}"
        f"@{STG_CRED['HOST']}:{STG_CRED['PORT']}/{STG_CRED['DBNAME']}"
        f"?sslmode=require&connect_timeout=30"
    )
    
    try:
        engine = create_engine(conn_string, pool_pre_ping=True)
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # Get all green_taxi tables
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'green_taxi_%'
                    ORDER BY table_name
                """))
                tables = [row[0] for row in result]
                
                if not tables:
                    print("✓ No staging tables found - already clean")
                    trans.commit()
                    return True
                
                print(f"Found {len(tables)} staging tables:")
                for table in tables:
                    print(f"  • {table}")
                
                print("\nDropping tables...")
                for table in tables:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    print(f"  ✓ Dropped: {table}")
                
                trans.commit()
                print(f"\n✅ Successfully dropped {len(tables)} staging tables")
                return True
                
            except Exception as e:
                trans.rollback()
                print(f"\n❌ Error during transaction: {e}")
                raise
                
        engine.dispose()
        
    except Exception as e:
        print(f"\n❌ Failed to clean staging database: {e}")
        return False


def clean_data_warehouse():
    """Truncate fact_trips table in data warehouse"""
    print("\n" + "=" * 60)
    print("CLEANING DATA WAREHOUSE")
    print("=" * 60)
    
    conn_string = (
        f"postgresql+psycopg2://{DWH_CRED['USER']}:{DWH_CRED['PASSWORD']}"
        f"@{DWH_CRED['HOST']}:{DWH_CRED['PORT']}/{DWH_CRED['DBNAME']}"
        f"?sslmode=require&connect_timeout=30"
    )
    
    try:
        engine = create_engine(conn_string, pool_pre_ping=True)
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # Check fact_trips count before
                result = conn.execute(text("SELECT COUNT(*) FROM fact_trips"))
                count_before = result.scalar()
                print(f"Records before cleanup: {count_before:,}")
                
                if count_before == 0:
                    print("✓ Data warehouse already clean")
                    trans.commit()
                    return True
                
                # Get date range
                result = conn.execute(text("""
                    SELECT 
                        MIN(pickup_date_key) as min_date,
                        MAX(pickup_date_key) as max_date
                    FROM fact_trips
                """))
                min_date, max_date = result.fetchone()
                print(f"Date range: {min_date} → {max_date}")
                
                # Truncate fact_trips
                print("\nTruncating fact_trips table...")
                conn.execute(text("TRUNCATE TABLE fact_trips"))
                
                trans.commit()
                
                # Verify
                result = conn.execute(text("SELECT COUNT(*) FROM fact_trips"))
                count_after = result.scalar()
                print(f"Records after cleanup: {count_after:,}")
                
                print(f"\n✅ Successfully removed {count_before:,} records from data warehouse")
                return True
                
            except Exception as e:
                trans.rollback()
                print(f"\n❌ Error during transaction: {e}")
                raise
                
        engine.dispose()
        
    except Exception as e:
        print(f"\n❌ Failed to clean data warehouse: {e}")
        return False


def main():
    """Main execution function"""
    print("\n" + "=" * 60)
    print("NYC GREEN TAXI - TEST DATA CLEANUP SCRIPT")
    print("=" * 60)
    print("\nThis script will:")
    print("  1. Drop all green_taxi_* tables from staging database")
    print("  2. Truncate fact_trips table in data warehouse")
    print("\n⚠️  WARNING: This action cannot be undone!")
    
    # Check if running in interactive mode
    if sys.stdin.isatty():
        response = input("\nDo you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("\n❌ Cleanup cancelled by user")
            return 1
    else:
        print("\n⚠️  Running in non-interactive mode - proceeding with cleanup...")
    
    # Execute cleanup
    staging_success = clean_staging_database()
    dwh_success = clean_data_warehouse()
    
    # Summary
    print("\n" + "=" * 60)
    print("CLEANUP SUMMARY")
    print("=" * 60)
    print(f"Staging Database: {'✅ SUCCESS' if staging_success else '❌ FAILED'}")
    print(f"Data Warehouse:   {'✅ SUCCESS' if dwh_success else '❌ FAILED'}")
    print("=" * 60)
    
    if staging_success and dwh_success:
        print("\n🎉 All test data cleaned successfully!")
        print("\nYou can now run a fresh backfill:")
        print("  docker compose exec airflow-scheduler airflow backfill create \\")
        print("    --dag-id nyc_green_taxi_pipeline \\")
        print("    --from-date 2024-01-01 \\")
        print("    --to-date 2025-10-31 \\")
        print("    --max-active-runs 3")
        return 0
    else:
        print("\n⚠️  Cleanup completed with errors")
        return 1


if __name__ == "__main__":
    sys.exit(main())
