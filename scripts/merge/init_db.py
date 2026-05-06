import os
import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from scripts.config import UNIFIED_DB_CONFIG

def create_database_if_not_exists():
    """
    Connects to the default 'postgres' database to create the target database if it doesn't exist.
    """
    target_db = UNIFIED_DB_CONFIG['database']
    print(f"Checking if database '{target_db}' exists...")
    
    # Use default params but connect to 'postgres'
    conn_params = UNIFIED_DB_CONFIG.copy()
    conn_params['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'")
        exists = cur.fetchone()
        
        if not exists:
            print(f"Database '{target_db}' does not exist. Creating...")
            cur.execute(f"CREATE DATABASE {target_db}")
            print(f"Database '{target_db}' created successfully.")
        else:
            print(f"Database '{target_db}' already exists.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Warning: Could not verify/create database '{target_db}': {e}")
        print("Continuing with connection attempt to target database...")

def init_db(schema_path, drop_first=False):
    create_database_if_not_exists()
    
    print(f"Connecting to database: {UNIFIED_DB_CONFIG['database']} on {UNIFIED_DB_CONFIG['host']}...")
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}")
        return

    try:
        conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
        cur = conn.cursor()
        
        if drop_first:
            schemas = ['ref', 'audit', 'clinical', 'drug', 'scientific', 'company', 'ingest']
            print(f"Wiping existing schemas ({', '.join(schemas)})...")
            for schema in schemas:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            
            # Also reset public if requested
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public;")
            cur.execute("GRANT ALL ON SCHEMA public TO public;")
            
            conn.commit()
            print("Cleanup complete.")

        print(f"Reading schema from {schema_path}...")
        with open(schema_path, 'r') as f:
            sql = f.read()
        
        print("Applying unified schema to the database...")
        cur.execute(sql)
        
        conn.commit()
        print("Unified Database initialized successfully!")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Initialize the Unified Clinical Database.")
    parser.add_argument(
        "--schema", 
        help="Path to the unified_schema.sql file",
        default=os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'unified_schema.sql')
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate all schemas (clinical, ref, ingest, etc.) before applying the SQL file (Destructive!)"
    )
    
    args = parser.parse_args()
    init_db(args.schema, drop_first=args.drop)

if __name__ == "__main__":
    main()
