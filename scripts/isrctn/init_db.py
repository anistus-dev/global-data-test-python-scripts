import os
import argparse
import psycopg2
from scripts.config import ISRCTN_DB_CONFIG

def init_db(schema_path, drop_first=False):
    print(f"Connecting to database: {ISRCTN_DB_CONFIG['database']} on {ISRCTN_DB_CONFIG['host']}...")
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}")
        return

    try:
        conn = psycopg2.connect(**ISRCTN_DB_CONFIG)
        cur = conn.cursor()
        
        if drop_first:
            print("Wiping existing schema (DROP SCHEMA public CASCADE)...")
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public;")
            cur.execute("GRANT ALL ON SCHEMA public TO public;")
            # Safely attempt to grant to current user if possible, 
            # though GRANT ALL ON SCHEMA public TO public usually covers it.
            conn.commit()
            print("Schema reset complete.")

        print(f"Reading schema from {schema_path}...")
        with open(schema_path, 'r') as f:
            sql = f.read()
        
        print("Applying schema to the database...")
        cur.execute(sql)
        
        conn.commit()
        print("Database initialized successfully!")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Initialize database with a SQL schema.")
    parser.add_argument(
        "--schema", 
        help="Path to the SQL schema file",
        default=os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'isrctn_schema.sql')
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate the 'public' schema before applying the SQL file (Destructive!)"
    )
    
    args = parser.parse_args()
    init_db(args.schema, drop_first=args.drop)

if __name__ == "__main__":
    main()
