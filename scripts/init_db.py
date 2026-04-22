import os
import psycopg2
from scripts.config import ISRCTN_DB_CONFIG

def init_db():
    print(f"Connecting to database: {ISRCTN_DB_CONFIG['database']} on {ISRCTN_DB_CONFIG['host']}...")
    
    schema_file = os.path.join(os.path.dirname(__file__), '..', 'database', 'isrctn_schema.sql')
    
    if not os.path.exists(schema_file):
        print(f"Error: Schema file not found at {schema_file}")
        return

    try:
        conn = psycopg2.connect(**ISRCTN_DB_CONFIG)
        cur = conn.cursor()
        
        print(f"Reading schema from {schema_file}...")
        with open(schema_file, 'r') as f:
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

if __name__ == "__main__":
    init_db()
