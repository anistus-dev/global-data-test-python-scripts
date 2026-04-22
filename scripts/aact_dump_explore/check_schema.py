import psycopg2
from scripts.config import DB_CONFIG, SCHEMA

try:
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        # Check all_browse_conditions columns
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = 'all_browse_conditions'
            ORDER BY ordinal_position;
        """, (SCHEMA,))
        print("Columns in all_browse_conditions:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
