import sys
import argparse
import csv
import psycopg2
from scripts.config import ISRCTN_DB_CONFIG

def get_db_connection():
    try:
        conn = psycopg2.connect(**ISRCTN_DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def populate_queue(csv_path):
    print(f"Reading CSV from: {csv_path}")
    
    ids_to_insert = []
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Find the ISRCTN column (case-insensitive)
            headers = reader.fieldnames
            isrctn_col = next((h for h in headers if h.upper() == 'ISRCTN'), None)
            
            if not isrctn_col:
                print(f"Error: Could not find 'ISRCTN' column in {csv_path}")
                print(f"Available columns: {headers}")
                return

            for row in reader:
                val = row[isrctn_col]
                if val and val.strip():
                    ids_to_insert.append(val.strip())
                    
    except FileNotFoundError:
        print(f"Error: File not found at {csv_path}")
        return
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if not ids_to_insert:
        print("No IDs found to process.")
        return

    print(f"Found {len(ids_to_insert)} IDs. Inserting into trial_queue...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    processed_count = 0
    duplicate_count = 0
    
    try:
        for isrctn_id in ids_to_insert:
            try:
                # Use ON CONFLICT DO NOTHING to handle duplicates gracefully
                cur.execute(
                    "INSERT INTO trial_queue (isrctn_id) VALUES (%s) ON CONFLICT (isrctn_id) DO NOTHING",
                    (isrctn_id,)
                )
                if cur.rowcount == 1:
                    processed_count += 1
                else:
                    duplicate_count += 1
            except Exception as e:
                conn.rollback()
                print(f"Error inserting {isrctn_id}: {e}")
        
        conn.commit()
        print(f"Success! {processed_count} new IDs added, {duplicate_count} duplicates skipped.")
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate ISRCTN trial queue from a CSV file.")
    parser.add_argument("csv_path", help="Path to the CSV file containing an 'ISRCTN' column")
    
    args = parser.parse_args()
    populate_queue(args.csv_path)
