"""
Seed Script: CTGov (AACT) → Sync Log
Fetches all NCT IDs and initializes the sync_log table.
"""
import argparse
import psycopg2
from scripts.config import CTGOV_DB_CONFIG, UNIFIED_DB_CONFIG

def seed(batch_size=5000):
    print("=" * 60)
    print("Seeding Sync Log: CTGov")
    print("=" * 60)
    
    print("Connecting to databases...")
    src_conn = psycopg2.connect(**CTGOV_DB_CONFIG)
    dst_conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    print("Fetching NCT IDs from CTGov...")
    src_cur.execute("SELECT nct_id FROM ctgov.studies ORDER BY nct_id")
    ids = src_cur.fetchall()
    total = len(ids)
    print(f"Found {total:,} IDs.")

    print(f"Seeding ingest.sync_log (batch_size={batch_size})...")
    newly_added = 0
    
    for i in range(0, total, batch_size):
        batch = ids[i:i+batch_size]
        # values format: (source_code, source_record_id)
        values = [('CTGOV', row[0]) for row in batch]
        
        args_str = ','.join(dst_cur.mogrify("(%s, %s)", x).decode('utf-8') for x in values)
        dst_cur.execute(f"""
            INSERT INTO ingest.sync_log (source_code, source_record_id)
            VALUES {args_str}
            ON CONFLICT (source_code, source_record_id) DO NOTHING
        """)
        newly_added += dst_cur.rowcount
        dst_conn.commit()
        print(f"  Batch {i//batch_size + 1}: Attempted {len(batch)}... Total Attempted: {min(i + batch_size, total):,}/{total:,}")

    skipped = total - newly_added
    print("\n" + "=" * 60)
    print("Seeding Summary")
    print("-" * 60)
    print(f"Total IDs checked:  {total:,}")
    print(f"Newly added:        {newly_added:,}")
    print(f"Skipped (existed):  {skipped:,}")
    print("=" * 60)

    src_cur.close()
    src_conn.close()
    dst_cur.close()
    dst_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Seed the ingest.sync_log table with NCT IDs from CTGov (AACT).")
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of IDs to insert per batch (default: 5000)")
    args = parser.parse_args()
    seed(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
