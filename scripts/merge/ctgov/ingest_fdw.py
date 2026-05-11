"""
CTGov (AACT) -> Unified Clinical Database Ingestion Script (FDW Version)
Utilizes PostgreSQL Foreign Data Wrappers and PL/pgSQL Stored Procedures 
to achieve absolute maximum bulk-ingestion throughput.
"""
import os
import sys
import time
import argparse
import psycopg2
from scripts.config import CTGOV_DB_CONFIG, UNIFIED_DB_CONFIG

def get_or_create_source(cur):
    cur.execute("SELECT source_id FROM ref.source WHERE source_code = 'CTGOV'")
    row = cur.fetchone()
    if row:
        return row[0]
    raise RuntimeError("CTGOV source not found in ref.source. Run init_db first.")

def deploy_sql_scripts(conn):
    """Formats and executes the setup SQL scripts directly on the Unified DB"""
    cur = conn.cursor()
    base_dir = os.path.dirname(__file__)
    
    # 1. Setup FDW
    setup_path = os.path.join(base_dir, 'sql', 'setup_fdw.sql')
    with open(setup_path, 'r') as f:
        setup_sql = f.read()
    
    formatted_setup = setup_sql.format(
        host=CTGOV_DB_CONFIG.get('host', 'localhost'),
        dbname=CTGOV_DB_CONFIG.get('database', 'ctgov_db'),
        port=CTGOV_DB_CONFIG.get('port', 5432),
        user=CTGOV_DB_CONFIG.get('user', 'postgres'),
        password=CTGOV_DB_CONFIG.get('password', '')
    )
    
    print("Deploying FDW infrastructure...")
    cur.execute(formatted_setup)
    
    # 2. Deploy Procedure
    proc_path = os.path.join(base_dir, 'sql', 'migrate_procedure.sql')
    with open(proc_path, 'r') as f:
        proc_sql = f.read()
        
    print("Deploying PL/pgSQL migration procedure...")
    cur.execute(proc_sql)
    
    conn.commit()
    cur.close()

def run_ingestion_loop(limit, batch_size, retry=False):
    conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    cur = conn.cursor()
    
    try:
        source_id = get_or_create_source(cur)
        # deploy_sql_scripts(conn)
        
        total_processed = 0
        total_success = 0
        start_time = time.time()
        
        status_filter = ('error',) if retry else ('pending',)
        
        print(f"Starting FDW Ingestion... (Batch Size: {batch_size})")
        while limit is None or total_processed < limit:
            fetch_size = batch_size
            if limit is not None:
                fetch_size = min(batch_size, limit - total_processed)
                
            # Grab a batch from sync_log
            cur.execute("""
                WITH batch AS (
                    SELECT sync_id, source_record_id 
                    FROM ingest.sync_log
                    WHERE source_code = 'CTGOV' AND status IN %s
                    ORDER BY sync_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ingest.sync_log s
                SET status = 'processing', updated_at = now()
                FROM batch b
                WHERE s.sync_id = b.sync_id
                RETURNING b.sync_id, b.source_record_id
            """, (status_filter, fetch_size))
            
            batch = cur.fetchall()
            if not batch:
                print("No more pending records found in sync_log.")
                break
                
            nct_ids = [row[1] for row in batch]
            sync_ids = [row[0] for row in batch]
            total_processed += len(batch)
            
            # Execute Stored Procedure
            try:
                cur.execute("CALL ingest.migrate_ctgov_batch(%s, %s)", (nct_ids, source_id))
                
                # Mark Success
                cur.execute("""
                    UPDATE ingest.sync_log
                    SET status = 'completed', updated_at = now()
                    WHERE sync_id = ANY(%s::uuid[])
                """, (sync_ids,))
                
                conn.commit()
                total_success += len(batch)
                
                elapsed = time.time() - start_time
                rate = total_success / elapsed if elapsed > 0 else 0
                print(f"Processed batch of {len(batch)}. Total Success: {total_success}. Speed: {rate:.1f} trials/sec")
                
            except Exception as e:
                conn.rollback()
                print(f"\n[ERROR] Batch Failed: {e}")
                
                # Mark as error
                cur.execute("""
                    UPDATE ingest.sync_log
                    SET status = 'error', error_message = %s, updated_at = now()
                    WHERE sync_id = ANY(%s::uuid[])
                """, (str(e)[:500], sync_ids))
                conn.commit()
                
        print(f"\nMigration complete. Successfully processed {total_success} trials.")
        
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CTGov FDW Database Ingestion")
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for FDW operations')
    parser.add_argument('--limit', type=int, default=None, help='Max records to process')
    parser.add_argument('--retry', action='store_true', help='Retry previously failed sync_log entries')
    args = parser.parse_args()

    run_ingestion_loop(limit=args.limit, batch_size=args.batch_size, retry=args.retry)
