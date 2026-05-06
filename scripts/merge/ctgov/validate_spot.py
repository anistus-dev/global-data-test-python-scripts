import os
import sys
import json
import argparse
import datetime
from decimal import Decimal
from uuid import UUID

import psycopg2
from psycopg2.extras import RealDictCursor

# --- Config ---
from scripts.config import CTGOV_DB_CONFIG, UNIFIED_DB_CONFIG

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle UUIDs, Decimals, and Dates returned by psycopg2."""
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super().default(obj)

def fetch_aact_data(cur, nct_id):
    """Fetch raw source data from AACT tables."""
    tables = [
        'studies', 'sponsors', 'conditions', 'facilities', 'interventions', 
        'design_groups', 'design_group_interventions', 'design_outcomes',
        'result_groups', 'result_outcomes', 'outcome_measurements', 
        'outcome_analyses', 'outcome_analysis_groups', 'reported_events'
    ]
    data = {}
    for table in tables:
        try:
            cur.execute(f"SELECT * FROM ctgov.{table} WHERE nct_id = %s", (nct_id,))
            data[table] = cur.fetchall()
        except psycopg2.Error as e:
            data[table] = []
            cur.connection.rollback() # recover if table is missing
    return data

def fetch_unified_data(cur, nct_id):
    """Fetch structured data from the Unified Database."""
    cur.execute("SELECT trial_id FROM clinical.trial WHERE primary_registry_id = %s", (nct_id,))
    row = cur.fetchone()
    if not row:
        return None
    trial_id = row['trial_id']
    
    queries = {
        'trial': ("SELECT * FROM clinical.trial WHERE trial_id = %s", [trial_id]),
        'sponsors': ("SELECT * FROM clinical.trial_sponsor WHERE trial_id = %s", [trial_id]),
        'conditions': ("SELECT * FROM clinical.trial_indication WHERE trial_id = %s", [trial_id]),
        'facilities': ("SELECT * FROM clinical.trial_site WHERE trial_id = %s", [trial_id]),
        'interventions': ("SELECT * FROM clinical.intervention WHERE trial_id = %s", [trial_id]),
        'arms': ("SELECT * FROM clinical.arm WHERE trial_id = %s", [trial_id]),
        'arm_interventions': ("""
            SELECT ai.* FROM clinical.arm_intervention ai 
            JOIN clinical.arm a ON a.arm_id = ai.arm_id WHERE a.trial_id = %s
        """, [trial_id]),
        'outcomes': ("SELECT * FROM clinical.outcome WHERE trial_id = %s", [trial_id]),
        'result_groups': ("SELECT * FROM clinical.result_group WHERE trial_id = %s", [trial_id]),
        'outcome_measurements': ("""
            SELECT om.* FROM clinical.outcome_measurement om 
            JOIN clinical.outcome o ON o.outcome_id = om.outcome_id WHERE o.trial_id = %s
        """, [trial_id]),
        'outcome_analyses': ("""
            SELECT oa.* FROM clinical.outcome_analysis oa 
            JOIN clinical.outcome o ON o.outcome_id = oa.outcome_id WHERE o.trial_id = %s
        """, [trial_id]),
        'outcome_analysis_groups': ("""
            SELECT oag.* FROM clinical.outcome_analysis_group oag 
            JOIN clinical.outcome_analysis oa ON oa.analysis_id = oag.analysis_id
            JOIN clinical.outcome o ON o.outcome_id = oa.outcome_id WHERE o.trial_id = %s
        """, [trial_id]),
        'adverse_events': ("SELECT * FROM clinical.adverse_event WHERE trial_id = %s", [trial_id]),
    }
    
    data = {}
    for key, (sql, params) in queries.items():
        cur.execute(sql, params)
        data[key] = cur.fetchall()
    return data

def main():
    parser = argparse.ArgumentParser(
        description="Spot Validation Tool: Extracts and compares data for a specific NCT ID from both the AACT source DB and the Unified Clinical DB.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("nct_id", help="The NCT ID to validate (e.g., NCT02147626). Must match an existing study in the source DB.")
    parser.add_argument("-o", "--output", default=None, help="Output file path. Defaults to 'validation_report_<nct_id>.txt' in the current directory.")
    args = parser.parse_args()
    
    default_filename = f"validation_report_{args.nct_id}.txt"
    if args.output:
        if os.path.isdir(args.output) or args.output.endswith(('/', '\\')):
            output_path = os.path.join(args.output, default_filename)
        else:
            output_path = args.output
    else:
        output_path = default_filename
    
    print(f"Connecting to databases...")
    try:
        src_conn = psycopg2.connect(**CTGOV_DB_CONFIG)
        dst_conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
        
        src_cur = src_conn.cursor(cursor_factory=RealDictCursor)
        dst_cur = dst_conn.cursor(cursor_factory=RealDictCursor)
        
        print(f"Fetching data for {args.nct_id}...")
        
        aact_data = fetch_aact_data(src_cur, args.nct_id)
        unified_data = fetch_unified_data(dst_cur, args.nct_id)
        
        if not unified_data:
            print(f"Error: Trial {args.nct_id} not found in the Unified database! Has it been ingested?")
            return
            
        print(f"Generating report: {output_path}")
        with open(output_path, 'w') as f:
            f.write(f"=== Validation Report for {args.nct_id} ===\n\n")
            
            f.write("--- 1. RECORD COUNTS COMPARISON ---\n")
            f.write("Note: Counts may differ slightly due to intended deduplication (e.g., matching outcomes).\n\n")
            f.write(f"{'Source (AACT)':<45} | {'Unified (Clinical)':<45}\n")
            f.write("-" * 93 + "\n")
            
            comparisons = [
                ('studies', 'trial'),
                ('sponsors', 'sponsors'),
                ('conditions', 'conditions'),
                ('facilities', 'facilities'),
                ('interventions', 'interventions'),
                ('design_groups', 'arms'),
                ('design_group_interventions', 'arm_interventions'),
                ('result_groups', 'result_groups'),
                ('outcome_measurements', 'outcome_measurements'),
                ('outcome_analyses', 'outcome_analyses'),
                ('outcome_analysis_groups', 'outcome_analysis_groups'),
                ('reported_events', 'adverse_events')
            ]
            
            for aact_key, uni_key in comparisons:
                a_count = len(aact_data.get(aact_key, []))
                u_count = len(unified_data.get(uni_key, []))
                f.write(f"{aact_key} ({a_count})".ljust(45) + f" | {uni_key} ({u_count})\n")
            
            # Combine outcomes for accurate counting
            total_aact_outcomes = len(aact_data.get('design_outcomes', [])) + len(aact_data.get('result_outcomes', []))
            uni_outcomes = len(unified_data.get('outcomes', []))
            f.write(f"{'outcomes (design+result)'} ({total_aact_outcomes})".ljust(45) + f" | outcomes ({uni_outcomes})\n")
            
            f.write("\n\n--- 2. RAW UNIFIED DATA DUMP (JSON) ---\n")
            f.write("This section contains the exact rows extracted from the unified clinical database.\n")
            for table, rows in unified_data.items():
                f.write(f"\n>> ENTITY: {table}\n")
                f.write(json.dumps(rows, indent=2, cls=JSONEncoder))
                f.write("\n")
                
            f.write("\n\n--- 3. RAW SOURCE DATA DUMP (AACT) ---\n")
            f.write("This section contains the exact rows extracted from the source CTGov database.\n")
            for table, rows in aact_data.items():
                f.write(f"\n>> SOURCE TABLE: ctgov.{table}\n")
                f.write(json.dumps(rows, indent=2, cls=JSONEncoder))
                f.write("\n")
                
        print("Done!")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if 'src_conn' in locals() and src_conn: src_conn.close()
        if 'dst_conn' in locals() and dst_conn: dst_conn.close()

if __name__ == '__main__':
    main()
