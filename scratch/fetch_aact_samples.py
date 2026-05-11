import psycopg2
import os
from dotenv import load_dotenv
import json

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("CTGOV_DB_HOST"),
        port=os.getenv("CTGOV_DB_PORT"),
        database=os.getenv("CTGOV_DB_NAME"),
        user=os.getenv("CTGOV_DB_USER"),
        password=os.getenv("CTGOV_DB_PASSWORD")
    )

def fetch_samples():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Fetch diverse study IDs
    query = """
    (SELECT nct_id, 'Phase 3 Drug' as category FROM studies WHERE study_type='Interventional' AND phase='Phase 3' LIMIT 2)
    UNION
    (SELECT nct_id, 'Observational' as category FROM studies WHERE study_type='Observational' LIMIT 2)
    UNION
    (SELECT nct_id, 'With Results' as category FROM studies WHERE has_dmc=true AND results_first_submitted_date IS NOT NULL LIMIT 2)
    """
    cur.execute(query)
    samples = cur.fetchall()
    
    results = {}
    
    for nct_id, category in samples:
        print(f"\n--- Fetching Data for {nct_id} ({category}) ---")
        trial_data = {}
        
        # Core Study Data
        cur.execute("SELECT * FROM studies WHERE nct_id = %s", (nct_id,))
        trial_data['study'] = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        
        # Sponsors
        cur.execute("SELECT * FROM sponsors WHERE nct_id = %s", (nct_id,))
        trial_data['sponsors'] = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        
        # Design Groups (Arms)
        cur.execute("SELECT * FROM design_groups WHERE nct_id = %s", (nct_id,))
        trial_data['arms'] = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        
        # Interventions
        cur.execute("SELECT * FROM interventions WHERE nct_id = %s", (nct_id,))
        trial_data['interventions'] = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        
        # Outcomes
        cur.execute("SELECT * FROM outcomes WHERE nct_id = %s LIMIT 3", (nct_id,))
        trial_data['outcomes'] = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        
        # Facilities (Sites)
        cur.execute("SELECT * FROM facilities WHERE nct_id = %s LIMIT 3", (nct_id,))
        trial_data['facilities'] = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        
        # Calculated Values
        cur.execute("SELECT * FROM calculated_values WHERE nct_id = %s", (nct_id,))
        row = cur.fetchone()
        if row:
            trial_data['calculated'] = dict(zip([d[0] for d in cur.description], row))
            
        results[nct_id] = trial_data
        
    print(json.dumps(results, indent=2, default=str))
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_samples()
