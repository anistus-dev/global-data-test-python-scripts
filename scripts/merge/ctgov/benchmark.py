"""
Benchmark: Find the optimal workers × batch-size combination for CTGov ingestion.

Usage:
    uv run python -m scripts.merge.ctgov.benchmark
"""
import time
import subprocess
import psycopg2
from scripts.config import UNIFIED_DB_CONFIG


LIMIT = 500  # Studies per test run
CONFIGS = [
    # (workers, batch_size)
    (1, 500),
    (2, 250),
    (4, 100),
    (4, 250),
    (8, 50),
    (8, 100),
    (12, 50),
    (16, 50),
]


def reset_sync_log(limit):
    """Reset the first N 'completed' records back to 'pending' so we can re-test."""
    conn = psycopg2.connect(**UNIFIED_DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        UPDATE ingest.sync_log SET status = 'pending', processed_at = NULL, error_message = NULL
        WHERE source_code = 'CTGOV' AND source_record_id IN (
            SELECT source_record_id FROM ingest.sync_log
            WHERE source_code = 'CTGOV' AND status = 'completed'
            ORDER BY created_at ASC
            LIMIT %s
        )
    """, (limit,))
    reset_count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return reset_count


def run_test(workers, batch_size, limit):
    """Run the ingest script with the given parameters and measure wall time."""
    cmd = [
        "uv", "run", "python", "-m", "scripts.merge.ctgov.ingest_bulk",
        "--workers", str(workers),
        "--batch-size", str(batch_size),
        "--limit", str(limit),
    ]
    
    start = time.time()
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd="/home/anistus-wsl/Anistus/Documents/work/git/clinical-data-pipeline",
    )
    elapsed = time.time() - start
    
    # Count errors from output
    error_count = result.stdout.count("ERROR on")
    
    return elapsed, error_count, result.returncode


def main():
    print("=" * 70)
    print("CTGov Ingestion Benchmark")
    print(f"Testing {LIMIT} studies per configuration")
    print("=" * 70)
    
    results = []
    
    for workers, batch_size in CONFIGS:
        label = f"workers={workers:>2}, batch={batch_size:>3}"
        
        # Reset records back to pending
        reset_count = reset_sync_log(LIMIT)
        if reset_count < LIMIT:
            print(f"\n  WARNING: Only {reset_count} records available to reset (need {LIMIT})")
            if reset_count == 0:
                print("  SKIP: No completed records to reset. Run the ingest once first.")
                continue
        
        print(f"\n  Testing: {label} ...")
        elapsed, errors, rc = run_test(workers, batch_size, LIMIT)
        rate = LIMIT / elapsed if elapsed > 0 else 0
        results.append((workers, batch_size, elapsed, errors, rate))
        print(f"  Result:  {elapsed:.1f}s  |  {rate:.0f} studies/sec  |  {errors} errors  |  exit={rc}")
    
    # Summary table
    print(f"\n{'=' * 70}")
    print(f"{'Workers':>8} {'Batch':>6} {'Time (s)':>10} {'Rate (st/s)':>12} {'Errors':>8}")
    print(f"{'-' * 70}")
    for w, b, t, e, r in sorted(results, key=lambda x: x[2]):
        marker = " ← BEST" if t == min(x[2] for x in results) else ""
        print(f"{w:>8} {b:>6} {t:>10.1f} {r:>12.0f} {e:>8}{marker}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
