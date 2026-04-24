import os
import sys
import subprocess
import argparse
from scripts.config import CTGOV_DB_CONFIG

def init_aact_db(dump_path, clean=True):
    """
    Restore the AACT database from a .dmp file using pg_restore.
    """
    if not os.path.exists(dump_path):
        print(f"Error: Dump file not found at {dump_path}")
        sys.exit(1)

    print(f"Starting restoration of AACT database: {CTGOV_DB_CONFIG['database']}...")
    print(f"Using dump file: {dump_path}")

    # Prepare environment with password to avoid interactive prompts
    env = os.environ.copy()
    if CTGOV_DB_CONFIG.get('password'):
        env['PGPASSWORD'] = CTGOV_DB_CONFIG['password']

    # Build pg_restore command
    # -h: host, -p: port, -U: user, -d: database
    cmd = [
        "pg_restore",
        "-h", CTGOV_DB_CONFIG['host'],
        "-p", str(CTGOV_DB_CONFIG['port']),
        "-U", CTGOV_DB_CONFIG['user'],
        "-d", CTGOV_DB_CONFIG['database'],
        "--no-owner",
        "--no-privileges"
    ]

    if clean:
        cmd.append("--clean")
        cmd.append("--if-exists")

    cmd.append(dump_path)

    try:
        # Run pg_restore
        # Note: pg_restore often outputs warnings that are not necessarily errors (return code 0 still possible)
        result = subprocess.run(
            cmd,
            env=env,
            check=False, # We'll handle errors manually based on return code
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("\nDatabase restoration completed successfully!")
        elif result.returncode == 1:
            # Code 1 often means minor warnings (e.g. some objects already exist)
            print("\nRestoration finished with warnings (Return Code 1). This is common for pg_restore.")
            if result.stderr:
                print("Last few lines of warnings:")
                print("\n".join(result.stderr.splitlines()[-10:]))
        else:
            print(f"\nError: pg_restore failed with return code {result.returncode}")
            print(result.stderr)
            sys.exit(result.returncode)

    except FileNotFoundError:
        print("Error: 'pg_restore' command not found. Please ensure PostgreSQL client tools are installed.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Initialize the AACT database from a PostgreSQL dump file.")
    parser.add_argument("dump_file", help="Path to the .dmp file")
    parser.add_argument(
        "--no-clean", 
        action="store_false", 
        dest="clean",
        help="Do not use --clean and --if-exists flags (default: True)"
    )
    parser.set_defaults(clean=True)

    args = parser.parse_args()
    init_aact_db(args.dump_file, clean=args.clean)

if __name__ == "__main__":
    main()
