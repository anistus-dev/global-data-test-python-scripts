#!/usr/bin/env python3
"""
Script to query the AACT ctgov schema for a specific trial (nct_id).
Exports all related data to a CSV file with format:
- Row 1: Table name
- Row 2: Column names
- Following rows: Data rows (repeats for each table)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import sys
from pathlib import Path

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'aact',  # Change to your database name
    'user': 'postgres',  # Change to your username
    'password': 'password',  # Change to your password
}

SCHEMA = 'ctgov'


def get_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        exit(1)


def get_all_tables(conn):
    """Get all table names in the ctgov schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name;
        """, (SCHEMA,))
        return [row[0] for row in cur.fetchall()]


def get_nct_id_columns(conn, table_name):
    """Check which columns contain nct_id in a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            AND column_name = 'nct_id'
            ORDER BY ordinal_position;
        """, (SCHEMA, table_name))
        results = cur.fetchall()
        if results:
            return [row[0] for row in results]
        return []


def get_table_columns(conn, table_name):
    """Get column names and data types for a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (SCHEMA, table_name))
        return cur.fetchall()


def query_table_by_nct_id(conn, table_name, nct_id, nct_id_column):
    """Get data from a table for a specific nct_id."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = f"SELECT * FROM {SCHEMA}.{table_name} WHERE {nct_id_column} = %s LIMIT 1000;"
        cur.execute(query, (nct_id,))
        return cur.fetchall()


def main():
    # Get nct_id from command line argument
    if len(sys.argv) < 2:
        print("Usage: python query_trial_to_csv.py <nct_id>")
        print("Example: python query_trial_to_csv.py NCT04234412")
        sys.exit(1)
    
    nct_id = sys.argv[1]
    conn = get_connection()
    output_dir = Path(__file__).parent.parent / 'output'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f'trial_data_{nct_id}.csv'
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            tables = get_all_tables(conn)
            
            if not tables:
                print(f"No tables found in schema '{SCHEMA}'")
                return
            
            total_records = 0
            
            for table_name in tables:
                # Find nct_id column(s) in this table
                nct_id_columns = get_nct_id_columns(conn, table_name)
                
                if not nct_id_columns:
                    continue
                
                for nct_id_column in nct_id_columns:
                    try:
                        records = query_table_by_nct_id(conn, table_name, nct_id, nct_id_column)
                        total_records += len(records)
                        
                        # Write table name as first row
                        writer.writerow([table_name])
                        
                        # Get column names
                        columns = get_table_columns(conn, table_name)
                        column_names = [col[0] for col in columns]
                        
                        # Write column names as second row
                        writer.writerow(column_names)
                        
                        # Write data rows
                        if records:
                            for record in records:
                                row_data = [record.get(col) for col in column_names]
                                writer.writerow(row_data)
                            status_msg = f"✓ {table_name}: {len(records)} records"
                        else:
                            # Write empty row to indicate no data
                            writer.writerow([])
                            status_msg = f"⊘ {table_name}: (no records)"
                        
                        print(status_msg)
                        
                        # Add blank row between tables for readability
                        writer.writerow([])
                    
                    except Exception as e:
                        conn.rollback()
                        error_msg = f"✗ {table_name}: {str(e)}"
                        print(error_msg)
            
            if total_records == 0:
                print(f"\nNo records found for NCT ID: {nct_id}")
            else:
                print(f"\nTotal Records: {total_records}")
                print(f"Output saved to: {output_file.absolute()}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
