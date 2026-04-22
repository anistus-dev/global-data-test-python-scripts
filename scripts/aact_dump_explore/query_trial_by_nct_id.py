#!/usr/bin/env python3
"""
Script to query the AACT ctgov schema for a specific trial (nct_id).
Retrieves all related data from all tables for the given nct_id.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
import sys
from pathlib import Path
from scripts.config import DB_CONFIG, SCHEMA


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
        print("Usage: python query_trial_by_nct_id.py <nct_id>")
        print("Example: python query_trial_by_nct_id.py NCT04234412")
        sys.exit(1)
    
    nct_id = sys.argv[1]
    conn = get_connection()
    output_dir = Path(__file__).parent.parent / 'output'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f'trial_data_{nct_id}.txt'
    
    try:
        with open(output_file, 'w') as f:
            tables = get_all_tables(conn)
            
            if not tables:
                msg = f"No tables found in schema '{SCHEMA}'"
                print(msg)
                f.write(msg + '\n')
                return
            
            header = f"\n{'='*80}\nTrial Data for NCT ID: {nct_id}\n{'='*80}\n"
            print(header, end='')
            f.write(header)
            
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
                        
                        table_header = f"\n{'-'*80}\nTABLE: {table_name} (Column: {nct_id_column})\n{'-'*80}\n"
                        print(table_header, end='')
                        f.write(table_header)
                        
                        record_count = f"Records Found: {len(records)}\n\n"
                        print(record_count, end='')
                        f.write(record_count)
                        
                        # Get column info
                        columns = get_table_columns(conn, table_name)
                        col_header = f"{'Column Name':<40} {'Data Type':<20}\n" + "-" * 60 + "\n"
                        print(col_header, end='')
                        f.write(col_header)
                        
                        for col_name, data_type in columns:
                            col_line = f"{col_name:<40} {data_type:<20}\n"
                            print(col_line, end='')
                            f.write(col_line)
                        
                        # Write data rows
                        if records:
                            data_header = f"\n\nDATA:\n"
                            print(data_header, end='')
                            f.write(data_header)
                            
                            for i, row in enumerate(records, 1):
                                row_header = f"\nRecord {i}:\n"
                                print(row_header, end='')
                                f.write(row_header)
                                
                                for key, value in row.items():
                                    # Truncate long values
                                    if isinstance(value, str) and len(value) > 200:
                                        row_data = f"  {key}: {value[:200]}...\n"
                                    else:
                                        row_data = f"  {key}: {value}\n"
                                    print(row_data, end='')
                                    f.write(row_data)
                        else:
                            no_data_msg = f"(No data available for this trial)\n"
                            print(no_data_msg, end='')
                            f.write(no_data_msg)
                    
                    except Exception as e:
                        conn.rollback()
                        error_msg = f"\nError querying {table_name}: {str(e)}\n"
                        print(error_msg, end='')
                        f.write(error_msg)
            
            footer = f"\n\n{'='*80}\nTotal Records Found: {total_records}\n{'='*80}\n"
            print(footer, end='')
            f.write(footer)
            
            print(f"\nOutput saved to: {output_file.absolute()}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
