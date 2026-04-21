#!/usr/bin/env python3
"""
Script to explore the AACT ctgov schema in PostgreSQL.
Displays all tables, their columns, data types, and sample data.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
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


def get_table_columns(conn, table_name):
    """Get column names and data types for a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (SCHEMA, table_name))
        return cur.fetchall()


def get_sample_data(conn, table_name, limit=5):
    """Get sample data from a table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM {SCHEMA}.{table_name} LIMIT %s;", (limit,))
        return cur.fetchall()


def get_row_count(conn, table_name):
    """Get the number of rows in a table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table_name};")
        return cur.fetchone()[0]


def main():
    conn = get_connection()
    output_dir = Path(__file__).parent.parent / 'output'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / 'ctgov_exploration_output.txt'
    
    try:
        with open(output_file, 'w') as f:
            tables = get_all_tables(conn)
            
            if not tables:
                msg = f"No tables found in schema '{SCHEMA}'"
                print(msg)
                f.write(msg + '\n')
                return
            
            header = f"\n{'='*80}\nCTGOV Schema Exploration - Total Tables: {len(tables)}\n{'='*80}\n"
            print(header, end='')
            f.write(header)
            
            for table_name in tables:
                table_header = f"\n{'-'*80}\nTABLE: {table_name}\n{'-'*80}\n"
                print(table_header, end='')
                f.write(table_header)
                
                # Get row count
                try:
                    row_count = get_row_count(conn, table_name)
                    row_msg = f"Row Count: {row_count:,}\n\n"
                    print(row_msg, end='')
                    f.write(row_msg)
                except Exception as e:
                    error_msg = f"Error getting row count: {e}\n\n"
                    print(error_msg, end='')
                    f.write(error_msg)
                
                # Get columns
                columns = get_table_columns(conn, table_name)
                col_header = "COLUMNS:\n" + f"{'Column Name':<40} {'Data Type':<20} {'Nullable':<10}\n" + "-" * 70 + "\n"
                print(col_header, end='')
                f.write(col_header)
                
                for col_name, data_type, is_nullable in columns:
                    nullable = "Yes" if is_nullable == 'YES' else "No"
                    col_line = f"{col_name:<40} {data_type:<20} {nullable:<10}\n"
                    print(col_line, end='')
                    f.write(col_line)
                
                # Get sample data
                sample_header = f"\nSAMPLE DATA (first 5 rows):\n"
                print(sample_header, end='')
                f.write(sample_header)
                
                try:
                    sample_data = get_sample_data(conn, table_name)
                    
                    if sample_data:
                        # Write column names
                        col_names = list(sample_data[0].keys())
                        col_names_json = json.dumps(col_names, indent=2) + '\n'
                        print(col_names_json, end='')
                        f.write(col_names_json)
                        
                        # Write rows
                        for i, row in enumerate(sample_data, 1):
                            row_header = f"\nRow {i}:\n"
                            print(row_header, end='')
                            f.write(row_header)
                            
                            for key, value in row.items():
                                # Truncate long values
                                if isinstance(value, str) and len(value) > 100:
                                    row_data = f"  {key}: {value[:100]}...\n"
                                else:
                                    row_data = f"  {key}: {value}\n"
                                print(row_data, end='')
                                f.write(row_data)
                    else:
                        no_data_msg = "  (No data in this table)\n"
                        print(no_data_msg, end='')
                        f.write(no_data_msg)
                except Exception as e:
                    error_msg = f"  Error retrieving sample data: {e}\n"
                    print(error_msg, end='')
                    f.write(error_msg)
            
            footer = f"\n\n{'='*80}\nExploration Complete\n{'='*80}\n"
            print(footer, end='')
            f.write(footer)
            print(f"\nOutput saved to: {output_file.absolute()}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
