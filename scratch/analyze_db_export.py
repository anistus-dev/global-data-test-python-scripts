import csv
import os

def analyze_aact_export(filepath):
    if not os.path.exists(filepath):
        print(f"File {filepath} not found")
        return

    with open(filepath, 'r') as f:
        lines = f.readlines()

    current_table = None
    tables = {}
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
            
        # Check if this line is a table name (heuristic: no commas, preceded by newline or at start)
        if ',' not in line and (i == 0 or not lines[i-1].strip()):
            current_table = line
            i += 1
            if i < len(lines):
                header = lines[i].strip().split(',')
                tables[current_table] = {'header': header, 'row_count': 0}
                i += 1
                # Count rows until next empty line or end
                while i < len(lines) and lines[i].strip():
                    tables[current_table]['row_count'] += 1
                    i += 1
        else:
            i += 1
            
    return tables

tables = analyze_aact_export('data/fromdb_NCT06439277.csv')
for table, info in tables.items():
    print(f"Table: {table}")
    print(f"  Columns: {', '.join(info['header'])}")
    print(f"  Rows: {info['row_count']}")
    print("-" * 20)
