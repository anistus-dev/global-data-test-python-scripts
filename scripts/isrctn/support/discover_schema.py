import os
import sys
import csv
import argparse
import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
import json

# Namespace used in ISRCTN XML
NS = {
    'isr': 'http://www.67bricks.com/isrctn',
}

def fetch_xml(isrctn_id):
    url = f"https://www.isrctn.com/api/trial/{isrctn_id}/format/default"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return ET.fromstring(response.text)
    except Exception as e:
        print(f"  Error fetching {isrctn_id}: {e}")
        return None

def get_full_path(node, parent_path=""):
    # Strip namespace for cleaner path
    tag = node.tag.split('}')[-1] if '}' in node.tag else node.tag
    return f"{parent_path}/{tag}" if parent_path else tag

def discover_paths(node, discovered_data, current_path=""):
    full_path = get_full_path(node, current_path)
    
    # Increment tag occurrence
    discovered_data['paths'][full_path] += 1
    
    # Check for attributes
    for attr in node.attrib:
        attr_path = f"{full_path}@{attr}"
        discovered_data['attributes'][attr_path] += 1
    
    # Recurse
    for child in node:
        discover_paths(child, discovered_data, full_path)

def main():
    parser = argparse.ArgumentParser(description="Discover the complete XML schema by sampling multiple ISRCTN records.")
    parser.add_argument("csv_path", help="Path to the CSV file containing an 'ISRCTN' column")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of IDs to sample (default: 100)")
    parser.add_argument("--output", default="output/schema_discovery_results.json", help="Output file (default: schema_discovery_results.json)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"Error: File not found at {args.csv_path}")
        sys.exit(1)

    ids = []
    try:
        with open(args.csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            isrctn_col = next((h for h in reader.fieldnames if h.upper() == 'ISRCTN'), None)
            if not isrctn_col:
                print("Error: Could not find 'ISRCTN' column in CSV.")
                sys.exit(1)
            
            for i, row in enumerate(reader):
                if i >= args.limit:
                    break
                val = row[isrctn_col]
                if val and val.strip():
                    ids.append(val.strip())
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    if not ids:
        print("No IDs found to process.")
        sys.exit(1)

    print(f"Starting schema discovery on {len(ids)} trials...")
    
    discovered_data = {
        'total_trials': 0,
        'paths': defaultdict(int),
        'attributes': defaultdict(int)
    }

    for i, isrctn_id in enumerate(ids, 1):
        print(f"[{i}/{len(ids)}] Processing {isrctn_id}...")
        root = fetch_xml(isrctn_id)
        if root is not None:
            discovered_data['total_trials'] += 1
            discover_paths(root, discovered_data)

    # Sort results
    sorted_paths = dict(sorted(discovered_data['paths'].items()))
    sorted_attrs = dict(sorted(discovered_data['attributes'].items()))

    output_data = {
        'total_trials_sampled': discovered_data['total_trials'],
        'unique_tag_paths_found': len(sorted_paths),
        'tag_paths': sorted_paths,
        'attribute_paths': sorted_attrs
    }

    with open(args.output, 'w') as f:
        json.dump(output_data, f, indent=4)

    print(f"\nDiscovery complete!")
    print(f"Total trials sampled: {discovered_data['total_trials']}")
    print(f"Unique tag paths found: {len(sorted_paths)}")
    print(f"Results saved to: {args.output}")
    
    print("\nTop 10 most common paths:")
    top_paths = sorted(sorted_paths.items(), key=lambda x: x[1], reverse=True)[:10]
    for path, count in top_paths:
        percentage = (count / discovered_data['total_trials']) * 100 if discovered_data['total_trials'] > 0 else 0
        print(f"  {path}: {count} ({percentage:.1f}%)")

if __name__ == "__main__":
    main()
