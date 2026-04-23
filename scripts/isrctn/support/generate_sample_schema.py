import os
import sys
import csv
import argparse
import requests
import xml.etree.ElementTree as ET
import json
from collections import defaultdict

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

def get_tag_name(node):
    return node.tag.split('}')[-1] if '}' in node.tag else node.tag

def build_schema_tree(node, schema_node):
    tag = get_tag_name(node)
    
    # Initialize node if not present
    if tag not in schema_node:
        schema_node[tag] = {
            "count": 0,
            "sample_values": set(),
            "attributes": defaultdict(lambda: {"count": 0, "samples": set()}),
            "children": {}
        }
    
    current = schema_node[tag]
    current["count"] += 1
    
    # Store sample value if it's a leaf node with text
    if node.text and node.text.strip() and not list(node):
        val = node.text.strip()
        if len(val) > 200:
            val = val[:200] + "..."
        if len(current["sample_values"]) < 3: # Keep up to 3 unique samples
            current["sample_values"].add(val)
            
    # Store attribute samples
    for attr, val in node.attrib.items():
        attr_data = current["attributes"][attr]
        attr_data["count"] += 1
        if len(attr_data["samples"]) < 3:
            attr_data["samples"].add(val)
            
    # Recurse children
    for child in node:
        build_schema_tree(child, current["children"])

def set_to_list(obj):
    """Recursive helper to convert sets to lists for JSON serialization."""
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, dict):
        return {k: set_to_list(v) for k, v in obj.items()}
    return obj

def main():
    parser = argparse.ArgumentParser(description="Generate a comprehensive sample schema with real values.")
    parser.add_argument("csv_path", help="Path to the CSV file containing an 'ISRCTN' column")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of IDs to sample (default: 10)")
    parser.add_argument("--output", default="output/isrctn_sample_schema.json", help="Output file (default: output/isrctn_sample_schema.json)")
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    ids = []
    try:
        with open(args.csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            isrctn_col = next((h for h in reader.fieldnames if h.upper() == 'ISRCTN'), None)
            if not isrctn_col:
                print("Error: Could not find 'ISRCTN' column in CSV.")
                sys.exit(1)
            for i, row in enumerate(reader):
                if i >= args.limit: break
                val = row[isrctn_col]
                if val and val.strip(): ids.append(val.strip())
    except Exception as e:
        print(f"Error reading CSV: {e}"); sys.exit(1)

    if not ids:
        print("No IDs found to process."); sys.exit(1)

    print(f"Generating sample schema from {len(ids)} trials...")
    
    root_schema = {}
    
    for i, isrctn_id in enumerate(ids, 1):
        print(f"[{i}/{len(ids)}] Sampling {isrctn_id}...")
        root = fetch_xml(isrctn_id)
        if root is not None:
            build_schema_tree(root, root_schema)

    # Convert sets to lists for JSON
    serializable_schema = set_to_list(root_schema)

    with open(args.output, 'w') as f:
        json.dump(serializable_schema, f, indent=4)

    print(f"\nSchema generation complete!")
    print(f"Detailed sample schema saved to: {args.output}")
    print("You can open this JSON file to see the structure along with real data samples for each field.")

if __name__ == "__main__":
    main()
