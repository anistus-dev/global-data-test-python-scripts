import os
import sys
import csv
import argparse
import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom

# Namespace used in ISRCTN XML
NS_URL = 'http://www.67bricks.com/isrctn'
ET.register_namespace('', NS_URL)

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

def merge_to_master(source_node, master_node):
    """
    Recursively merges source_node into master_node to create a union of all tags.
    Only keeps one instance of each unique child tag.
    """
    # 1. Merge Attributes
    for attr, val in source_node.attrib.items():
        if attr not in master_node.attrib:
            master_node.set(attr, val)
    
    # 2. Merge Text (only if master has none)
    if source_node.text and source_node.text.strip() and (not master_node.text or not master_node.text.strip()):
        master_node.text = source_node.text.strip()
    
    # 3. Merge Children
    source_children = list(source_node)
    for s_child in source_children:
        s_tag = s_child.tag
        
        # Find if this tag already exists in master
        m_child = master_node.find(s_tag)
        
        if m_child is None:
            # Create new child in master
            m_child = ET.SubElement(master_node, s_tag)
            merge_to_master(s_child, m_child)
        else:
            # Recursively merge into existing child
            merge_to_master(s_child, m_child)

def prettify(elem):
    """Return a pretty-printed XML string for the Element."""
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def main():
    parser = argparse.ArgumentParser(description="Generate a 'Master XML' containing the union of all discovered fields.")
    parser.add_argument("csv_path", help="Path to the CSV file containing an 'ISRCTN' column")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of IDs to sample (default: 50)")
    parser.add_argument("--output", default="output/isrctn_master_template.xml", help="Output file (default: output/isrctn_master_template.xml)")
    
    args = parser.parse_args()
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    ids = []
    try:
        with open(args.csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            isrctn_col = next((h for h in reader.fieldnames if h.upper() == 'ISRCTN'), None)
            if not isrctn_col:
                print("Error: Could not find 'ISRCTN' column in CSV."); sys.exit(1)
            for i, row in enumerate(reader):
                if i >= args.limit: break
                val = row[isrctn_col]
                if val and val.strip(): ids.append(val.strip())
    except Exception as e:
        print(f"Error reading CSV: {e}"); sys.exit(1)

    if not ids:
        print("No IDs found to process."); sys.exit(1)

    print(f"Generating Master XML from {len(ids)} trials...")
    
    master_root = None
    
    for i, isrctn_id in enumerate(ids, 1):
        print(f"[{i}/{len(ids)}] Sampling {isrctn_id}...")
        trial_root = fetch_xml(isrctn_id)
        if trial_root is not None:
            if master_root is None:
                # Initialize master with the first trial structure
                master_root = ET.Element(trial_root.tag)
                merge_to_master(trial_root, master_root)
            else:
                merge_to_master(trial_root, master_root)

    if master_root is not None:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(prettify(master_root))
        
        print(f"\nMaster Template generation complete!")
        print(f"File saved to: {args.output}")
        print("This file contains the union of all tags and attributes found in the sample.")
    else:
        print("Error: No data successfully sampled.")

if __name__ == "__main__":
    main()
