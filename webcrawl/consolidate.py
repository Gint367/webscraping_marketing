import os
import json
import argparse
from typing import Dict, List, Any, Union
from collections import Counter


def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return []


def get_all_json_files(directory: str) -> List[str]:
    """Get all JSON files in the directory."""
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, f) for f in os.listdir(directory) 
            if f.endswith('.json') and os.path.isfile(os.path.join(directory, f))]


def normalize_items(items: List[str]) -> List[str]:
    """Normalize items by converting them to lowercase and removing duplicates."""
    normalized = {}
    for item in items:
        lower_item = item.lower()
        if lower_item not in normalized:
            normalized[lower_item] = item
    return list(normalized.values())

def sort_items(items: List[str]) -> List[str]:
    """Sort items by number of duplicates and prioritize items containing 'machine'."""
    # Normalize items
    items = normalize_items(items)
    
    # Count the occurrences of each item
    item_counts = Counter(items)
    
    # Sort items first by count (descending), then by presence of 'machine' (descending), then alphabetically
    sorted_items = sorted(item_counts.keys(), key=lambda x: (-item_counts[x], 'machine' in x.lower(), x))
    
    return sorted_items


def consolidate_entries(entries: List[Dict[str, Any]]) -> Union[Dict[str, Any], None]:
    """Consolidate multiple entries into a single entry."""
    if not entries:
        return None
    
    # Choose the longest company name
    company_name = max([e.get('company_name', '') for e in entries], key=len)
    
    # Skip entries with empty company names
    if not company_name.strip():
        return None
    
    # Combine and deduplicate products and machines
    products = []
    machines = []
    process_types = []
    lohnfertigung = False
    
    for entry in entries:
        products.extend(entry.get('products', []))
        machines.extend(entry.get('machines', []))
        process_types.extend(entry.get('process_type', []))
        if entry.get('lohnfertigung', False):
            lohnfertigung = True
    
    # Sort items by number of duplicates and prioritize items containing 'machine'
    unique_products = sort_items(products)
    unique_machines = sort_items(machines)
    unique_process_types = sort_items(process_types)
    
    return {
        'company_name': company_name,
        'products': unique_products,
        'machines': unique_machines,
        'process_type': unique_process_types,
        'lohnfertigung': lohnfertigung,
        'error': any(entry.get('error', False) for entry in entries)
    }


def get_default_output_path(input_path: str) -> str:
    """Generate default output path in the current working directory."""
    # Use current working directory for output
    output_dir = os.path.join(os.getcwd(), "consolidated_output")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.isfile(input_path):
        # Use input filename for the output
        base_name = os.path.basename(input_path)
        return os.path.join(output_dir, base_name)
    else:
        # If input is a directory, use directory name
        dir_name = os.path.basename(os.path.normpath(input_path))
        return os.path.join(output_dir, f"{dir_name}.json")


def process_files(input_path: str, output_path: str) -> None:
    """Process input files and generate consolidated output."""
    consolidated_entries = []
    
    if os.path.isfile(input_path):
        # Single file - consolidate the entries within this file
        entries = load_json_data(input_path)
        if entries:
            # All entries in this file are from the same company, so consolidate them
            consolidated_entry = consolidate_entries(entries)
            if consolidated_entry:
                consolidated_entries.append(consolidated_entry)
                
    else:
        # Directory - each file represents one company, but may have multiple entries
        for file_path in get_all_json_files(input_path):
            entries = load_json_data(file_path)
            if entries:
                # All entries in this file are from the same company, so consolidate them
                consolidated_entry = consolidate_entries(entries)
                if consolidated_entry:
                    consolidated_entries.append(consolidated_entry)
    
    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(consolidated_entries, f, indent=4, ensure_ascii=False)
    
    print(f"Consolidated data into {len(consolidated_entries)} companies")
    print(f"Output saved to {output_path}")


def main():
    """
    Load all JSON data from the specified input
    Group entries by company name (case-insensitive)
    For each group, create a consolidated entry
    Choose the longest company name
    Combine and deduplicate products, machines, and process types
    Sort products and machines to put any containing "machine" at the top
    Set lohnfertigung to true if any entry has it as true
    Write the consolidated results to the specified output file
    """
    parser = argparse.ArgumentParser(description='Consolidate company entries from JSON files')
    parser.add_argument('input', help='Input JSON file or directory containing JSON files')
    parser.add_argument('--output', '-o', help='Output JSON file path (optional)')
    
    args = parser.parse_args()
    
    # Use default output path if not specified
    output_path = args.output if args.output else get_default_output_path(args.input)
    
    process_files(args.input, output_path)


if __name__ == "__main__":
    main()
