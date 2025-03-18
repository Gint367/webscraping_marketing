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
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON in {file_path}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error loading {file_path}: {e}")
        return []


def get_all_json_files(directory: str) -> List[str]:
    """
    Get all JSON files in the directory.
    """
    if not os.path.isdir(directory):
        return []
    
    json_files = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if (entry.is_file() and 
                entry.name.endswith('.json') and 
                not entry.name.startswith('combined_')):
                json_files.append(entry.path)
    
    return json_files


def normalize_items(items: List[str]) -> List[str]:
    """Normalize items by converting them to lowercase and removing duplicates."""
    normalized = {}
    for item in items:
        lower_item = item.lower()
        if lower_item not in normalized:
            normalized[lower_item] = item
    return list(normalized.values())


def sort_items(items: List[str]) -> List[str]:
    """
    Sort a list of items based on frequency, keyword presence, and alphabetical order.
    The sorting is done in the following priority:
    1. Number of duplicates (descending order)
    2. Presence of the word 'machine' (items with 'machine' ranked higher)
    3. Alphabetical order
    Args:
        items: A list of strings to be sorted
    Returns:
        A list of unique strings sorted according to the criteria above
    Note:
        Items are normalized before sorting using the normalize_items function
    """
    # Remove duplicates first (case-insensitive)
    unique_items = []
    lowercase_items = set()
    
    for item in items:
        if item.lower() not in lowercase_items:
            unique_items.append(item)
            lowercase_items.add(item.lower())
    
    # Sort with machine-related items first
    machine_items = [item for item in unique_items if "machine" in item.lower()]
    non_machine_items = [item for item in unique_items if "machine" not in item.lower()]
    
    return machine_items + non_machine_items


def filter_items(items: List[str], exclude_substrings: List[str], exact_match: bool = False) -> List[str]:
    """
    Filter out items containing any of the specified substrings or exact words.
    
    Args:
        items: List of strings to filter
        exclude_substrings: List of substrings to filter out
        exact_match: If True, will only filter items that match exactly (word boundary)
                    If False, will filter if the item contains the substring anywhere
    
    Returns:
        List of filtered items
    """
    if not exclude_substrings:
        return items
    
    # Convert exclude substrings to lowercase for case-insensitive comparison
    exclude_substrings_lower = [substr.lower() for substr in exclude_substrings]
    
    filtered_items = []
    for item in items:
        should_include = True
        item_lower = item.lower()
        
        for substr_lower in exclude_substrings_lower:
            if exact_match:
                # Split the item into words and check for exact matches
                words = item_lower.split()
                if substr_lower in words:
                    should_include = False
                    break
            else:
                # Check if the item contains the substring anywhere
                # This will match "automat" in words like "Automatisierungstechnik"
                if substr_lower in item_lower:
                    should_include = False
                    break
        
        if should_include:
            filtered_items.append(item)
    
    return filtered_items


def load_filter_words(filter_file: str) -> List[str]:
    """Load list of words/substrings to filter out from a file."""
    if not filter_file or not os.path.exists(filter_file):
        return []
    
    try:
        with open(filter_file, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except Exception as e:
        print(f"Error loading filter words from {filter_file}: {e}")
        return []


def consolidate_entries(entries: List[Dict[str, Any]], exclude_substrings: List[str] = None, exact_match: bool = False) -> Union[Dict[str, Any], None]:
    """Consolidate multiple entries into a single entry."""
    if not entries:
        return None
    
    # Choose the longest company name
    company_name = max([e.get('company_name', '') for e in entries], key=len)
    
    # Skip entries with empty company names
    if not company_name.strip():
        return None
    
    # Get the first non-empty company URL
    company_url = next((e.get('company_url', '') for e in entries if e.get('company_url')), '')
    
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
    
    # Filter out items containing excluded substrings
    if exclude_substrings:
        products = filter_items(products, exclude_substrings, exact_match)
        machines = filter_items(machines, exclude_substrings, exact_match)
        process_types = filter_items(process_types, exclude_substrings, exact_match)
    
    # Sort items by number of duplicates and prioritize items containing 'machine'
    unique_products = sort_items(products)
    unique_machines = sort_items(machines)
    unique_process_types = sort_items(process_types)
    
    return {
        'company_name': company_name,
        'company_url': company_url,
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


def process_single_file(file_path: str, exclude_substrings: List[str], exact_match: bool) -> Union[Dict[str, Any], None]:
    """Process a single JSON file and consolidate its entries."""
    entries = load_json_data(file_path)
    if entries:
        return consolidate_entries(entries, exclude_substrings, exact_match)
    return None


def process_files(input_path, output_path):
    """
    Process all JSON files in the input path or a specific file,
    consolidate the data, and save to the output file.
    """
    all_companies = {}  # Use a dictionary keyed by company_name for merging
    
    # Handle single file or directory
    if os.path.isfile(input_path):
        files = [input_path]
    else:
        files = [os.path.join(input_path, f) for f in os.listdir(input_path)
                if f.endswith('.json')]
    
    # Process each file
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                companies = json.load(f)
                
            for company in companies:
                company_name = company.get('company_name')
                
                # If company already exists, merge the data
                if company_name in all_companies:
                    existing_company = all_companies[company_name]
                    
                    # Merge products
                    if 'products' in company:
                        existing_company.setdefault('products', []).extend(company.get('products', []))
                        # Remove duplicates and sort
                        existing_company['products'] = sort_items(existing_company['products'])
                    
                    # Merge machines
                    if 'machines' in company:
                        existing_company.setdefault('machines', []).extend(company.get('machines', []))
                        # Remove duplicates and sort
                        existing_company['machines'] = sort_items(existing_company['machines'])
                    
                    # Merge process_types
                    if 'process_type' in company:
                        existing_company.setdefault('process_type', []).extend(company.get('process_type', []))
                        # Remove duplicates
                        existing_company['process_type'] = list(set(existing_company['process_type']))
                    
                    # Set lohnfertigung to True if any instance is True
                    if company.get('lohnfertigung', False):
                        existing_company['lohnfertigung'] = True
                        
                else:
                    # New company, add to dictionary
                    all_companies[company_name] = company
                    
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    
    # Convert companies dictionary to list
    result = list(all_companies.values())
    
    # Write consolidated data to output file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"Processed {len(files)} files. Found {len(result)} unique companies.")
    return result


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
    parser.add_argument('--filter', '-f', help='Path to a file containing words/substrings to filter out')
    parser.add_argument('--exact', '-e', action='store_true', help='Filter only exact word matches rather than substrings')
    
    args = parser.parse_args()
    
    # Use default output path if not specified
    output_path = args.output if args.output else get_default_output_path(args.input)
    
    process_files(args.input, output_path, args.filter, args.exact)


if __name__ == "__main__":
    main()
