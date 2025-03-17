import json
import csv
import os
from typing import List, Dict
import argparse
from datetime import datetime

def extract_company_name(data: List[Dict]) -> str:
    """
    Extracts company name directly from the JSON data.
    Cleans any trailing commas from the company name.
    
    Args:
        data (List[Dict]): JSON data containing tables with company_name field
    
    Returns:
        str: Properly formatted company name
    """
    if not data or len(data) == 0:
        return "Unknown Company"
    
    # Get company name from the first table
    company_name = data[0].get('company_name', 'Unknown Company')
    
    return company_name

def extract_values(data: List[Dict], max_values: int, filter_words: List[str]) -> tuple[List[str], str, str]:
    """
    Enhanced value extraction function with number cleaning:
    - Removes anything after the comma
    - Removes dots used as thousand separators
    - Converts to plain numbers without formatting
    - Returns the maximum value for categorization
    """
    for table in data:
        table_name = table.get('table_name', '')[:100]
        
        # Check all header levels in the first row
        if table.get('matching_rows'):
            first_row = table['matching_rows'][0]
            all_headers = []
            for i in range(1, table.get('header_levels', 0) + 1):
                headers = first_row.get(f'header{i}', [])
                all_headers.extend([str(h).lower() for h in headers])
            
            if any(filter_word.lower() in header 
                   for header in all_headers 
                   for filter_word in filter_words):
                continue
            
            for row in table.get('matching_rows', []):
                row_values = row.get('values', {})
                
                # Filter and clean numeric values
                numeric_values = {}
                for key, value in row_values.items():
                    if isinstance(value, str):
                        # Skip values that start with 0
                        if value.strip().startswith('0'):
                            continue
                            
                        # Clean the number:
                        # 1. Take everything before the comma (if exists)
                        # 2. Remove dots (thousand separators)
                        # 3. Strip whitespace
                        clean_value = value.split(',')[0].replace('.', '').strip()
                        
                        if clean_value.isdigit():
                            numeric_values[key] = clean_value
                
                if len(numeric_values) <= max_values:
                    numbers = list(numeric_values.values())
                    while len(numbers) < max_values:
                        numbers.append('')
                    
                    # Get the maximum value for categorization
                    max_value = ''
                    if numbers:
                        valid_numbers = [n for n in numbers if n]
                        if valid_numbers:
                            max_value = max(valid_numbers)
                    
                    return numbers[:max_values], table_name, max_value
    
    return [''] * max_values, '', ''

def generate_csv_report(input_dir: str, 
                       output_file: str, 
                       n: int, 
                       extract_func: callable):
    """
    Generates a CSV report by processing JSON files and extracting numeric values.
    Only includes companies that have at least one valid numeric value.
    
    Args:
        input_dir (str): Directory containing the filtered JSON files
        output_file (str): Path where the CSV report will be saved
        n (int): Maximum number of machine values to extract per company
        extract_func (callable): Function to use for extracting values from tables
    """
    # Add timestamp to output filename
    timestamp = datetime.now().strftime("%Y%m%d")
    output_file_with_timestamp = output_file.replace('.csv', f'_{timestamp}.csv')
    
    # Prepare CSV headers - removed Maschinen Park Size from headers
    headers = ['Company', 'Table'] + [f'Machine_{i+1}' for i in range(n)]
    
    with open(output_file_with_timestamp, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        # Process each JSON file
        for filename in os.listdir(input_dir):
            if filename.endswith('_filtered.json'):
                file_path = os.path.join(input_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as jsonfile:
                    data = json.load(jsonfile)
                    
                company_name = extract_company_name(data)
                values, table_name, _ = extract_func(data, n)
                
                # Only write to CSV if at least one value is not empty
                if any(values):
                    writer.writerow([company_name, table_name.replace('\n', ' ')] + values)

if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description="Generate CSV report from JSON files.")
    # Add input directory argument
    parser.add_argument("input_directory", type=str, help="Path to the directory containing JSON files.")
    # Parse arguments
    args = parser.parse_args()
    
    # Get input directory from arguments
    input_directory = args.input_directory
    N = 3  # Parameter N - change this value as needed
    
    # Generate report using extract_values_v3 with filter words
    filter_words = ["anschaffungs","ahk", "abschreibung", "buchwert"]
    generate_csv_report(
        input_directory, 
        f"machine_report_n{N}.csv", 
        N,
        lambda data, n: extract_values(data, n, filter_words)
    )
    print(f"CSV report generated: machine_report_n{N}.csv")
