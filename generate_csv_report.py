import json
import csv
import os
from typing import List, Dict

def extract_company_name(filename: str) -> str:
    """
    Extracts company name from a filtered JSON filename by removing the '_filtered.json' suffix.
    
    Args:
        filename (str): Name of the JSON file
    
    Returns:
        str: Company name extracted from the filename
    """
    return filename.replace('_filtered.json', '')

def extract_values(data: List[Dict], max_values: int) -> tuple[List[str], str]:
    """
    Original value extraction function. Finds the first table with row "values" 
    that have a key length less than or equal to max_values.
    
    Args:
        data (List[Dict]): JSON data containing tables and their rows
        max_values (int): Maximum number of values to extract
    
    Returns:
        tuple[List[str], str]: A tuple containing:
            - List of numeric values (padded with empty strings if needed)
            - Name of the table where values were found
    """
    for table in data:
        table_name = table.get('table_name', '')[:100]
        for row in table.get('matching_rows', []):
            row_values = row.get('values', {})
            # Check if number of keys in values is less than or equal to max_values
            if len(row_values) <= max_values:
                # Get only numeric values, replace non-numeric with empty string
                numbers = [v if v.replace('.', '').replace(',', '').replace('0', '').isdigit() else '' 
                          for v in row_values.values() if isinstance(v, str)]
                # Pad with empty strings if needed
                while len(numbers) < max_values:
                    numbers.append('')
                return numbers[:max_values], table_name
    
    # If no matching table found, return empty list and empty string
    return [''] * max_values, ''

def extract_values_v2(data: List[Dict], max_values: int) -> tuple[List[str], str]:
    """
    Enhanced value extraction function. Finds the first table with numeric values in "values"
    that satisfies these conditions:
    - Contains only numeric values (excluding values starting with 0)
    - Number of numeric values is less than or equal to max_values
    
    Args:
        data (List[Dict]): JSON data containing tables and their rows
        max_values (int): Maximum number of values to extract
    
    Returns:
        tuple[List[str], str]: A tuple containing:
            - List of numeric values (padded with empty strings if needed)
            - Name of the table where values were found
    """
    for table in data:
        table_name = table.get('table_name', '')[:100]
        for row in table.get('matching_rows', []):
            row_values = row.get('values', {})
            
            # Filter numeric values
            numeric_values = {}
            for key, value in row_values.items():
                # Check if value is string and contains only numbers, dots, and commas
                if isinstance(value, str):
                    # Skip values that start with 0
                    if value.strip().startswith('0'):
                        continue
                    cleaned_value = value.replace('.', '').replace(',', '').strip()
                    if cleaned_value.isdigit():
                        numeric_values[key] = value
            
            # Check if number of numeric values is less than or equal to max_values
            if len(numeric_values) <= max_values:
                numbers = list(numeric_values.values())
                # Pad with empty strings if needed
                while len(numbers) < max_values:
                    numbers.append('')
                return numbers[:max_values], table_name
    
    # If no matching table found, return empty list and empty string
    return [''] * max_values, ''

def generate_csv_report(input_dir: str, output_file: str, n: int, use_v2: bool = False):
    """
    Generates a CSV report by processing JSON files and extracting numeric values.
    Only includes companies that have at least one valid numeric value.
    
    Args:
        input_dir (str): Directory containing the filtered JSON files
        output_file (str): Path where the CSV report will be saved
        n (int): Maximum number of machine values to extract per company
        use_v2 (bool, optional): Whether to use the enhanced value extraction. Defaults to False.
    """
    # Prepare CSV headers
    headers = ['Company', 'Table'] + [f'Machine_{i+1}' for i in range(n)]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        # Process each JSON file
        for filename in os.listdir(input_dir):
            if filename.endswith('_filtered.json'):
                file_path = os.path.join(input_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as jsonfile:
                    data = json.load(jsonfile)
                    
                company_name = extract_company_name(filename)
                extract_func = extract_values_v2 if use_v2 else extract_values
                values, table_name = extract_func(data, n)
                
                # Only write to CSV if at least one value is not empty
                if any(values):
                    writer.writerow([company_name, table_name] + values)

if __name__ == "__main__":
    input_directory = "./bundesanzeiger_local_data_output"
    
    N = 4  # Parameter N - change this value as needed
    
    output_csv = f"machine_report_n{N}.csv"
    # Use the new version with v2
    generate_csv_report(input_directory, output_csv, N, use_v2=False)
