import json
import csv
import os
import argparse

def convert_json_to_csv(json_file_path, csv_file_path=None):
    """
    Convert JSON file to CSV format with specified headers.
    Takes first 3 items from products, machines, and process_type arrays.
    
    Args:
        json_file_path: Path to the input JSON file
        csv_file_path: Path to the output CSV file. If None, derived from JSON filename
    """
    # Check if input file exists
    if not os.path.isfile(json_file_path):
        print(f"Error: Input file '{json_file_path}' does not exist")
        return None
    
    # If no CSV file path is provided, create one based on the JSON file name
    if csv_file_path is None:
        base_name = os.path.splitext(json_file_path)[0]
        csv_file_path = f"{base_name}.csv"
    
    try:
        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        # Define CSV headers
        headers = [
            'Company name', 'Company Url', 'Lohnfertigung(True/False)',
            'Produkte_1', 'Produkte_2', 'Produkte_3',
            'Maschinen_1', 'Maschinen_2', 'Maschinen_3',
            'Prozess_1', 'Prozess_2', 'Prozess_3'
        ]
        
        # Write to CSV with BOM for Excel compatibility
        with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.writer(csv_file)
            
            # Write header row
            writer.writerow(headers)
            
            # Process each company record
            for company in data:
                # Get first 3 entries for each array or empty strings if not available
                products = company.get('products', [])
                products = (products + ['', '', ''])[:3]
                
                machines = company.get('machines', [])
                machines = (machines + ['', '', ''])[:3]
                
                process_types = company.get('process_type', [])
                process_types = (process_types + ['', '', ''])[:3]
                
                # Format lohnfertigung as True/False string
                lohnfertigung = str(company.get('lohnfertigung', False))
                
                # Create row
                row = [
                    company.get('company_name', ''),
                    company.get('company_url', ''),
                    lohnfertigung,
                    *products,
                    *machines,
                    *process_types
                ]
                
                # Write row
                writer.writerow(row)
        
        print(f"Conversion successful. CSV file created at: {csv_file_path}")
        return csv_file_path
        
    except FileNotFoundError:
        print(f"Error: The file '{json_file_path}' was not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: '{json_file_path}' is not a valid JSON file")
        return None
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert JSON file to CSV format')
    parser.add_argument('input_file', help='Path to the input JSON file')
    parser.add_argument('-o', '--output', dest='output_file', 
                        help='Path to the output CSV file (optional)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Convert JSON to CSV
    convert_json_to_csv(args.input_file, args.output_file)
