import argparse
import csv
import json
import os
import re
from pathlib import Path
import pandas as pd
from fuzzywuzzy import process

def clean_numeric_value(value):
    """Clean and standardize numeric values for comparison."""
    if pd.isna(value) or value == 'NA':
        return None
    
    if isinstance(value, str):
        # Remove surrounding quotes if present
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        
        # Handle German number format (1.234.567,89 -> 1234567.89)
        if ',' in value:
            if value.count('.') > 0:
                # Remove all dots and replace comma with dot
                value = value.replace('.', '').replace(',', '.')
            else:
                # Simply replace comma with dot for decimal point
                value = value.replace(',', '.')
        
        # Remove any non-numeric chars except the decimal point
        value = re.sub(r'[^\d.]', '', value)
        
        # If empty string after cleaning, return None
        if not value:
            return None
        
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def find_matching_company(company_name, company_list):
    """Find the best match for company_name in company_list using fuzzy matching."""
    # Clean up the filename to make it more comparable to company names in CSV
    clean_name = company_name.replace('_', ' ')
    
    # Try to find the best match
    match, score = process.extractOne(clean_name, company_list)
    
    if score >= 75:  # Threshold for a good match
        return match
    return None

def main():
    parser = argparse.ArgumentParser(description='Compare Sachanlagen values from regex CSV and LLM JSON files')
    parser.add_argument('--input', required=True, help='Path to the CSV file with regex extracted data')
    parser.add_argument('--folder', required=True, help='Path to the folder containing JSON files with LLM extracted data')
    args = parser.parse_args()
    
    # Read the CSV file
    try:
        # Explicitly set skipinitialspace to True to handle whitespace after commas
        csv_df = pd.read_csv(args.input, sep=',', skipinitialspace=True)
        
        # Check for and fix column name whitespace issues
        csv_df.columns = [col.strip() for col in csv_df.columns]
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Prepare output data
    results = []
    stats = {
        'total_json_files': 0,
        'matched_companies': 0,
        'matched_start_values': 0,
        'matched_end_values': 0,
        'mismatched_start_values': 0,
        'mismatched_end_values': 0,
        'missing_values': 0
    }
    
    # Process each JSON file
    json_files = list(Path(args.folder).glob('*_cleaned_extracted.json'))
    stats['total_json_files'] = len(json_files)
    
    # Print some debugging info
    print(f"Found {len(json_files)} JSON files in {args.folder}")
    print(f"CSV columns: {csv_df.columns.tolist()}")
    
    for json_file in json_files:
        # Extract company name from filename
        company_filename = json_file.stem.replace('_cleaned_extracted', '')
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"Error reading JSON file {json_file}: {e}")
            continue
        
        # Find matching company in CSV
        matching_company = find_matching_company(company_filename, csv_df['company name'].tolist())
        
        if matching_company:
            stats['matched_companies'] += 1
            
            # Get CSV values
            csv_row = csv_df[csv_df['company name'] == matching_company].iloc[0]
            csv_start = clean_numeric_value(csv_row['Sachanlagen Start'])
            csv_end = clean_numeric_value(csv_row['Sachanlagen End'])
            
            # Initialize variables for JSON data
            json_start = None
            json_end = None
            table_name = "Not Found"
            
            # Extract Sachanlagen values from JSON
            for table in json_data:
                values = table.get('values', {})
                sachanlagen_keys = [k for k in values.keys() if k.startswith('Sachanlagen_')]
                
                if len(sachanlagen_keys) >= 2:
                    # Sort keys by year (assuming format is Sachanlagen_YYYY)
                    sorted_keys = sorted(sachanlagen_keys, 
                                        key=lambda k: int(k.split('_')[1]) if '_' in k and k.split('_')[1].isdigit() else 0, 
                                        reverse=True)
                    
                    # Get the latest two years
                    if len(sorted_keys) >= 2:
                        latest_key = sorted_keys[0]
                        prev_key = sorted_keys[1]
                        
                        json_start = clean_numeric_value(values[latest_key])
                        json_end = clean_numeric_value(values[prev_key])
                        table_name = table.get('table_name', "Unknown")
                        break
            
            # Compare values and update stats
            start_match_status = "N/A"
            end_match_status = "N/A"
            
            if json_start is not None and csv_start is not None:
                # Use relative tolerance for large numbers
                if abs(json_start - csv_start) / max(abs(json_start), abs(csv_start)) < 0.01:
                    start_match_status = "Match"
                    stats['matched_start_values'] += 1
                else:
                    start_match_status = "Mismatch"
                    stats['mismatched_start_values'] += 1
            else:
                stats['missing_values'] += 1
            
            if json_end is not None and csv_end is not None:
                if abs(json_end - csv_end) / max(abs(json_end), abs(csv_end)) < 0.01:
                    end_match_status = "Match"
                    stats['matched_end_values'] += 1
                else:
                    end_match_status = "Mismatch"
                    stats['mismatched_end_values'] += 1
            else:
                stats['missing_values'] += 1
            
            results.append({
                'company_name': matching_company,
                'json_company': company_filename,
                'csv_start': csv_start,
                'csv_end': csv_end,
                'json_start': json_start,
                'json_end': json_end,
                'table_name': table_name,
                'start_match_status': start_match_status,
                'end_match_status': end_match_status
            })
    
    # Save results to CSV
    output_file = 'sachanlagen_comparison_results.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['company_name', 'json_company', 'csv_start', 'csv_end', 
                     'json_start', 'json_end', 'table_name', 'start_match_status', 'end_match_status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    # Print statistics
    print(f"Comparison complete. Results saved to {output_file}")
    print(f"Total JSON files: {stats['total_json_files']}")
    print(f"Matched companies: {stats['matched_companies']}")
    print(f"Matched start values: {stats['matched_start_values']}")
    print(f"Matched end values: {stats['matched_end_values']}")
    print(f"Mismatched start values: {stats['mismatched_start_values']}")
    print(f"Mismatched end values: {stats['mismatched_end_values']}")
    print(f"Missing values: {stats['missing_values']}")

if __name__ == "__main__":
    main()