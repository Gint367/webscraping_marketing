import json
import os
import csv
from collections import defaultdict
from typing import Dict, List

def read_json_files(folder_path: str) -> List[dict]:
    """Read all JSON files from the specified folder."""
    json_data = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data.append({
                    'filename': filename,
                    'data': json.load(f)
                })
    return json_data

def analyze_table_names(json_files: List[dict]) -> Dict[str, int]:
    """Analyze frequency of table names across all files."""
    table_names = defaultdict(int)
    for file_data in json_files:
        data = file_data['data']
        for table in data:
            table_name = table.get('table_name', '')
            if table_name:
                table_names[table_name] += 1
    return dict(table_names)

def compare_first_tables(json_files: List[dict]) -> Dict[str, str]:
    """Compare the first table from each file."""
    first_tables = {}
    for file_data in json_files:
        filename = file_data['filename']
        data = file_data['data']
        if data and len(data) > 0:
            first_tables[filename] = data[0].get('table_name', '')
    return first_tables

def analyze_header_levels(json_files: List[dict]) -> Dict[int, List[str]]:
    """Analyze files based on their table header levels."""
    header_level_files = defaultdict(list)
    
    for file_data in json_files:
        filename = file_data['filename']
        data = file_data['data']
        
        # Get maximum header level for each file
        max_header_level = 0
        for table in data:
            header_level = table.get('header_levels', 0)
            max_header_level = max(max_header_level, header_level)
        
        header_level_files[max_header_level].append(filename)
    
    return dict(header_level_files)

def truncate_string(s: str, max_length: int = 200) -> str:
    """Truncate string to max_length and add ellipsis if needed."""
    return s[:max_length] + '...' if len(s) > max_length else s

def format_analysis_results(table_frequencies: Dict[str, int], 
                          first_tables: Dict[str, str],
                          header_levels: Dict[int, List[str]]) -> dict:
    """Format analysis results into a dictionary for JSON export."""
    header_level_stats = {
        level: {
            'count': len(files),
            'files': files
        }
        for level, files in header_levels.items()
    }
    
    return {
        'summary': {
            'total_unique_tables': len(table_frequencies),
            'total_files_analyzed': len(first_tables)
        },
        'header_level_analysis': {
            'stats_by_level': header_level_stats,
            'levels_summary': {
                f'files_with_headers_le_{x}': sum(
                    len(header_levels[level]) 
                    for level in header_levels 
                    if level <= x
                )
                for x in range(max(header_levels.keys()) + 1)
            }
        },
        'table_frequencies': {
            name: count for name, count in sorted(
                table_frequencies.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
        },
        'first_tables_by_file': first_tables
    }

def format_for_csv(table_frequencies: Dict[str, int]) -> List[List[str]]:
    """Format the analysis results for CSV export."""
    # Prepare header row
    rows = [['Table Name', 'Occurrences']]
    
    # Add data rows sorted by frequency
    sorted_frequencies = sorted(table_frequencies.items(), key=lambda x: x[1], reverse=True)
    rows.extend([[name, str(count)] for name, count in sorted_frequencies])
    
    return rows

def analyze_table_key_counts(json_files: List[dict], max_keys: int = 3) -> Dict[str, Dict]:
    """
    Analyze files based on the number of keys in their table "values".
    
    Args:
        json_files: List of dictionaries containing JSON file data
        max_keys: Maximum number of keys to analyze up to (inclusive)
    
    Returns:
        Dictionary containing statistics about files based on their table value key counts
    """
    key_count_stats = defaultdict(lambda: {'files': [], 'count': 0})
    
    for file_data in json_files:
        filename = file_data['filename']
        data = file_data['data']
        
        # Track unique key counts found in this file
        key_counts_in_file = set()
        
        for table in data:
            for row in table.get('matching_rows', []):
                if isinstance(row.get('values'), dict):
                    num_keys = len(row['values'].keys())
                    if num_keys > 0:
                        key_counts_in_file.add(num_keys)
        
        # Add file to exactly matching categories
        for num_keys in key_counts_in_file:
            if num_keys <= max_keys:
                key_count_stats[num_keys]['files'].append(filename)
                key_count_stats[num_keys]['count'] += 1
    
    # Convert to regular dict and ensure all numbers up to max_keys exist
    result_stats = {
        i: key_count_stats[i] 
        for i in range(1, max_keys + 1)
        if key_count_stats[i]['count'] > 0  # Only include counts that actually exist
    }
    
    return {
        'stats_by_keys': result_stats,
        'summary': {
            f'files_with_keys_eq_{x}': len(key_count_stats[x]['files'])
            for x in range(1, max_keys + 1)
            if key_count_stats[x]['count'] > 0  # Only include counts that actually exist
        }
    }

def main():
    input_folder = "bundesanzeiger_local_data_output"
    output_file_json = "table_analysis_results.json"
    output_file_csv = "table_analysis_results.csv"
    
    json_files = read_json_files(input_folder)
    
    # Save the read JSON data to a file for reference
    with open('read_json_data.json', 'w', encoding='utf-8') as f:
        json.dump(json_files, f, indent=2, ensure_ascii=False)
    table_frequencies = analyze_table_names(json_files)
    first_tables = compare_first_tables(json_files)
    header_levels = dict(sorted(analyze_header_levels(json_files).items()))
    
    # Format and save results to JSON
    analysis_results = format_analysis_results(table_frequencies, first_tables, header_levels)
    
    # Save to JSON file
    with open(output_file_json, 'w', encoding='utf-8') as f:
        json.dump(analysis_results, f, indent=2, ensure_ascii=False)
    
    # Save to CSV file
    csv_rows = format_for_csv(table_frequencies)
    with open(output_file_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(csv_rows)
    
    # Print results to console
    print("\nAnalysis Results:")
    print("=" * 50)
    print(f"Total unique table names: {analysis_results['summary']['total_unique_tables']}")
    print(f"Total files analyzed: {analysis_results['summary']['total_files_analyzed']}")
    
    # Print header level analysis
    print("\nHeader Level Analysis:")
    print("-" * 50)
    for level, stats in analysis_results['header_level_analysis']['stats_by_level'].items():
        print(f"Level {level}: {stats['count']} files")
    
    print("\nCumulative Header Level Summary:")
    for key, count in analysis_results['header_level_analysis']['levels_summary'].items():
        print(f"{key}: {count} files")
    
    print("\nResults have been saved to:")
    print(f"- JSON: {output_file_json}")
    print(f"- CSV: {output_file_csv}")
    
    # Print detailed table frequencies
    print("\nTable Name Frequencies:")
    print("-" * 50)
    """     for table_name, count in analysis_results['table_frequencies'].items():
        print(f"Occurrences: {count}")
        print(f"Table Name: '{truncate_string(table_name)}'")
        print("-" * 50)
    """
    # Analyze table key counts (add before the JSON save)
    key_analysis = analyze_table_key_counts(json_files, max_keys=10)
    analysis_results['key_count_analysis'] = key_analysis
    
    # Add this after the header level analysis print section
    print("\nTable \"values\" Key Count Analysis:")
    print("-" * 50)
    for keys, stats in key_analysis['stats_by_keys'].items():
        print(f"Tables with {keys} keys: {stats['count']} files")
    
    print("\nCumulative Key Count Summary:")
    for key, count in key_analysis['summary'].items():
        print(f"{key}: {count} files")
if __name__ == "__main__":
    main()
