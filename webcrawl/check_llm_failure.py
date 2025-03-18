import os
import json
import argparse
from collections import defaultdict
import csv
import re  # added import for regex


def check_multi_word_entries(file_path):
    """
    Check if a JSON file contains entries with conjunctions like 'and'/'und' in products, machines, or process_type.
    Returns a dict with results if found, None otherwise.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        results = {
            'file': file_path,
            'products': [],
            'machines': [],
            'process_type': [],
            'has_multi_word': False
        }
        
        pattern = re.compile(r'\b(?:and|und)\b', re.IGNORECASE)
        
        for item in data:
            # Check products
            if 'products' in item and isinstance(item['products'], list):
                for product in item['products']:
                    if isinstance(product, str) and pattern.search(product):
                        results['products'].append(product)
                        results['has_multi_word'] = True
            
            # Check machines
            if 'machines' in item and isinstance(item['machines'], list):
                for machine in item['machines']:
                    if isinstance(machine, str) and pattern.search(machine):
                        results['machines'].append(machine)
                        results['has_multi_word'] = True
            
            # Check process_type
            if 'process_type' in item and isinstance(item['process_type'], list):
                for process in item['process_type']:
                    if isinstance(process, str) and pattern.search(process):
                        results['process_type'].append(process)
                        results['has_multi_word'] = True
        
        return results if results['has_multi_word'] else None
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Find JSON files with multi-word entries in specific fields')
    parser.add_argument('folder_path', help='Path to the folder containing JSON files')
    parser.add_argument('--output', default='llm_failures.csv', help='Output CSV file name')
    args = parser.parse_args()
    
    if not os.path.isdir(args.folder_path):
        print(f"Error: {args.folder_path} is not a valid directory")
        return
    
    # Get current working directory for output file
    current_dir = os.getcwd()
    output_path = os.path.join(current_dir, args.output)
    
    multi_word_files = []
    stats = defaultdict(int)
    
    # Walk through the directory
    for root, _, files in os.walk(args.folder_path):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                result = check_multi_word_entries(file_path)
                
                if result:
                    multi_word_files.append(result)
                    stats['files_with_multi_word'] += 1
                    stats['products_multi_word'] += len(result['products'])
                    stats['machines_multi_word'] += len(result['machines'])
                    stats['process_type_multi_word'] += len(result['process_type'])
    
    # Print statistics
    print("\nAnalysis Results:")
    print(f"Total files with multi-word entries: {stats['files_with_multi_word']}")
    print(f"Total multi-word products: {stats['products_multi_word']}")
    print(f"Total multi-word machines: {stats['machines_multi_word']}")
    print(f"Total multi-word process types: {stats['process_type_multi_word']}")
    
    # Save list of files with issues
    if multi_word_files:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['file', 'products_count', 'machines_count', 'process_type_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in multi_word_files:
                writer.writerow({
                    'file': result['file'],
                    'products_count': len(result['products']),
                    'machines_count': len(result['machines']),
                    'process_type_count': len(result['process_type'])
                })
        
        print(f"\nList of files with multi-word entries saved to: {output_path}")
        
        # Print some examples if available
        if multi_word_files:
            print("\nExamples of multi-word entries:")
            example = multi_word_files[0]
            if example['products']:
                print(f"Products: {example['products'][:3]}")
            if example['machines']:
                print(f"Machines: {example['machines'][:3]}")
            if example['process_type']:
                print(f"Process Types: {example['process_type'][:3]}")


if __name__ == "__main__":
    main()
