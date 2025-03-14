#!/usr/bin/env python3

import os
import sys
import csv
import shutil
import argparse
from pathlib import Path

def extract_domain_from_path(file_path):
    """Extract domain name from file path like llm_extracted_data_maschinenbauer/elumatec.com_extracted.json"""
    filename = os.path.basename(file_path)
    # Remove _extracted.json and return the domain part
    return filename.replace('_extracted.json', '')

def copy_files(csv_file, source_folder, destination_folder):
    """Copy files from source to destination based on CSV file data"""
    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)
    
    copied_count = 0
    failed_count = 0
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            json_path = row['file']
            domain = extract_domain_from_path(json_path)
            markdown_filename = f"{domain}.md"
            
            source_file = os.path.join(source_folder, markdown_filename)
            destination_file = os.path.join(destination_folder, markdown_filename)
            
            try:
                if os.path.exists(source_file):
                    shutil.copy2(source_file, destination_file)
                    print(f"Copied: {markdown_filename}")
                    copied_count += 1
                else:
                    print(f"Warning: Source file not found - {source_file}")
                    failed_count += 1
            except Exception as e:
                print(f"Error copying {markdown_filename}: {str(e)}")
                failed_count += 1
    
    print(f"\nSummary:")
    print(f"Files successfully copied: {copied_count}")
    print(f"Files failed: {failed_count}")
    print(f"Total files processed: {copied_count + failed_count}")

def main():
    parser = argparse.ArgumentParser(description='Copy markdown files based on a CSV list.')
    parser.add_argument('csv_file', help='Path to the CSV file containing the list of files')
    parser.add_argument('source_folder', help='Source folder where markdown files are located')
    parser.add_argument('destination_folder', help='Destination folder where files will be copied')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found.")
        sys.exit(1)
    
    if not os.path.exists(args.source_folder):
        print(f"Error: Source folder '{args.source_folder}' not found.")
        sys.exit(1)
    
    copy_files(args.csv_file, args.source_folder, args.destination_folder)

if __name__ == "__main__":
    main()
