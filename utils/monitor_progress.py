import argparse
import glob
import json
import os
import re

import pandas as pd
from colorama import Fore, Style, init

# Define constants for repeated patterns
FOLDER_PATTERNS = {
    'bundesanzeiger': 'archive_bundesanzeiger_local/bundesanzeiger_local_{}*',
    'output': 'archive_bundesanzeiger_local/bundesanzeiger_local_{}_output*',
    'domain': 'domain_content_{}*',
    'llm': 'llm_extracted_{}*',
    'pluralized': 'pluralized_{}*'
}

FILE_PATTERNS = {
    'company': 'company_{}.csv',
    'machine_report': 'archive/machine_report_{}*.csv',
    'sachanlagen': 'sachanlagen_{}.csv',
    'merged': 'merged_{}*.csv',
    'final_export': 'final_export_{}.csv',
    'enriched': 'enriched_final_export_{}.csv',
    'consolidated_json': 'consolidated_output/*{}*.json',
    'consolidated_csv': 'consolidated_output/*{}*.csv'
}

# Initialize colorama
init()


def get_csv_files():
    """Find all company_*.csv files excluding those with _output in name"""
    csv_files = glob.glob(FILE_PATTERNS['company'].format('*'))
    return [f for f in csv_files if '_output' not in f]


def extract_category(filename):
    """Extract category name from filename"""
    match = re.search(r'company_(.+?)(?:_BA)?\.csv', filename)
    if match:
        return match.group(1)
    return None


def count_csv_rows(filepath):
    """Count number of rows in CSV file"""
    df = pd.read_csv(filepath)
    return len(df)


def find_matching_folders(category):
    """Find bundesanzeiger_local_ folders for a category"""
    folders = glob.glob(FOLDER_PATTERNS['bundesanzeiger'].format(category))
    return [f for f in folders if '_output' not in f]


def count_subfolders(folder_path):
    """Count direct subfolders in a folder"""
    if not os.path.exists(folder_path):
        return 0

    # Count only direct subfolders, excluding files and nested subfolders
    return len([name for name in os.listdir(folder_path)
                if os.path.isdir(os.path.join(folder_path, name))])


def get_machine_report_count(category):
    """Check if machine_report_<category> file exists and count rows"""
    report_files = glob.glob(FILE_PATTERNS['machine_report'].format(category))
    if not report_files:
        return 0

    # Use the first matching file if multiple exist
    try:
        df = pd.read_csv(report_files[0])
        return len(df)
    except Exception:
        return 0


def count_output_files(category):
    """Count files in bundesanzeiger_local_<category>_output folders"""
    output_folders = glob.glob(FOLDER_PATTERNS['output'].format(category))
    total_files = 0

    for folder in output_folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            # Count only files in the folder, not in subfolders
            total_files += len([name for name in os.listdir(folder)
                                if os.path.isfile(os.path.join(folder, name))])

    return total_files


def get_merged_file_count(category):
    """Check if merged_<category> file exists and count rows"""
    merged_files = glob.glob(FILE_PATTERNS['merged'].format(category))
    if not merged_files:
        return 0

    # Use the first matching file if multiple exist
    try:
        df = pd.read_csv(merged_files[0])
        return len(df)
    except Exception:
        return 0


def count_domain_content_files(category):
    """Count files in domain_content_<category> folders"""
    domain_folders = glob.glob(FOLDER_PATTERNS['domain'].format(category))
    total_files = 0

    for folder in domain_folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            # Count only files in the folder, not in subfolders
            total_files += len([name for name in os.listdir(folder)
                                if os.path.isfile(os.path.join(folder, name))])

    return total_files


def count_llm_files_and_errors(category):
    """Count files in llm_extracted_<category> folders and check for errors"""
    llm_folders = glob.glob(FOLDER_PATTERNS['llm'].format(category))
    total_files = 0
    error_count = 0
    error_files = []

    for folder in llm_folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            # Get all JSON files in the folder (not in subfolders)
            json_files = [name for name in os.listdir(folder)
                          if os.path.isfile(os.path.join(folder, name))
                          and name.lower().endswith('.json')]

            total_files += len(json_files)

            # Check each JSON file for error field
            for json_file in json_files:
                try:
                    with open(os.path.join(folder, json_file), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        has_error = False
                        # Check if data is a list that contains objects with error field
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and item.get('error') is True:
                                    has_error = True
                                    break  # Count one error per file, even if multiple objects have errors
                        # Check if the data itself is an object with error field
                        elif isinstance(data, dict) and data.get('error') is True:
                            has_error = True

                        if has_error:
                            error_count += 1
                            error_files.append(os.path.join(folder, json_file))
                except Exception:
                    # Count failed JSON parsing as errors too
                    error_count += 1
                    error_files.append(os.path.join(folder, json_file))

    return total_files, error_count, error_files


def count_pluralized_files(category):
    """Count files in pluralized_<category> folders"""
    pluralized_folders = glob.glob(FOLDER_PATTERNS['pluralized'].format(category))
    total_files = 0

    for folder in pluralized_folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            # Count only files in the folder, not in subfolders
            total_files += len([name for name in os.listdir(folder)
                                if os.path.isfile(os.path.join(folder, name))])

    return total_files


def check_consolidated_output(category):
    """Check if consolidated_output folder has JSON and CSV files for the category"""
    if not os.path.exists('consolidated_output') or not os.path.isdir('consolidated_output'):
        return False, False

    # Check for JSON files matching the category
    json_files = glob.glob(FILE_PATTERNS['consolidated_json'].format(category))
    has_json = len(json_files) > 0

    # Check for CSV files matching the category
    csv_files = glob.glob(FILE_PATTERNS['consolidated_csv'].format(category))
    has_csv = len(csv_files) > 0

    return has_json, has_csv


def check_final_export(category):
    """Check if final_export_<category>.csv file exists and count rows"""
    export_files = glob.glob(FILE_PATTERNS['final_export'].format(category))
    if not export_files:
        return 0, False

    try:
        df = pd.read_csv(export_files[0])
        return len(df), True
    except Exception:
        return 0, False


def check_enriched_data(category):
    """Check if enriched_final_export_<category>.csv file exists and count rows"""
    enriched_files = glob.glob(FILE_PATTERNS['enriched'].format(category))
    if not enriched_files:
        return 0, False

    try:
        df = pd.read_csv(enriched_files[0])
        return len(df), True
    except Exception:
        return 0, False


def get_sachanlagen_count(category):
    """Check if sachanlagen_<category>.csv file exists and count rows

    Args:
        category (str): The category name

    Returns:
        int: Number of rows in the CSV file (0 if file doesn't exist)
    """
    sachanlagen_files = glob.glob(FILE_PATTERNS['sachanlagen'].format(category))
    if not sachanlagen_files:
        return 0

    # Use the first matching file if multiple exist
    try:
        df = pd.read_csv(sachanlagen_files[0])
        return len(df)
    except Exception:
        return 0


def count_cleaned_html_files(category):
    """Count HTML files in cleaned_html folders inside bundesanzeiger_local_{category}_output

    Args:
        category (str): The category name

    Returns:
        int: Total count of cleaned HTML files
    """
    output_folders = glob.glob(FOLDER_PATTERNS['output'].format(category))
    total_files = 0

    for folder in output_folders:
        cleaned_html_path = os.path.join(folder, 'cleaned_html')
        if os.path.exists(cleaned_html_path) and os.path.isdir(cleaned_html_path):
            # Count only files in the cleaned_html folder, not in subfolders
            total_files += len([name for name in os.listdir(cleaned_html_path)
                                if os.path.isfile(os.path.join(cleaned_html_path, name))
                                and name.lower().endswith('.html')])

    return total_files


def main():
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description="Monitor progress of data processing pipeline")
    parser.add_argument("--verbose", action="store_true", help="Display detailed LLM error information")
    args = parser.parse_args()

    print("Monitoring Progress Report\n" + "-" * 50)

    csv_files = get_csv_files()
    results = []

    # Collect all results first
    for csv_file in csv_files:
        category = extract_category(csv_file)
        if not category:
            continue

        company_count = count_csv_rows(csv_file)

        # Find matching bundesanzeiger folders
        matching_folders = find_matching_folders(category)

        total_processed = 0
        for folder in matching_folders:
            subfolder_count = count_subfolders(folder)
            total_processed += subfolder_count

        # Get machine report count
        machine_report_count = get_machine_report_count(category)

        # Count output files
        output_files_count = count_output_files(category)

        # Count cleaned HTML files
        cleaned_html_count = count_cleaned_html_files(category)

        # Get sachanlagen count
        sachanlagen_count = get_sachanlagen_count(category)

        # Get merged file count
        merged_file_count = get_merged_file_count(category)

        # Count domain content files
        domain_content_count = count_domain_content_files(category)

        # Count LLM extracted files and errors
        llm_file_count, llm_error_count, llm_error_files = count_llm_files_and_errors(category)

        # Count pluralize_with_llm.py files
        pluralized_file_count = count_pluralized_files(category)

        # Check consolidate.py & convert_to_csv.py output files
        has_json, has_csv = check_consolidated_output(category)

        # Check final export files
        final_export_count, has_final_export = check_final_export(category)

        # Check enriched data files
        enriched_count, has_enriched = check_enriched_data(category)

        # Calculate progress percentages
        progress_percent = (total_processed / company_count * 100) if company_count > 0 else 0
        machine_report_percent = (machine_report_count / output_files_count * 100) if output_files_count > 0 else 0
        sachanlagen_percent = (sachanlagen_count / cleaned_html_count * 100) if cleaned_html_count > 0 else 0
        merged_percent = (merged_file_count / cleaned_html_count * 100) if cleaned_html_count > 0 else 0
        domain_content_percent = (domain_content_count / merged_file_count * 100) if merged_file_count > 0 else 0
        llm_percent = (llm_file_count / domain_content_count * 100) if domain_content_count > 0 else 0
        llm_error_percent = (llm_error_count / llm_file_count * 100) if llm_file_count > 0 else 0
        pluralized_percent = (pluralized_file_count / llm_file_count * 100) if llm_file_count > 0 else 0
        final_export_percent = (final_export_count / merged_file_count * 100) if merged_file_count > 0 and has_final_export else 0
        enriched_percent = (enriched_count / final_export_count * 100) if final_export_count > 0 and has_enriched else 0

        # Store the results
        results.append({
            'category': category,
            'total_processed': total_processed,
            'company_count': company_count,
            'progress_percent': progress_percent,
            'machine_report_count': machine_report_count,
            'output_files_count': output_files_count,
            'machine_report_percent': machine_report_percent,
            'cleaned_html_count': cleaned_html_count,
            'sachanlagen_count': sachanlagen_count,
            'sachanlagen_percent': sachanlagen_percent,
            'merged_file_count': merged_file_count,
            'merged_percent': merged_percent,
            'domain_content_count': domain_content_count,
            'domain_content_percent': domain_content_percent,
            'llm_file_count': llm_file_count,
            'llm_error_count': llm_error_count,
            'llm_percent': llm_percent,
            'llm_error_percent': llm_error_percent,
            'pluralized_file_count': pluralized_file_count,
            'pluralized_percent': pluralized_percent,
            'has_consolidated_json': has_json,
            'has_consolidated_csv': has_csv,
            'final_export_count': final_export_count,
            'has_final_export': has_final_export,
            'final_export_percent': final_export_percent,
            'enriched_count': enriched_count,
            'has_enriched': has_enriched,
            'enriched_percent': enriched_percent,
            'llm_error_files': llm_error_files
        })

    # Sort results by category alphabetically
    results.sort(key=lambda x: x['category'])

    # Print sorted results
    for result in results:
        print(f"Category: {result['category']}")

        # Format the progress information
        progress_info = f"Cleaned: {result['total_processed']}/{result['company_count']} ({result['progress_percent']:.2f}%)"

        # Format the machine report information
        machine_report_info = ""
        if result['machine_report_count'] > 0:
            machine_report_info = f" | Machine Reports: {result['machine_report_count']}/{result['output_files_count']} ({result['machine_report_percent']:.2f}%)"

        # Format the sachanlagen information
        sachanlagen_info = ""
        if result['sachanlagen_count'] > 0:
            sachanlagen_info = f" | Sachanlagen: {result['sachanlagen_count']}/{result['cleaned_html_count']} ({result['sachanlagen_percent']:.2f}%)"

        # Format the merged file information
        merged_info = ""
        if result['merged_file_count'] > 0:
            merged_info = f" | Merged: {result['merged_file_count']}/{result['cleaned_html_count']} ({result['merged_percent']:.2f}%)"

        print(f"  {progress_info}{machine_report_info}{sachanlagen_info}{merged_info}")

        # Format the domain content and LLM information on a new line
        domain_content_info = ""
        llm_info = ""
        pluralized_info = ""
        if result['domain_content_count'] > 0:
            domain_content_info = f"  Crawled: {result['domain_content_count']}/{result['merged_file_count']} ({result['domain_content_percent']:.2f}%)"

            if result['llm_file_count'] > 0:
                llm_info = f" | LLM: {result['llm_file_count']}/{result['domain_content_count']} ({result['llm_percent']:.2f}%) (Err: {result['llm_error_count']} ({result['llm_error_percent']:.2f}%))"

                if result['pluralized_file_count'] > 0:
                    pluralized_info = f" | Pluralized: {result['pluralized_file_count']}/{result['llm_file_count']} ({result['pluralized_percent']:.2f}%)"

            print(f"{domain_content_info}{llm_info}{pluralized_info}")

        # Add consolidated output information with colored icons
        checkmark_icon = f"{Fore.GREEN}✓{Style.RESET_ALL}"
        x_icon = f"{Fore.RED}✗{Style.RESET_ALL}"

        json_status = checkmark_icon if result['has_consolidated_json'] else x_icon
        csv_status = checkmark_icon if result['has_consolidated_csv'] else x_icon

        print(f"  Consolidated: {json_status}  | CSV: {csv_status}")

        # Add final export and enriched data information
        final_export_status = "Final Export: "
        if result['has_final_export']:
            final_export_status += f"{checkmark_icon}"
        else:
            final_export_status += f"{x_icon} "

        enriched_status = "Enriched: "
        if result['has_enriched']:
            enriched_status += f"{checkmark_icon}"
        else:
            enriched_status += f"{x_icon}"

        print(f"  {final_export_status} | {enriched_status}")

        # Display error filenames if verbose is enabled
        if args.verbose and result['llm_error_count'] > 0:
            print(f"\n  {Fore.RED}LLM Errors:{Style.RESET_ALL}")
            # Process filenames to extract just the base filename for cleaner output
            for error_file in result['llm_error_files']:
                base_filename = os.path.basename(error_file)
                print(f"    - {base_filename}")
            print()  # Add extra line for better readability

        print("-" * 50)

if __name__ == "__main__":
    main()
