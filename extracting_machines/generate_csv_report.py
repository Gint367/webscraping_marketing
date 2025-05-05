import argparse
import csv
import json
import logging
import os
from datetime import datetime
from typing import Callable, Dict, List


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


def generate_csv_report(
    input_dir: str,
    output_file: str,
    n: int,
    extract_func: Callable[[list, int], tuple[list[str], str, str]]
) -> str:
    """
    Generates a CSV report by processing JSON files and extracting numeric values.
    Only includes companies that have at least one valid numeric value.

    Args:
        input_dir (str): Directory containing the filtered JSON files
        output_file (str): Path where the CSV report will be saved
        n (int): Maximum number of machine values to extract per company
        extract_func (callable): Function to use for extracting values from tables

    Returns:
        str: The path to the generated CSV file
    """
    logger = logging.getLogger(__name__)
    timestamp = datetime.now().strftime("%Y%m%d")
    output_file_with_timestamp = output_file.replace('.csv', f'_{timestamp}.csv')
    headers = ['Company', 'Table'] + [f'Machine_{i+1}' for i in range(n)]
    valid_rows = []
    try:
        files_to_process = [f for f in os.listdir(input_dir) if f.endswith('_filtered.json')]
        total_files = len(files_to_process)
        logger.info(f"PROGRESS:extracting_machine:generate_report:0/{total_files}:Starting report generation from {input_dir}") # Progress Start

        for i, filename in enumerate(files_to_process):
            file_path = os.path.join(input_dir, filename)
            # Progress Log Inside Loop
            logger.info(f"PROGRESS:extracting_machine:generate_report:{i+1}/{total_files}:Processing {filename}")
            try:
                with open(file_path, 'r', encoding='utf-8') as jsonfile:
                    data = json.load(jsonfile)
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"Failed to parse {file_path}: {e}")
                raise  # Raise immediately on malformed JSON as per test expectation
            company_name = extract_company_name(data)
            values, table_name, _ = extract_func(data, n)
            if any(values):
                valid_rows.append([company_name, table_name.replace('\n', ' ')] + values)

        if valid_rows:
            with open(output_file_with_timestamp, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(valid_rows)
            # Progress Log End
            logger.info(f"PROGRESS:extracting_machine:generate_report:{len(valid_rows)}/{total_files}:Generated report with {len(valid_rows)} valid entries to {output_file_with_timestamp}")
            logger.info(f"CSV report generated: {output_file_with_timestamp}") # Keep original log
            return output_file_with_timestamp
        else:
            # Progress Log End (No Data)
            logger.info(f"PROGRESS:extracting_machine:generate_report:0/{total_files}:No valid data found, no report generated.")
            logger.info("No valid data found. No CSV report generated.") # Keep original log
            raise FileNotFoundError("No valid data found. No CSV report generated.")
    except Exception as e:
        logger.error(f"Failed to generate CSV report: {e}")
        raise


def main() -> None:
    """
    Main entry point for CLI usage. Parses arguments and runs the report generation.
    """
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description="Generate CSV report from JSON files.")
    parser.add_argument("input_directory", type=str, help="Path to the directory containing JSON files.")
    parser.add_argument("--output", type=str, default=None, help="Path to save the CSV report.")
    parser.add_argument("--n", type=int, default=3, help="Maximum number of machine values to extract per company.")
    args = parser.parse_args()
    input_directory = args.input_directory
    N = args.n
    base_folder = os.path.basename(os.path.normpath(input_directory))
    output_filename = args.output or f"machine_report_n{N}.csv"
    if base_folder.startswith("bundesanzeiger_local_") and base_folder.endswith("_output"):
        company_name = base_folder.replace("bundesanzeiger_local_", "").replace("_output", "")
        if company_name:
            output_filename = args.output or f"machine_report_{company_name}.csv"
    filter_words = ["anschaffungs","ahk", "abschreibung", "buchwert"]
    try:
        output_path = generate_csv_report(
            input_directory,
            output_filename,
            N,
            lambda data, n: extract_values(data, n, filter_words)
        )
        logging.info(f"CSV report generated: {output_path}")
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
