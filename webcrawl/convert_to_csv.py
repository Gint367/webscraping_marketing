import json
import csv
import os
import argparse
import logging
import re
from typing import List, Optional, Set

def load_omit_keywords(config_path: Optional[str]) -> Set[str]:
    """
    Load keywords to omit from a config file. Ignore lines starting with '#' and empty lines.

    Args:
        config_path: Path to the config file.

    Returns:
        Set of keywords to omit (lowercase).
    """
    if not config_path or not os.path.isfile(config_path):
        return set()
    with open(config_path, 'r', encoding='utf-8') as f:
        logging.info(f"Loading omit keywords from {config_path}")
        return set(
            line.strip().lower()
            for line in f
            if line.strip() and not line.strip().startswith('#')
        )

def clean_text(text: str) -> str:
    """
    Clean a string by removing unnecessary symbols (e.g., '\n', '\t') and normalizing spaces.

    Args:
        text: The input string to clean.
    Returns:
        Cleaned string.
    """
    # Replace newlines, tabs, and carriage returns with a space
    cleaned = re.sub(r'[\n\t\r]', ' ', text)
    # Replace multiple spaces with a single space
    cleaned = re.sub(r' +', ' ', cleaned)
    # Remove leading/trailing spaces
    return cleaned.strip()

def clean_items(items: List[str]) -> List[str]:
    """
    Clean a list of strings by removing unnecessary symbols and normalizing spaces.

    Args:
        items: List of strings to clean.
    Returns:
        List of cleaned strings.
    """
    return [clean_text(item) for item in items if item and clean_text(item)]

def filter_items(items: List[str], omit_keywords: Set[str]) -> List[str]:
    """
    Filter out items containing any of the omit keywords.
    - For each item in the input list (items), it converts the item to lowercase.
    - It checks if any of the omit_keywords (also lowercase) are present as substrings in the item.
    - If none of the keywords are found in the item, the item is kept; otherwise, it is omitted.
    - The result is a new list with all items containing any omit keyword removed.
    Args:
        items: List of strings to filter.
        omit_keywords: Set of keywords to omit.

    Returns:
        Filtered list of items.
    """
    return [item for item in items if not any(keyword in item.lower() for keyword in omit_keywords)]

def convert_json_to_csv(json_file_path: str, csv_file_path: Optional[str] = None, omit_config_path: Optional[str] = None) -> Optional[str]:
    """
    Convert JSON file to CSV format with specified headers, omitting items containing specified keywords.
    Takes first 3 items from products, machines, and process_type arrays after filtering.
    Args:
        json_file_path: Path to the input JSON file
        csv_file_path: Path to the output CSV file. If None, derived from JSON filename
        omit_config_path: Path to the omit keywords config file (optional)
    Returns:
        Path to the created CSV file, or None on failure.
    """
    logger = logging.getLogger(__name__)
    if not os.path.isfile(json_file_path):
        logger.error(f"Input file '{json_file_path}' does not exist")
        return None
    if csv_file_path is None:
        base_name = os.path.splitext(json_file_path)[0]
        csv_file_path = f"{base_name}.csv"
    omit_keywords = load_omit_keywords(omit_config_path)
    try:
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        headers = [
            'Company name', 'Company Url', 'Lohnfertigung(True/False)',
            'Produkte_1', 'Produkte_2', 'Produkte_3',
            'Maschinen_1', 'Maschinen_2', 'Maschinen_3',
            'Prozess_1', 'Prozess_2', 'Prozess_3'
        ]
        with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)
            for company in data:
                products = clean_items(company.get('products', []))
                products = filter_items(products, omit_keywords)
                products = (products + ['', '', ''])[:3]
                machines = clean_items(company.get('machines', []))
                machines = filter_items(machines, omit_keywords)
                machines = (machines + ['', '', ''])[:3]
                process_types = clean_items(company.get('process_type', []))
                process_types = filter_items(process_types, omit_keywords)
                process_types = (process_types + ['', '', ''])[:3]
                lohnfertigung = str(company.get('lohnfertigung', False))
                row = [
                    company.get('company_name', ''),
                    company.get('company_url', ''),
                    lohnfertigung,
                    *products,
                    *machines,
                    *process_types
                ]
                writer.writerow(row)
        logger.info(f"Conversion successful. CSV file created at: {csv_file_path}")
        return csv_file_path
    except FileNotFoundError:
        logger.error(f"The file '{json_file_path}' was not found")
        return None
    except json.JSONDecodeError:
        logger.error(f"'{json_file_path}' is not a valid JSON file")
        return None
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}")
        return None

def main() -> None:
    """
    Main entry point for command-line execution. Parses arguments and runs conversion.
    Raises exceptions for error conditions to support testability.
    """
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Convert JSON file to CSV format')
    parser.add_argument('input_file', help='Path to the input JSON file')
    parser.add_argument('-o', '--output', dest='output_file', help='Path to the output CSV file (optional)')
    parser.add_argument('-c', '--config', dest='omit_config', default='webcrawl/omit_keywords.txt', help='Path to omit keywords config file (optional)')
    args = parser.parse_args()

    result = convert_json_to_csv(args.input_file, args.output_file, args.omit_config)
    if result is None:
        # Determine the error type for more precise exception raising
        if not os.path.isfile(args.input_file):
            raise FileNotFoundError(f"Input file '{args.input_file}' does not exist")
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip() == '':
                    # Treat empty file as empty list, write only headers
                    headers = [
                        'Company name', 'Company Url', 'Lohnfertigung(True/False)',
                        'Produkte_1', 'Produkte_2', 'Produkte_3',
                        'Maschinen_1', 'Maschinen_2', 'Maschinen_3',
                        'Prozess_1', 'Prozess_2', 'Prozess_3'
                    ]
                    with open(args.output_file, 'w', newline='', encoding='utf-8-sig') as csv_file:
                        import csv
                        writer = csv.writer(csv_file)
                        writer.writerow(headers)
                    return
                json.loads(content)
        except json.JSONDecodeError:
            raise json.JSONDecodeError(f"'{args.input_file}' is not a valid JSON file", args.input_file, 0)
        raise Exception("Unknown error during conversion")

if __name__ == "__main__":
    main()
