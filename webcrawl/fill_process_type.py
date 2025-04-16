#!/usr/bin/env python3
import os
import json
import logging
import argparse
import re
import time
import random
from typing import List
from litellm import completion, JSONSchemaValidationError
import litellm
from pydantic import BaseModel, Field

# Configure logging
def setup_logging(log_level=logging.INFO):
    """
    Configure logging with proper formatting.
    
    Args:
        log_level: The minimum logging level to display
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Set log level for HTTPx, which is used by AsyncWebCrawler
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # Set log level for LiteLLM and Botocore
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

# Global tracking for analytics
processed_companies = 0
empty_process_types_filled = 0
conjugation_issues_fixed = 0
conjugation_issues_fixed_companies = []

# Folder patterns for category extraction
FOLDER_PATTERNS = [
    r"llm_extracted_([^/\\]+)",
    r"pluralized_([^/\\]+)"
]

def extract_category_from_folder(folder_path: str) -> str:
    """
    Extract the category name from a folder path using known patterns.
    Supports both llm_extracted_<category> and pluralized_<category>.

    Args:
        folder_path (str): The folder path
    Returns:
        str: The extracted category or None if not found
    """
    basename = os.path.basename(os.path.normpath(folder_path))
    for pattern in FOLDER_PATTERNS:
        match = re.match(pattern, basename)
        if match:
            return match.group(1)
    return None

def extract_category_from_filename(filename: str) -> str:
    """
    Extract the category name from the filename.
    
    Args:
        filename (str): The filename (e.g., 'pluralized_aluminiumwerke.json')
        
    Returns:
        str: The extracted category (e.g., 'aluminiumwerke')
    """
    match = re.match(r'pluralized_([^\.]+)\.json', filename)
    if match:
        return match.group(1)
    return None

class ProcessTypes(BaseModel):
    """
    Pydantic model for LLM response containing process types.
    """
    process_types: List[str] = Field(
        ...,
        description="Liste typischer Fertigungsprozesse auf Deutsch (Plural, je ein Wort)"
    )

def generate_process_types(products: List[str], machines: List[str], category: str, max_retries: int = 3, base_delay: int = 3) -> List[str]:
    """
    Use LLM to generate process_type values based on products and category,
    with exponential backoff for retries. Uses JSON schema for structured output.

    Args:
        products (List[str]): List of products the company manufactures
        machines (List[str]): List of machines used
        category (str): The industry category
        max_retries (int): Maximum number of retry attempts
        base_delay (int): Initial delay for exponential backoff (in seconds)

    Returns:
        List[str]: Generated process types in German
    """
    if not products:
        return []
    machines_line = f"Die Maschinen sind z.b: {', '.join(machines)}\n" if machines else ""

    prompt = f"""
    Als Fertigungsexperte, gib mir die typischen Fertigungsprozesse (mit Maschinen) an, die zur Fertigung der folgenden Produkte in der Branche \"{category}\" verwendet werden.
    Die Produkte sind: {', '.join(products)}.
    {machines_line}

    Wichtig:
    1. Gib nur die Prozesse als JSON-Array unter dem Schlüssel 'process_types' zurück, keine Erklärungen.
    2. Maximal 5 Prozesse als Liste von Strings.
    3. Jeder Prozess sollte ein einzelnes Wort sein (keine Konjunktionen wie 'und').
    4. Jeder Prozess soll kurz und prägnant sein (für Keyword-Variablen im E-Mail-Marketing, zusammenfassen in 1 Wort).
    5. Die Prozesse müssen auf Deutsch sein.
    6. Verwende die Pluralform für die Prozesse (z.B. 'Fräsungen' statt 'Fräsung').
    7. Schließe nicht-fertigungsbezogene Wörter wie Transport, Logistik, Politik, Nachhaltigkeit usw. aus.
    8. Wenn du keine Prozesse findest, gib ['NA'] zurück.

    Deine Antwort (nur JSON!):
    """
    litellm.enable_json_schema_validation = True
    retries = 0
    while retries <= max_retries:
        try:
            response = completion(
                model="bedrock/amazon.nova-pro-v1:0",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800,
                response_format=ProcessTypes,  # Use Pydantic model for schema
            )
            # Extract JSON string from response and parse it
            content = response.choices[0].message.content
            data = json.loads(content)
            process_types = data.get("process_types", [])
            logging.debug(f"LLM returned process_types: {process_types}")
            return [p.strip() for p in process_types if p.strip()]
        except json.JSONDecodeError as ve:
            logging.error(f"JSON validation error: {ve}")
            return []
        except JSONSchemaValidationError as se:
            logging.error(f"JSON schema validation failed: {se}")
            return []
        except Exception as e:
            retries += 1
            if retries > max_retries:
                logging.error(f"Failed after {max_retries} retries: {e}")
                return []
            delay = base_delay * (2 ** (retries - 1)) + random.uniform(0, 0.5)
            logging.warning(f"Rate limit or error encountered. Retry {retries}/{max_retries} in {delay:.1f} seconds. Error: {e}")
            time.sleep(delay)

def check_for_conjugations(process_types: List[str], company_name: str) -> List[str]:
    """
    Check for and remove conjugations like 'und' from process types.
    
    Args:
        process_types (List[str]): List of process types to check
        company_name (str): Name of the company being processed
        
    Returns:
        List[str]: Cleaned process types without conjugations
    """
    global conjugation_issues_fixed
    global conjugation_issues_fixed_companies
    
    cleaned_process_types = []
    conjugation_words = ['und', 'oder', 'sowie', 'als auch', '&']
    
    for process in process_types:
        has_conjugation = False
        
        # Check if any conjugation word appears in the process
        for conj in conjugation_words:
            if conj in process.lower():
                has_conjugation = True
                conjugation_issues_fixed += 1
                if company_name not in conjugation_issues_fixed_companies:
                    conjugation_issues_fixed_companies.append(company_name)
                break
        
        # Only add processes without conjugations
        if not has_conjugation and process.strip():
            cleaned_process_types.append(process)
    
    # Filter out any empty strings and return non-empty list
    return [p for p in cleaned_process_types if p.strip()]

# Constants for 'na' words
NA_WORDS = ['na', 'n.a.', 'n/a', 'nicht verfügbar', 'keine', 'none']

def remove_na_words(process_types: List[str]) -> List[str]:
    """
    Remove any process types that exactly match known 'na' words (case-insensitive).
    Args:
        process_types (List[str]): List of process types to check
    Returns:
        List[str]: List with 'na' words removed
    """
    return [p for p in process_types if p.strip().lower() not in NA_WORDS]

def process_json_file(input_file: str, output_file: str, category: str = None) -> None:
    """
    Process a single JSON file to fill empty process_type fields.
    
    Args:
        input_file (str): Path to the input JSON file
        output_file (str): Path to save the processed JSON file
        category (str, optional): Category to use for LLM prompt. If None, extract from filename.
    """
    global processed_companies, empty_process_types_filled
    
    try:
        # Get category from argument or filename
        if not category:
            category = extract_category_from_filename(os.path.basename(input_file))
        if not category:
            logging.error(f"Could not extract category from filename: {input_file}")
            return
        
        logging.info(f"Processing file: {input_file} (Category: {category})")
        
        # Load the JSON file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Process each company in the data
        for company in data:
            processed_companies += 1
            company_name = company.get('company_name', 'Unknown')
            # Check if process_type is empty
            if not company.get('process_type'):
                products = company.get('products', [])
                machines = company.get('machines', [])
                if products:
                    # Generate process types using LLM
                    process_types = generate_process_types(products, machines, category)
                    # Remove 'na' words before further processing
                    process_types = remove_na_words(process_types)
                    # Check for and fix conjugations
                    process_types = check_for_conjugations(process_types, company_name)
                    # Update the company data
                    company['process_type'] = process_types
                    empty_process_types_filled += 1
                    logging.info(f"  Updated process_type for company: {company.get('company_name', 'Unknown')}")
                    logging.info(f"  Products: {products}")
                    logging.info(f"  Generated process_type: {process_types}")
            else:
                # Remove 'na' words from existing process_type
                original_process_type = company['process_type']
                cleaned_process_type = remove_na_words(original_process_type)
                # Check existing process_type for conjugations
                cleaned_process_type = check_for_conjugations(cleaned_process_type, company_name)
                # Update only if changes were made
                if cleaned_process_type != original_process_type:
                    company['process_type'] = cleaned_process_type
                    logging.info(f"  Fixed conjugations in process_type for company: {company.get('company_name', 'Unknown')}")
                    logging.info(f"  Original: {original_process_type}")
                    logging.info(f"  Fixed: {cleaned_process_type}")
        
        # Save the updated data
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Saved processed data to: {output_file}")
    
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {e}")

def find_pluralized_files(folder_path: str) -> List[str]:
    """
    Find all JSON files in the given folder.
    
    Args:
        folder_path (str): Path to the folder to scan
        
    Returns:
        List[str]: List of full paths to matching JSON files
    """
    matching_files = []
    
    if not os.path.isdir(folder_path):
        logging.error(f"Folder not found: {folder_path}")
        return matching_files
    
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            full_path = os.path.join(folder_path, filename)
            if os.path.isfile(full_path):
                matching_files.append(full_path)
    
    return matching_files

def main():
    """
    Main function to process JSON files - either a single file or all matching files in a folder.
    """
    parser = argparse.ArgumentParser(description='Fill empty process_type fields in JSON files using LLM. will overwrite the file')
    parser.add_argument('--input-file', type=str, 
                        help='Path to a single input JSON file (e.g., pluralized_aluminiumwerke.json)')
    parser.add_argument('--folder', type=str,
                        help='Path to a folder containing JSON files to process (will process all JSON files)')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directory to save processed JSON files (defaults to same as input)')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Setup logging with the specified level
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level)
    
    # Check if at least one input option is provided
    if not args.input_file and not args.folder:
        logging.error("Either --input-file or --folder must be specified")
        parser.print_help()
        return
    
    files_to_process = []
    category = None
    
    # If a single file is specified
    if args.input_file:
        if not os.path.isfile(args.input_file):
            logging.error(f"Input file not found: {args.input_file}")
            return
        
        if not args.input_file.endswith('.json'):
            logging.error(f"Input file must be a JSON file: {args.input_file}")
            return
        
        files_to_process.append(args.input_file)
        
        # Try to extract category from filename
        category = extract_category_from_filename(os.path.basename(args.input_file))
    
    # If a folder is specified
    if args.folder:
        folder_files = find_pluralized_files(args.folder)
        if not folder_files:
            logging.warning(f"No matching JSON files found in folder: {args.folder}")
        else:
            files_to_process.extend(folder_files)
            # Extract category from folder name
            category = extract_category_from_folder(args.folder)
    
    if not files_to_process:
        logging.error("No files to process")
        return
    
    logging.info(f"Found {len(files_to_process)} files to process")
    
    for input_file in files_to_process:
        filename = os.path.basename(input_file)
        output_filename = filename
        
        if args.output_dir:
            output_dir = args.output_dir
        else:
            output_dir = os.path.dirname(input_file)
        
        output_file = os.path.join(output_dir, output_filename)
        
        process_json_file(input_file, output_file, category=category)
    
    logging.info("===== PROCESSING SUMMARY =====")
    logging.info(f"Files processed: {len(files_to_process)}")
    logging.info(f"Companies processed: {processed_companies}")
    logging.info(f"Empty process_type fields filled: {empty_process_types_filled}")
    logging.info(f"Conjugation issues fixed: {conjugation_issues_fixed} (across {len(conjugation_issues_fixed_companies)} companies)")

if __name__ == "__main__":
    main()