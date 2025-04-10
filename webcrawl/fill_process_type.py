#!/usr/bin/env python3
import os
import json
import logging
import argparse
import re
import time
import random
from typing import List, Dict, Any
from litellm import completion

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

# Global tracking for analytics
processed_companies = 0
empty_process_types_filled = 0
conjugation_issues_fixed = 0
conjugation_issues_fixed_companies = []

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

def generate_process_types(products: List[str], category: str, max_retries=3, base_delay=3) -> List[str]:
    """
    Use LLM to generate process_type values based on products and category,
    with exponential backoff for retries.
    
    Args:
        products (List[str]): List of products the company manufactures
        category (str): The industry category
        max_retries (int): Maximum number of retry attempts
        base_delay (int): Initial delay for exponential backoff (in seconds)
        
    Returns:
        List[str]: Generated process types in German
    """
    if not products:
        return []
    
    # Create a prompt in German for better results
    prompt = f"""
    Als Fertigungsexperte, gib mir die typischen Produktionsprozesse(mit maschinen) an, die zur Fertigung der folgenden Produkte in der Branche "{category}" verwendet werden.
    Die Produkte sind: {', '.join(products)}.

    Wichtig:
    1. Gib nur die Prozesse zurück, keine Erklärungen
    2. Maximal 5 Prozesse als kommagetrennte Liste
    3. Jeder Prozess sollte ein einzelnes Wort sein (keine Konjunktionen wie 'und')
    4. Jeder Prozess soll kurz und prägnant sein (für Keyword-Variablen im E-Mail-Marketing, zussamenfassen in 1 wort).
    4. Die Prozesse müssen auf Deutsch sein
    5. Verwende die Pluralform für die Prozesse (z.B. 'Fräsungen' statt 'Fräsung')

    Deine Antwort:
    """
    
    retries = 0
    while retries <= max_retries:
        try:
            # Call the LLM using LiteLLM
            response = completion(
                model="bedrock/amazon.nova-pro-v1:0",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800,
            )
            
            # Extract the content and split by commas
            response_text = response.choices[0].message.content.strip()
            process_types = [process.strip() for process in response_text.split(',')]
            
            # Filter out any empty items
            process_types = [process for process in process_types if process]
            
            return process_types
        
        except Exception as e:
            retries += 1
            if retries > max_retries:
                logging.error(f"Failed after {max_retries} retries: {e}")
                return []
            
            # Calculate backoff delay with jitter
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

def process_json_file(input_file: str, output_file: str) -> None:
    """
    Process a single JSON file to fill empty process_type fields.
    
    Args:
        input_file (str): Path to the input JSON file
        output_file (str): Path to save the processed JSON file
    """
    global processed_companies, empty_process_types_filled
    
    try:
        # Get category from filename
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
                if products:
                    # Generate process types using LLM
                    process_types = generate_process_types(products, category)
                    
                    # Check for and fix conjugations
                    process_types = check_for_conjugations(process_types, company_name)
                    
                    # Update the company data
                    company['process_type'] = process_types
                    empty_process_types_filled += 1
                    
                    logging.info(f"  Updated process_type for company: {company.get('company_name', 'Unknown')}")
                    logging.info(f"  Products: {products}")
                    logging.info(f"  Generated process_type: {process_types}")
            else:
                # Check existing process_type for conjugations
                original_process_type = company['process_type']
                cleaned_process_type = check_for_conjugations(original_process_type, company_name)
                
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
    Find all JSON files with 'pluralized_' prefix in the given folder.
    
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
        if filename.startswith('pluralized_') and filename.endswith('.json'):
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
                        help='Path to a folder containing JSON files to process (will process all files starting with "pluralized_")')
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
    
    # Initialize a list to store files to process
    files_to_process = []
    
    # If a single file is specified
    if args.input_file:
        if not os.path.isfile(args.input_file):
            logging.error(f"Input file not found: {args.input_file}")
            return
        
        if not args.input_file.endswith('.json'):
            logging.error(f"Input file must be a JSON file: {args.input_file}")
            return
        
        files_to_process.append(args.input_file)
    
    # If a folder is specified
    if args.folder:
        folder_files = find_pluralized_files(args.folder)
        if not folder_files:
            logging.warning(f"No matching JSON files found in folder: {args.folder}")
        else:
            files_to_process.extend(folder_files)
    
    # Check if we have any files to process
    if not files_to_process:
        logging.error("No files to process")
        return
    
    # Log the number of files to process
    logging.info(f"Found {len(files_to_process)} files to process")
    
    # Process each file
    for input_file in files_to_process:
        # Get the base filename and create the output filename 
        filename = os.path.basename(input_file)
        output_filename = filename
        
        # Determine the output directory
        if args.output_dir:
            output_dir = args.output_dir
        else:
            output_dir = os.path.dirname(input_file)
        
        output_file = os.path.join(output_dir, output_filename)
        
        # Process the JSON file
        process_json_file(input_file, output_file)
    
    # Print summary statistics
    logging.info("===== PROCESSING SUMMARY =====")
    logging.info(f"Files processed: {len(files_to_process)}")
    logging.info(f"Companies processed: {processed_companies}")
    logging.info(f"Empty process_type fields filled: {empty_process_types_filled}")
    logging.info(f"Conjugation issues fixed: {conjugation_issues_fixed} (across {len(conjugation_issues_fixed_companies)} companies)")

if __name__ == "__main__":
    main()