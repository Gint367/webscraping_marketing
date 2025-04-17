#!/usr/bin/env python3
import argparse
import json
import logging
import os
import re
from typing import Dict, List, Optional, Tuple, Union, Any

from dotenv import load_dotenv
from litellm import completion, JSONSchemaValidationError
from pydantic import BaseModel, Field

# Load environment variables from .env file
load_dotenv(override=True)

# Global tracking of failed files
failed_files = []

# Track compound word modifications for reporting
compound_word_stats = {
    "files_affected": set(),
    "words_modified": []
}

class PluralizedFields(BaseModel):
    """
    A model representing the pluralized fields in a company entry.
    This matches the structure for products, machines, and process_type fields.
    """
    products: Optional[List[str]] = Field(None, description="List of pluralized product names")
    machines: Optional[List[str]] = Field(None, description="List of pluralized machine names")
    process_type: Optional[List[str]] = Field(None, description="List of pluralized process types")


# Default temperature settings for retries
DEFAULT_TEMPERATURES = [0.5, 0.1, 1.0]

def setup_logging(log_level=logging.INFO) -> None:
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
    logging.getLogger('LiteLLM').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)


def clean_compound_words_for_field(
    words: List[str]
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Clean compound words containing conjunctions or separators for a single field.
    
    Args:
        words (List[str]): List of words to clean
        
    Returns:
        Tuple[List[str], List[Tuple[str, str]]]: 
            - Cleaned list of words
            - List of (original_word, cleaned_word) pairs for words that were modified
    """
    if not words:
        return [], []
        
    cleaned_words = []
    modified_pairs = []
    
    # Improved patterns to detect true compound phrases, not regular sentences
    # Match only when there's a clear pattern indicating a compound term, not just any "und"
    und_pattern = re.compile(r'^([A-Za-z\-]+)\s+(?:und|oder|&)\s+([A-Za-z\-]+)$')
    hyphen_und_pattern = re.compile(r'^([A-Za-z\-]+)-\s*(?:und|oder|&)\s+([A-Za-z\-]+)$')
    
    # Pattern to match any phrase with "und" between whitespaces
    general_und_pattern = re.compile(r'(\S+)\s+(?:und|oder|&)\s+(.+)$')
    
    # Pattern to detect words with "und -" - these should be kept as-is
    und_hyphen_pattern = re.compile(r'.*\s+(?:und|oder|&)\s+-.*')
    
    for word in words:
        # Special case for normal words without separators or conjunctions
        if not any(pattern in word.lower() for pattern in ['und', 'oder', '&', ',']):
            cleaned_words.append(word)
            continue
        
        # Skip phrases with "und -" pattern (e.g., "Leistungstransmissionstools und -händler")
        if und_hyphen_pattern.match(word):
            cleaned_words.append(word)
            continue
            
        # Handle comma-separated values
        if ',' in word:
            # Split by comma
            comma_parts = [part.strip() for part in word.split(',')]
            
            # First record this modification
            modified_pairs.append((word, f"Split into {len(comma_parts)} entries"))
            
            # Process each part
            for part in comma_parts:
                if not part:  # Skip empty parts
                    continue
                
                # Skip parts with "und -" pattern
                if und_hyphen_pattern.match(part):
                    cleaned_words.append(part)
                    continue
                    
                # Check for hyphenated conjunctions in each part
                hyphen_match = hyphen_und_pattern.match(part)
                if hyphen_match:
                    extracted = hyphen_match.group(2).strip()
                    cleaned_words.append(extracted)
                    modified_pairs.append((part, extracted))
                    continue
                
                # Check for normal conjunctions in each part
                und_match = und_pattern.match(part)
                if und_match:
                    extracted = und_match.group(2).strip()
                    cleaned_words.append(extracted)
                    modified_pairs.append((part, extracted))
                    continue
                    
                # If no conjunction patterns matched, add the part as is
                cleaned_words.append(part)
                
        # Handle hyphenated conjunction patterns
        elif hyphen_und_pattern.match(word):
            match = hyphen_und_pattern.match(word)
            extracted = match.group(2).strip()
            cleaned_words.append(extracted)
            modified_pairs.append((word, extracted))
            
        # Handle normal conjunction patterns (only for simple "X und Y" forms)
        elif und_pattern.match(word):
            match = und_pattern.match(word)
            extracted = match.group(2).strip()
            cleaned_words.append(extracted)
            modified_pairs.append((word, extracted))
            
        # Handle general cases where "und" is between whitespaces
        elif general_und_pattern.match(word):
            match = general_und_pattern.match(word)
            extracted = match.group(2).strip()
            cleaned_words.append(extracted)
            modified_pairs.append((word, extracted))
            
        # Fallback for any other case
        else:
            cleaned_words.append(word)
            
    return cleaned_words, modified_pairs


def clean_compound_words(
    fields_dict: Dict[str, List[str]]
) -> Tuple[Dict[str, List[str]], Dict[str, List[Tuple[str, str]]]]:
    """
    Clean compound words containing conjunctions or separators for all fields.
    
    Args:
        fields_dict (Dict[str, List[str]]): Dictionary with products, machines, and process_type lists
        
    Returns:
        Tuple[Dict[str, List[str]], Dict[str, List[Tuple[str, str]]]]: 
            - Dictionary with cleaned lists for each field
            - Dictionary with lists of (original_word, cleaned_word) pairs for each field
    """
    cleaned_fields = {}
    modified_pairs_by_field = {}
    
    # Handle empty input case by ensuring we have default empty lists for all expected fields
    if not fields_dict:
        return {"products": [], "machines": [], "process_type": []}, {}
    
    # Process each field if it exists
    for field_name in ["products", "machines", "process_type"]:
        if field_name in fields_dict and fields_dict[field_name]:
            cleaned_words, modified_pairs = clean_compound_words_for_field(fields_dict[field_name])
            cleaned_fields[field_name] = cleaned_words
            
            if modified_pairs:
                modified_pairs_by_field[field_name] = modified_pairs
        elif field_name in fields_dict:
            # Preserve empty lists in the output
            cleaned_fields[field_name] = []
    
    return cleaned_fields, modified_pairs_by_field


def track_cleaning_stats(
    modified_pairs_by_field: Dict[str, List[Tuple[str, str]]], 
    file_path: str
) -> None:
    """
    Track statistics for compound word cleaning.
    
    Args:
        modified_pairs_by_field (Dict[str, List[Tuple[str, str]]]): Modified pairs by field
        file_path (str): Path to the file being processed
    """
    if not modified_pairs_by_field or not file_path:
        return
        
    compound_word_stats["files_affected"].add(file_path)
    
    for field_name, modified_pairs in modified_pairs_by_field.items():
        for original, cleaned in modified_pairs:
            # Add to global stats
            compound_word_stats["words_modified"].append({
                "file": os.path.basename(file_path),
                "field": field_name,
                "original": original,
                "cleaned": cleaned
            })
            
            # Log what was changed
            logging.info(f"Cleaned compound word in {os.path.basename(file_path)} ({field_name}): '{original}' → '{cleaned}'")


def create_pluralization_prompt(fields_dict: Dict[str, List[str]]) -> str:
    """
    Create a prompt for the LLM to pluralize words with field context.
    
    Args:
        fields_dict (Dict[str, List[str]]): Dictionary with products, machines, and process_type lists
        
    Returns:
        str: The prompt for the LLM
    """
    prompt = """Please translate and pluralize the following German words. 
Return your answer as a JSON object with the same structure as my input, 
containing the pluralized German words in their respective categories.

Each input word must have exactly one output word in the same order.
Don't add any explanatory text, just return the JSON object.

Here is the input:
"""
    
    # Create a structured input that distinguishes between fields
    json_input = {}
    for field in ["products", "machines", "process_type"]:
        if field in fields_dict and fields_dict[field]:
            json_input[field] = fields_dict[field]
    
    # Add the JSON representation to the prompt
    prompt += json.dumps(json_input, ensure_ascii=False, indent=2)
    
    return prompt


def validate_pluralized_response(
    input_fields: Dict[str, List[str]], 
    output_fields: Dict[str, List[str]]
) -> Tuple[bool, str]:
    """
    Validate that the LLM response contains the expected structure and word counts.
    
    Args:
        input_fields (Dict[str, List[str]]): Original input fields
        output_fields (Dict[str, List[str]]): Pluralized output fields from LLM
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    # Check that all input fields are present in the output
    for field in input_fields:
        if field not in output_fields:
            return False, f"Missing field in response: {field}"
    
    # Check that each field has the correct number of words
    for field in input_fields:
        if len(input_fields[field]) != len(output_fields.get(field, [])):
            return False, f"Field {field} has {len(output_fields.get(field, []))} words but expected {len(input_fields[field])}"
    
    return True, ""


def pluralize_with_llm(
    fields_dict: Dict[str, List[str]], 
    file_path: str = None, 
    temperatures: List[float] = None
) -> Dict[str, List[str]]:
    """
    Use LLM to pluralize words with structured JSON output using PluralizedFields model.
    
    Args:
        fields_dict (Dict[str, List[str]]): Dictionary with products, machines, and process_type lists
        file_path (str, optional): Path to the file being processed
        temperatures (List[float], optional): List of temperature values for each retry
        
    Returns:
        Dict[str, List[str]]: Dictionary with pluralized words for each field
    """
    # Skip processing if all fields are empty
    if not any(fields_dict.get(f, []) for f in ["products", "machines", "process_type"]):
        return fields_dict
    
    # First clean compound words for all fields
    cleaned_fields, modified_pairs_by_field = clean_compound_words(fields_dict)
    
    # Track statistics for reporting
    if modified_pairs_by_field and file_path:
        track_cleaning_stats(modified_pairs_by_field, file_path)
    
    # Create a structured prompt for the LLM
    prompt = create_pluralization_prompt(cleaned_fields)
    
    # Set up temperatures for retries
    if temperatures is None:
        temperatures = DEFAULT_TEMPERATURES
        
    # Enable JSON schema validation
    max_retries = len(temperatures)
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Get the temperature for this attempt
            current_temp = temperatures[retry_count]
            
            # Call the LLM using LiteLLM with JSON response format
            response = completion(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=current_temp,
                max_tokens=1000,
                response_format={"type": "json_object"}
            )
            
            # Extract the content as JSON
            try:
                content = response.choices[0].message.content
                output_fields = json.loads(content)
                
                # Validate response structure and word counts
                is_valid, error_message = validate_pluralized_response(cleaned_fields, output_fields)
                
                if is_valid:
                    # Run clean_compound_words again on the response to handle any compound words
                    final_cleaned_fields, final_modified_pairs = clean_compound_words(output_fields)
                    
                    # Track statistics for any compounds cleaned in the response
                    if final_modified_pairs and file_path:
                        track_cleaning_stats(final_modified_pairs, file_path)
                    
                    # Create PluralizedFields object from the cleaned response
                    result = {}
                    for field in cleaned_fields:
                        if field in final_cleaned_fields:
                            result[field] = final_cleaned_fields[field]
                        else:
                            result[field] = cleaned_fields[field]  # Use original if missing
                    
                    return result
                else:
                    # If validation failed, retry
                    logging.warning(f"Validation error: {error_message}")
                    retry_count += 1
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON response: {e}")
                retry_count += 1
            
            # Check if we've reached max retries
            if retry_count >= max_retries:
                failure_info = f"Max retries reached for file: {file_path}"
                logging.error(failure_info)
                if file_path:
                    failed_fields = [f for f in cleaned_fields.keys()]
                    fields_str = ", ".join(failed_fields)
                    failed_files.append((file_path, fields_str))
                return cleaned_fields  # Return cleaned words even if pluralization failed
                
        except Exception as e:
            logging.error(f"Error pluralizing words with LLM: {e}")
            if file_path:
                failed_fields = [f for f in cleaned_fields.keys()]
                fields_str = ", ".join(failed_fields)
                failed_files.append((file_path, fields_str))
            return cleaned_fields  # Return cleaned words on error
    
    # This should not be reached, but just in case
    return cleaned_fields


def extract_fields_from_entry(entry: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extract the relevant fields from a single entry in the JSON file.
    Skips fields with empty lists.
    
    Args:
        entry (Dict[str, Any]): A single entry from the JSON file
        
    Returns:
        Dict[str, List[str]]: Dictionary with products, machines, and process_type lists
    """
    fields_dict = {}
    
    for field in ["products", "machines", "process_type"]:
        if field in entry and isinstance(entry[field], list):
            values = [str(item) for item in entry[field] if item]
            if values:  # Only include fields with non-empty values
                fields_dict[field] = values
    
    return fields_dict


def update_entry_with_pluralized_fields(
    entry: Dict[str, Any], 
    pluralized_fields: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    Update an entry with pluralized fields.
    
    Args:
        entry (Dict[str, Any]): The original entry
        pluralized_fields (Dict[str, List[str]]): The pluralized fields
        
    Returns:
        Dict[str, Any]: The updated entry
    """
    updated_entry = entry.copy()
    
    for field in ["products", "machines", "process_type"]:
        if field in pluralized_fields and pluralized_fields[field]:
            updated_entry[field] = pluralized_fields[field]
    
    return updated_entry


def process_json_file(input_file_path: str, output_file_path: str, temperatures: List[float] = None) -> None:
    """
    Process a single JSON file, pluralizing specific fields.
    
    Args:
        input_file_path (str): Path to the input JSON file.
        output_file_path (str): Path to save the processed JSON file.
        temperatures (List[float], optional): List of temperature values for each retry.
    """
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        if not isinstance(data, list):
            logging.error(f"Expected JSON array in {input_file_path}, but got {type(data)}")
            failed_files.append((input_file_path, "invalid_json_structure"))
            return
            
        # Process each entry in the JSON file
        for i, entry in enumerate(data):
            # Extract fields to be pluralized
            fields_dict = extract_fields_from_entry(entry)
            
            if fields_dict:
                # Pluralize all fields at once
                pluralized_fields = pluralize_with_llm(fields_dict, input_file_path, temperatures)
                
                # Update the entry with pluralized fields
                data[i] = update_entry_with_pluralized_fields(entry, pluralized_fields)
        
        # Save the processed data
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
            
        logging.info(f"Processed {input_file_path} -> {output_file_path}")
            
    except Exception as e:
        logging.error(f"Error processing file {input_file_path}: {e}")
        if input_file_path not in [f[0] for f in failed_files]:
            failed_files.append((input_file_path, "file_processing_error"))


def process_directory(input_dir: str, output_dir: str, temperatures: List[float] = None) -> None:
    """
    Process all JSON files in the input directory and save results to the output directory.
    
    Args:
        input_dir (str): Directory containing JSON files to process.
        output_dir (str): Directory to save processed JSON files.
        temperatures (List[float], optional): List of temperature values for each retry.
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        logging.error(f"Error creating directory: {e}")
        return  # Don't re-raise, just return after logging the error
        
    try:
        # Count total JSON files to process
        json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
        total_files = len(json_files)
        
        if total_files == 0:
            logging.warning(f"No JSON files found in {input_dir}")
            return
        
        logging.info(f"Found {total_files} JSON files to process")
        
        # Process each JSON file with progress reporting
        for i, filename in enumerate(json_files, 1):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)
            
            logging.info(f"Processing file {i}/{total_files}: {filename}")
            process_json_file(input_file_path, output_file_path, temperatures)
    except Exception as e:
        logging.error(f"Error accessing input directory: {e}")
        return
    
    # Report on compound word cleaning
    if compound_word_stats["files_affected"]:
        logging.info("\n===== COMPOUND WORD CLEANING SUMMARY =====")
        logging.info(f"Files affected: {len(compound_word_stats['files_affected'])}")
        logging.info(f"Words modified: {len(compound_word_stats['words_modified'])}")
        logging.info("Modified words (original → cleaned):")
        
        for item in compound_word_stats["words_modified"]:
            logging.info(f"  - {item['file']} ({item['field']}): '{item['original']}' → '{item['cleaned']}'")
    
    # Log summary of failed files
    if failed_files:
        logging.info("\n===== FAILURE SUMMARY =====")
        logging.info(f"Total files with failures: {len(set([f[0] for f in failed_files]))}")
        logging.info(f"Success rate: {(total_files - len(set([f[0] for f in failed_files]))) / total_files:.1%}")
        logging.info("Failed files and fields:")
        for file_path, field_name in failed_files:
            logging.info(f"  - {file_path}: {field_name}")
    else:
        logging.info(f"\nAll {total_files} files processed successfully with no pluralization failures.")


def process_file_or_directory(
    input_path: str, 
    output_path: str, 
    temperatures: List[float] = None
) -> None:
    """
    Process a file or directory based on the input path.
    
    Args:
        input_path (str): Path to an input file or directory
        output_path (str): Path to an output file or directory
        temperatures (List[float], optional): List of temperature values for each retry
    """
    if os.path.isfile(input_path):
        # Process a single file
        if not input_path.endswith('.json'):
            logging.error(f"Input file must be a JSON file: {input_path}")
            return
            
        logging.info(f"Processing single file: {input_path}")
        process_json_file(input_path, output_path, temperatures)
        
    elif os.path.isdir(input_path):
        # Process all files in a directory
        logging.info(f"Processing directory: {input_path}")
        process_directory(input_path, output_path, temperatures)
        
    else:
        logging.error(f"Input path does not exist: {input_path}")


def main() -> None:
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Pluralize words in JSON files using LLM.')
    parser.add_argument('--input', type=str, required=True, 
                        help='Input file or directory path (if directory, all JSON files will be processed)')
    parser.add_argument('--output', type=str, required=True, 
                        help='Output file or directory path (must match input type)')
    parser.add_argument('--temperatures', type=float, nargs='+', default=DEFAULT_TEMPERATURES, 
                        help='List of temperature values for each retry attempt (default: 0.5 0.1 1.0)')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Setup logging with the specified level
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level)
    
    logging.info(f"Starting pluralization with temperatures: {args.temperatures}")
    
    # Validate input/output path types match
    if os.path.isfile(args.input) and os.path.isdir(args.output):
        logging.error("When input is a file, output must be a file path")
        return
        
    if os.path.isdir(args.input) and os.path.isfile(args.output):
        logging.error("When input is a directory, output must be a directory path")
        return
    
    # Process file or directory
    process_file_or_directory(args.input, args.output, args.temperatures)
    
    logging.info("Pluralization process completed")


if __name__ == "__main__":
    main()
