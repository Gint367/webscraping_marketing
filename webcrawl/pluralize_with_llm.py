import os
import json
import argparse
import logging
import re
from typing import List, Tuple, Dict
from litellm import completion


# Global tracking of failed files
failed_files = []

# Track compound word modifications for reporting
compound_word_stats = {
    "files_affected": set(),
    "words_modified": []
}

# Default temperature settings for retries
DEFAULT_TEMPERATURES = [0.5, 0.1, 1.0]

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
    logging.getLogger('LiteLLM').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

def clean_compound_words(words: List[str]) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Clean compound words containing conjunctions or separators.
    
    Args:
        words (List[str]): List of words to clean
        
    Returns:
        Tuple[List[str], List[Tuple[str, str]]]: 
            - Cleaned list of words
            - List of (original_word, cleaned_word) pairs for words that were modified
    """
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

def pluralize_with_llm(words: List[str], file_path: str = None, field_name: str = None, 
                      temperatures: List[float] = None) -> List[str]:
    """
    Use LLM to pluralize a list of words.
    
    Args:
        words (List[str]): List of words to pluralize.
        file_path (str, optional): Path to the file being processed.
        field_name (str, optional): Name of the field being pluralized.
        temperatures (List[float], optional): List of temperature values for each retry.
        
    Returns:
        List[str]: List of pluralized words.
    """
    if not words:
        return []
    
    # First clean compound words
    cleaned_words, modified_pairs = clean_compound_words(words)
    
    # Track statistics for reporting
    if modified_pairs and file_path:
        compound_word_stats["files_affected"].add(file_path)
        for original, cleaned in modified_pairs:
            compound_word_stats["words_modified"].append({
                "file": os.path.basename(file_path),
                "field": field_name,
                "original": original,
                "cleaned": cleaned
            })
        
        # Log what was changed
        for original, cleaned in modified_pairs:
            logging.info(f"Cleaned compound word in {os.path.basename(file_path)}: '{original}' → '{cleaned}'")
    
    # Create a prompt for the LLM
    prompt = "Please translate and pluralize the following words into german language. Return ONLY the pluralized words as a comma-separated list without explanations:\n\n"
    prompt += ", ".join(cleaned_words)
    
    # Set up temperatures for retries
    if temperatures is None:
        temperatures = DEFAULT_TEMPERATURES
        
    # Ensure we have enough temperature values (fill with last value if needed)
    max_retries = len(temperatures)
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Get the temperature for this attempt
            current_temp = temperatures[retry_count]
            
            # Call the LLM using LiteLLM
            response = completion(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=current_temp,
                max_tokens=800
            )
            
            # Extract the content
            response_text = response.choices[0].message.content.strip()
            
            # Split the response back into a list
            pluralized_words = [word.strip() for word in response_text.split(",")]
            
            # Check if the list lengths match
            if len(pluralized_words) == len(cleaned_words):
                # Run clean_compound_words again to handle any compound words in the response
                final_cleaned_words, final_modified_pairs = clean_compound_words(pluralized_words)
                
                # Track statistics for reporting if any compounds were cleaned in the response
                if final_modified_pairs and file_path:
                    compound_word_stats["files_affected"].add(file_path)
                    for original, cleaned in final_modified_pairs:
                        compound_word_stats["words_modified"].append({
                            "file": os.path.basename(file_path),
                            "field": field_name,
                            "original": original,
                            "cleaned": cleaned
                        })
                    
                    # Log what was changed
                    for original, cleaned in final_modified_pairs:
                        logging.info(f"Cleaned compound word in LLM response for {os.path.basename(file_path)}: '{original}' → '{cleaned}'")
                
                return final_cleaned_words
            else:
                # If lengths don't match, retry
                retry_count += 1
                logging.warning(f"LLM returned {len(pluralized_words)} words but expected {len(cleaned_words)}. "
                      f"Retry {retry_count}/{max_retries} with temperature {temperatures[min(retry_count, max_retries-1)]}.")
                
                # If we've reached max retries, return original words
                if retry_count >= max_retries:
                    failure_info = f"Max retries reached for file: {file_path}, field: {field_name}"
                    logging.error(failure_info)
                    if file_path and file_path not in [f[0] for f in failed_files]:
                        failed_files.append((file_path, field_name))
                    return cleaned_words  # Return cleaned words even if pluralization failed
                
        except Exception as e:
            logging.error(f"Error pluralizing words with LLM: {e}")
            if file_path and file_path not in [f[0] for f in failed_files]:
                failed_files.append((file_path, field_name))
            return cleaned_words  # Return cleaned_words on error
    
    # This should not be reached, but just in case
    return cleaned_words

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
            
        # Process each entry in the JSON file
        for entry in data:
            # Pluralize the specific fields
            if "products" in entry and isinstance(entry["products"], list):
                entry["products"] = pluralize_with_llm(entry["products"], input_file_path, "products", temperatures)
                
            if "machines" in entry and isinstance(entry["machines"], list):
                entry["machines"] = pluralize_with_llm(entry["machines"], input_file_path, "machines", temperatures)
                
            if "process_type" in entry and isinstance(entry["process_type"], list):
                entry["process_type"] = pluralize_with_llm(entry["process_type"], input_file_path, "process_type", temperatures)
        
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
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
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

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Pluralize words in JSON files using LLM.')
    parser.add_argument('--input', type=str, required=True, help='Input directory containing JSON files')
    parser.add_argument('--output', type=str, required=True, help='Output directory for processed JSON files')
    parser.add_argument('--temperatures', type=float, nargs='+', default=DEFAULT_TEMPERATURES, 
                        help='List of temperature values for each retry attempt (default: 0.5 0.7 1.0)')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set the logging level')
    
    args = parser.parse_args()
    
    # Setup logging with the specified level
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level)
    
    logging.info(f"Starting pluralization with temperatures: {args.temperatures}")
    process_directory(args.input, args.output, args.temperatures)
    logging.info("Pluralization process completed")

if __name__ == "__main__":
    main()
