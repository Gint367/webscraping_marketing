import os
import json
import argparse
from typing import List
from litellm import completion

# Global tracking of failed files
failed_files = []

# Default temperature settings for retries
DEFAULT_TEMPERATURES = [0.5, 0.1, 1.0]

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
    
    # Create a prompt for the LLM
    prompt = "Please translate and pluralize the following words into german language. Return ONLY the pluralized words as a comma-separated list without explanations:\n\n"
    prompt += ", ".join(words)
    
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
            if len(pluralized_words) == len(words):
                return pluralized_words
            else:
                # If lengths don't match, retry
                retry_count += 1
                print(f"Warning: LLM returned {len(pluralized_words)} words but expected {len(words)}. "
                      f"Retry {retry_count}/{max_retries} with temperature {temperatures[min(retry_count, max_retries-1)]}.")
                
                # If we've reached max retries, return original words
                if retry_count >= max_retries:
                    failure_info = f"Max retries reached for file: {file_path}, field: {field_name}"
                    print(failure_info)
                    if file_path and file_path not in [f[0] for f in failed_files]:
                        failed_files.append((file_path, field_name))
                    return words
                
        except Exception as e:
            print(f"Error pluralizing words with LLM: {e}")
            if file_path and file_path not in [f[0] for f in failed_files]:
                failed_files.append((file_path, field_name))
            return words  # Return original words on error
    
    # This should not be reached, but just in case
    return words

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
            
        print(f"Processed {input_file_path} -> {output_file_path}")
            
    except Exception as e:
        print(f"Error processing file {input_file_path}: {e}")
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
    
    # Process each JSON file
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)
            process_json_file(input_file_path, output_file_path, temperatures)
    
    # Print summary of failed files
    if failed_files:
        print("\n===== FAILURE SUMMARY =====")
        print(f"Total files with failures: {len(set([f[0] for f in failed_files]))}")
        print("Failed files and fields:")
        for file_path, field_name in failed_files:
            print(f"  - {file_path}: {field_name}")
    else:
        print("\nAll files processed successfully with no pluralization failures.")

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Pluralize words in JSON files using LLM.')
    parser.add_argument('--input', type=str, required=True, help='Input directory containing JSON files')
    parser.add_argument('--output', type=str, required=True, help='Output directory for processed JSON files')
    parser.add_argument('--temperatures', type=float, nargs='+', default=DEFAULT_TEMPERATURES, 
                        help='List of temperature values for each retry attempt (default: 0.5 0.7 1.0)')
    
    args = parser.parse_args()
    
    process_directory(args.input, args.output, args.temperatures)

if __name__ == "__main__":
    main()
