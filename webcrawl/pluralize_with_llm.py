import os
import json
import argparse
from typing import List, Dict, Any, Union
from litellm import completion

def pluralize_with_llm(words: List[str]) -> List[str]:
    """
    Use LLM to pluralize a list of words.
    
    Args:
        words (List[str]): List of words to pluralize.
        
    Returns:
        List[str]: List of pluralized words.
    """
    if not words:
        return []
    
    # Create a prompt for the LLM
    prompt = "Please pluralize the following words. Return ONLY the pluralized words as a comma-separated list without explanations:\n\n"
    prompt += ", ".join(words)
    
    # Add retry logic
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Call the LLM using LiteLLM
            response = completion(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
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
                print(f"Warning: LLM returned {len(pluralized_words)} words but expected {len(words)}. Retry {retry_count}/{max_retries}.")
                
                # If we've reached max retries, return original words
                if retry_count >= max_retries:
                    print(f"Max retries reached. Returning original words.")
                    return words
                
        except Exception as e:
            print(f"Error pluralizing words with LLM: {e}")
            return words  # Return original words on error
    
    # This should not be reached, but just in case
    return words

def process_json_file(input_file_path: str, output_file_path: str) -> None:
    """
    Process a single JSON file, pluralizing specific fields.
    
    Args:
        input_file_path (str): Path to the input JSON file.
        output_file_path (str): Path to save the processed JSON file.
    """
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
        # Process each entry in the JSON file
        for entry in data:
            # Pluralize the specific fields
            if "products" in entry and isinstance(entry["products"], list):
                entry["products"] = pluralize_with_llm(entry["products"])
                
            if "machines" in entry and isinstance(entry["machines"], list):
                entry["machines"] = pluralize_with_llm(entry["machines"])
                
            if "process_type" in entry and isinstance(entry["process_type"], list):
                entry["process_type"] = pluralize_with_llm(entry["process_type"])
        
        # Save the processed data
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
            
        print(f"Processed {input_file_path} -> {output_file_path}")
            
    except Exception as e:
        print(f"Error processing file {input_file_path}: {e}")

def process_directory(input_dir: str, output_dir: str) -> None:
    """
    Process all JSON files in the input directory and save results to the output directory.
    
    Args:
        input_dir (str): Directory containing JSON files to process.
        output_dir (str): Directory to save processed JSON files.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each JSON file
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)
            process_json_file(input_file_path, output_file_path)

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Pluralize words in JSON files using LLM.')
    parser.add_argument('--input', type=str, required=True, help='Input directory containing JSON files')
    parser.add_argument('--output', type=str, required=True, help='Output directory for processed JSON files')
    
    args = parser.parse_args()
    
    process_directory(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()
