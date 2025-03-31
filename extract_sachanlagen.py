import os
import json
import asyncio
from pydantic import BaseModel, Field, RootModel
from typing import List
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig
import argparse


class SachanlagenValues(BaseModel):
    """Model for Sachanlagen values from different years"""
    values: dict[str, str] = Field(
        description="Dictionary of Sachanlagen values, keys are in format 'Sachanlagen_[number]' and values are monetary amounts as strings"
    )
    table_name: str = Field(
        description="Name or heading of the table containing the Sachanlagen values"
    )


class Sachanlagen(RootModel):
    """Container for all Sachanlagen values extracted from the document"""
    root: List[SachanlagenValues] = Field(
        default=[],
        description="List of table entries containing Sachanlagen values"
    )

prompt = """
Extract the "Sachanlagen" items from tables found in financial statement HTML files and identify the table names or headings associated with them.

Review the HTML content to locate all tables and preceding headings that may denote the table name. For each identified table, extract "Sachanlagen" items and their corresponding values.

# Steps

1. **Parse the HTML**: Open and parse the HTML file to locate tables and headings.
2. **Identify Table Names**: First look within the table header. if not found then look at the preceding heading.
3. **Extract "Sachanlagen" Values**: For each table, extract items labeled "Sachanlagen" with their corresponding values. there should be at least 2 values from 2 different year
4. **Organize Data**: Structure the extracted data into JSON format, listing the table names and corresponding "Sachanlagen" values.
5. **Output Result**: Present the data as a JSON array with the specified structure.

# Output Format

The output should be a JSON array with the following structure:
```json
[
  {
    "table_name": "TableName",
    "values": {
      "Sachanlagen_1": "Value1",
      "Sachanlagen_2": "Value2"
    }
  },
  ...
]
```

# Examples

**Input:**
HTML content includes two tables with headings, each containing "Sachanlagen" items.

**Output:**
```json
[
  {
    "table_name": "Aktiva",
    "values": {
      "Sachanlagen_1": "100.100,50",
      "Sachanlagen_2": "200.200,00"
    }
  }
]
```

**Note:** 
- The number of values under "values" can vary based on the number of "Sachanlagen" found in each table.
- make sure to not take the numbers from the sub item of the Sachanlagen like Technische Anlagen und Maschinen,Grundstücke, Andere Anlagen, etc.
"""


def ensure_output_directory(directory="llm_extracted_data"):
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


dispatcher = MemoryAdaptiveDispatcher(
    memory_threshold_percent=70.0,
    check_interval=1.0,
    max_session_permit=3,
)
rate_limiter = RateLimiter(
    base_delay=(30, 60), max_delay=60, max_retries=3, rate_limit_codes=[429, 503]
)


async def process_files(file_paths, llm_strategy, output_dir):
    """
    Process one or more files using a specified LLM extraction strategy and save the results.
    Args:
        file_paths (list of str): List of file paths to be processed.
        llm_strategy (LLMStrategy): The language model strategy to use for extraction.
        output_dir (str): Directory where the extracted data and combined results will be saved.
    Returns:
        list: A list of extracted content from each file.
    """
    # Convert file paths to URLs with file:// protocol
    file_urls = [f"file://{os.path.abspath(path)}" for path in file_paths]

    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=llm_strategy,
    )

    async with AsyncWebCrawler() as crawler:
        results = await crawler.arun_many(
            urls=file_urls,
            config=config,
            dispatcher=dispatcher,
            rate_limiter=rate_limiter,
        )

        extracted_data = []
        for idx, result in enumerate(results):
            file_path = file_paths[idx]
            if result.success and result.extracted_content:
                # Extract source URL info for validation and naming
                original_url = result.url
                parsed_url = urlparse(original_url)

                # Always use the URL for naming, regardless of whether it's a file or web URL
                netloc = parsed_url.netloc
                # For file URLs, netloc will be empty, so handle that case
                if not netloc and parsed_url.path:
                    # For file URLs, extract the filename from the path
                    basename = os.path.basename(parsed_url.path)
                    name_without_ext = os.path.splitext(basename)[0]
                else:
                    # For web URLs, remove 'www.' prefix if present
                    if netloc.startswith("www."):
                        netloc = netloc[4:]
                    name_without_ext = netloc

                output_file = os.path.join(
                    output_dir, f"{name_without_ext}.json"
                )

                # Save extracted content
                with open(output_file, "w", encoding="utf-8") as f:
                    if isinstance(result.extracted_content, str):
                        f.write(result.extracted_content)
                    else:
                        json.dump(
                            result.extracted_content, f, indent=2, ensure_ascii=False
                        )

                print(f"Extracted data saved to {output_file}")
                extracted_data.append(result.extracted_content)
            else:
                error_msg = getattr(result, "error_message", "Unknown error")
                print(f"No content extracted from {file_path}: {error_msg}")

        # Show usage stats
        llm_strategy.show_usage()


async def check_and_reprocess_error_files(output_dir, input_dir, ext, llm_strategy):
    """
    Check for files with errors in the output directory and reprocess them.
    
    Args:
        output_dir (str): Directory containing the extracted JSON files
        input_dir (str): Directory containing the original source files
        ext (str): File extension of the original files (e.g., ".md")
        llm_strategy (LLMExtractionStrategy): The language model strategy to use for extraction
    
    Returns:
        int: Number of files reprocessed
    """
    print(f"Checking for files with errors in {output_dir}...")
    
    # List to store files that need reprocessing
    files_to_reprocess = []
    
    # Iterate through JSON files in the output directory
    for json_file in os.listdir(output_dir):
        if not json_file.endswith("_extracted.json"):
            continue
        
        json_path = os.path.join(output_dir, json_file)
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Check if the file contains an error
                has_error = False
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict) and data[0].get('error') is True:
                        has_error = True
                elif isinstance(data, dict) and data.get('error') is True:
                    has_error = True
                
                if has_error:
                    # Extract the original filename from the JSON filename
                    original_name = json_file.replace('_extracted.json', ext)
                    
                    # Look for the original file in the input directory and subdirectories
                    original_files = []
                    for root, _, files in os.walk(input_dir):
                        for file in files:
                            if file == original_name:
                                original_files.append(os.path.join(root, file))
                    
                    if original_files:
                        # Use the first matching file if multiple exist
                        files_to_reprocess.append(original_files[0])
                        print(f"Found error in {json_file}, will reprocess {original_files[0]}")
                    else:
                        print(f"Error in {json_file}, but couldn't find original file {original_name}")
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
    
    # Reprocess the files with errors
    if files_to_reprocess:
        print(f"Reprocessing {len(files_to_reprocess)} files with errors...")
        await process_files(files_to_reprocess, llm_strategy, output_dir)
        return len(files_to_reprocess)
    else:
        print("No files with errors found")
        return 0


async def main():
    parser = argparse.ArgumentParser(
        description="Extract data from markdown files using LLM"
    )
    parser.add_argument("input", help="Input file or directory path")
    parser.add_argument(
        "--output",
        "-o",
        help="Output directory for extracted data",
        default="llm_extracted_data",
    )
    parser.add_argument(
        "--ext", "-e", help="File extension to process (default: .md)", default=".html"
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Limit number of files to process (default: all)",
        default=None,
    )
    parser.add_argument(
        "--only-recheck",
        help="Only recheck files with errors in the output directory",
        default=False,
    )

    args = parser.parse_args()

    # Ensure output directory exists
    output_dir = ensure_output_directory(args.output)

    # Define LLM strategy once
    temperature = 0.7
    max_tokens = 1000
    llm_strategy = LLMExtractionStrategy(
        llm_config=LLMConfig(
            # provider="openai/gpt-4o-mini",
            provider="bedrock/amazon.nova-pro-v1:0",
        ),
        extraction_type="schema",
        schema=Sachanlagen.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=4096,
        overlap_rate=0.1,
        input_format="html",
        apply_chunking=False,
        extra_args={"temperature": temperature, "max_tokens": max_tokens},
        # verbose=True,
    )

    # Prepare list of files to process
    files_to_process = []

    # Check if input is a file or directory
    if os.path.isfile(args.input):
        files_to_process = [args.input]
    elif os.path.isdir(args.input):
        # Get all files with the specified extension in the directory
        for root, _, files in os.walk(args.input):
            for file in files:
                if file.endswith(args.ext):
                    files_to_process.append(os.path.join(root, file))

        if not files_to_process:
            print(f"No {args.ext} files found in {args.input}")
            return

        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            files_to_process = files_to_process[: args.limit]

        print(f"Processing {len(files_to_process)} files...")
    else:
        print(f"Error: {args.input} is not a valid file or directory")
        return

    # Check for and reprocess files with errors
    if os.path.isdir(args.input):
        input_dir = args.input
    else:
        input_dir = os.path.dirname(args.input)
        
    await process_files(files_to_process, llm_strategy, output_dir)
    """    # If --only-recheck is specified, skip the initial processing
    if args.only_recheck:
        print("Only rechecking files with errors, skipping initial processing.")
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy)
    else:
        # Process all files and do error checking
        await process_files(files_to_process, llm_strategy, output_dir)
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy) """
        


if __name__ == "__main__":
    asyncio.run(main())
