import os
import json
import asyncio
import logging
from pydantic import BaseModel, Field, RootModel
from typing import List
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig
import argparse
import re
import csv
from decimal import Decimal

# Setup logging
logger = logging.getLogger(__name__)

def configure_logging(log_level=logging.INFO):
    """Configure logging with the specified verbosity level"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler()
        ]
    )
    # Set log level for other libraries to reduce noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    # Set log level for HTTPx, which is used by AsyncWebCrawler
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    # Set log level for LiteLLM and Botocore
    logging.getLogger('LiteLLM').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    # List all active loggers at configuration time
    #active_loggers = [name for name in logging.root.manager.loggerDict]
    #logger.info("Active loggers in the program: %s", active_loggers)
    logger.debug("Logging configured with level: %s", 
                 logging.getLevelName(log_level))


class SachanlagenValues(BaseModel):
    """Model for Sachanlagen values from different years"""
    values: dict[str, str] = Field(
        description="Dictionary of Sachanlagen values, keys are in format 'Sachanlagen_[number]' and values are monetary amounts as strings"
    )
    table_name: str = Field(
        description="Name of the table containing the Sachanlagen values"
    )
    is_Teuro: bool = Field(
        description="does one or more column contain the Thousand Euro sign"
    )


class Sachanlagen(RootModel):
    """Container for all Sachanlagen values extracted from the document"""
    root: List[SachanlagenValues] = Field(
        default=[],
        description="List of table entries containing Sachanlagen values"
    )

prompt = """
Extract the "Sachanlagen" items from tables found in this German financial statement HTML files and identify the table names or headings associated with them.

Review the HTML content to locate all tables and preceding headings that may denote the table name. For each identified table, extract "Sachanlagen" items and their corresponding values.

# Steps

1. **Parse the HTML**: Open and parse the HTML file to locate tables and headings.
2. **Identify Table Names**: First look within the table header. if not found then look at the preceding heading.the table name from the table header could either be from 
  "Aktiva",
  "Passiva",
  "Anlagevermögen",
  "Umlaufvermögen",
  "Rechnungsabgrenzungsposten",
  "Aktive latente Steuern",
  "Aktiver Unterschiedsbetrag aus der Vermögensverrechnung",
  "Immaterielle Vermögensgegenstände",
  "Sachanlagen",
  "Finanzanlagen",
  "Vorräte",
  "Forderungen",
  "Wertpapiere",
  "Kassenbestand und Bankguthaben",
  "Eigenkapital",
  "Gezeichnetes Kapital",
  "Kapitalrücklage",
  "Gewinnrücklagen",
  "Bilanzgewinn oder Bilanzverlust",
  "Rückstellungen",
  "Verbindlichkeiten",
  "Passive Rechnungsabgrenzungsposten",
  "Passive latente Steuern",
  "Anschaffungs- oder Herstellungskosten",
  "Zugänge",
  "Abgänge",
  "Umbuchungen",
  "Abschreibungen",
  "Außerplanmäßige Abschreibung",
  "Buchwert",
  "Nutzungsdauer",
  "Geleistete Anzahlungen",
  "Anlagen im Bau".
3. **Identify the T€ mark (is_Teuro)**:  look at the table column header and check if one of the column header contains the T€ mark. If yes, then set the is_Teuro to True. Otherwise, set it to False.
4. **Extract "Sachanlagen" Values**: For each table, extract items labeled "Sachanlagen" with their corresponding values. there should be at least 2 values from 2 different year
5. **Organize Data**: Structure the extracted data into JSON format, listing the table names and corresponding "Sachanlagen" values.
6. **Output Result**: Present the data as a JSON array with the specified structure.

# Output Format

The output should be a JSON array with the following structure:
```json
[
  {
    "table_name": "TableName",
    "values": {
      "Sachanlagen_1": "Value1",
      "Sachanlagen_2": "Value2"
    },
    "is_Teuro": true/false
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
    "is_Teuro": true
  }
]
```

**Note:** 
- The number of values under "values" can vary based on the number of "Sachanlagen" found in each table.
- make sure to not take the numbers from the sub item of the Sachanlagen like Technische Anlagen und Maschinen,Grundstücke, Andere Anlagen, etc.
"""

def extract_company_name(file_path):
    """
    Extract company name from HTML comment at the beginning of the file.
    The comment format should be: <!--original_filename: Company Name-->
    
    Args:
        file_path (str): Path to the HTML file
        
    Returns:
        str: Extracted company name or empty string if not found
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read first few lines of the file to look for the comment
            # Reading more than just first line in case there are other comments/tags before it
            content = ''.join(f.readline() for _ in range(10))
            
        # Look for the comment pattern
        import re
        match = re.search(r'<!--\s*original_filename:\s*([^>]+?)\s*-->', content)
        if match:
            logger.debug(f"(extract_company_name) Extracted company name from {file_path}: {match.group(1).strip()}")
            return match.group(1).strip()
    except Exception as e:
        logger.error(f"Error extracting company name from {file_path}: {e}")
    
    return ""

def extract_category_from_input_path(input_path):
    """
    Extract category from input path if it contains 'bundesanzeiger_local_'
    
    Args:
        input_path (str): Input directory or file path
        
    Returns:
        str: Extracted category or empty string if pattern not found
    """
    # Remove any trailing slashes
    input_path = input_path.rstrip('/')

    # If the input is a file, use its directory
    if os.path.isfile(input_path):
        input_path = os.path.dirname(input_path)
    
    # Extract the basename of the directory
    basename = os.path.basename(input_path)
    
    # Check if the name matches the pattern
    match = re.match(r'bundesanzeiger_local_([^_]+)(?:_output)?$', basename)
    if match:
        category = match.group(1)
        logger.info(f"Detected category from input path: {category}")
        return category
    
    return ""

def ensure_output_directory(directory="llm_extracted_data"):
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


dispatcher = MemoryAdaptiveDispatcher(
    memory_threshold_percent=70.0,
    check_interval=1.0,
    max_session_permit=5,
)
rate_limiter = RateLimiter(
    base_delay=(10, 60), max_delay=60, max_retries=3, rate_limit_codes=[429, 503]
)


async def process_files(file_paths, llm_strategy, output_dir):
    """
    Process one or more files using a specified LLM extraction strategy and save the results.
    Uses streaming mode to process results as they become available.
    """
    # Convert file paths to URLs with file:// protocol
    file_urls = [f"file://{os.path.abspath(path)}" for path in file_paths]
    
    logger.info(f"Processing {len(file_paths)} files using streaming mode")
    
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=llm_strategy,
        stream=True  # Enable streaming mode
    )

    extracted_data = []
    processed_count = 0

    async with AsyncWebCrawler() as crawler:
        # Use streaming mode to process files as they complete
        async for result in await crawler.arun_many(
            urls=file_urls,
            config=config,
            dispatcher=dispatcher,
            rate_limiter=rate_limiter,
        ):
            processed_count += 1
            
            # Process result as it comes in
            if result.success and result.extracted_content:
                # Find the corresponding file path
                file_idx = file_urls.index(result.url)
                file_path = file_paths[file_idx]
                
                # Extract company name from HTML comment
                company_name = extract_company_name(file_path)
                
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
                    logger.debug(f"File URL: extracted name '{name_without_ext}' from path '{parsed_url.path}'")
                else:
                    # For web URLs, remove 'www.' prefix if present
                    if netloc.startswith("www."):
                        netloc = netloc[4:]
                    name_without_ext = netloc
                    logger.debug(f"Web URL: using netloc '{netloc}' as name")

                output_file = os.path.join(
                    output_dir, f"{name_without_ext}.json"
                )

                # Add company_name to each entry in the extracted content
                try:
                    # Parse the extracted content if it's a string
                    content_to_modify = result.extracted_content
                    if isinstance(content_to_modify, str):
                        content_to_modify = json.loads(content_to_modify)
                    
                    # Add company name to each entry
                    if isinstance(content_to_modify, list):
                        for entry in content_to_modify:
                            if isinstance(entry, dict):
                                entry["company_name"] = company_name
                    elif isinstance(content_to_modify, dict):
                        content_to_modify["company_name"] = company_name
                    
                    # Update the result.extracted_content with the modified content
                    result.extracted_content = content_to_modify
                    logger.debug(f"Added company name '{company_name}' to content")
                except json.JSONDecodeError as e:
                    logger.warning(f"Could not parse extracted_content as JSON: {e}")
                except KeyError as e:
                    logger.warning(f"Missing expected key when adding company name: {e}")
                except Exception as e:
                    logger.warning(f"Error adding company name to content: {e}")
                
                # Save extracted content
                with open(output_file, "w", encoding="utf-8") as f:
                    if isinstance(result.extracted_content, str):
                        f.write(result.extracted_content)
                    else:
                        json.dump(
                            result.extracted_content, f, indent=2, ensure_ascii=False
                        )

                logger.info(f"[{processed_count}/{len(file_paths)}] Extracted data for '{company_name}' saved to {output_file}")
                extracted_data.append(result.extracted_content)
            else:
                error_msg = getattr(result, "error_message", "Unknown error")
                logger.warning(f"[{processed_count}/{len(file_paths)}] No content extracted: {error_msg}")

            # Periodically log progress
            if processed_count % 5 == 0:
                logger.info(f"Processed {processed_count}/{len(file_paths)} files")
        
        # Show usage stats
        usage_stats = llm_strategy.show_usage()
        logger.info(f"LLM usage stats: {usage_stats}")
        return extracted_data


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
    logger.info(f"Checking for files with errors in {output_dir}...")
    
    # List to store files that need reprocessing
    files_to_reprocess = []
    
    # Iterate through JSON files in the output directory
    for json_file in os.listdir(output_dir):
        if not json_file.endswith(".json"):
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
                    # Extract the original filename from the JSON filename - properly remove the .json extension
                    base_name = os.path.splitext(json_file)[0]  # Get filename without .json extension
                    original_name = base_name + ext  # Add the correct extension
                    
                    # Look for the original file in the input directory and subdirectories
                    original_files = []
                    for root, _, files in os.walk(input_dir):
                        for file in files:
                            if file == original_name:
                                original_files.append(os.path.join(root, file))
                    
                    if original_files:
                        # Use the first matching file if multiple exist
                        files_to_reprocess.append(original_files[0])
                        logger.info(f"Found error in {json_file}, will reprocess {original_files[0]}")
                    else:
                        logger.warning(f"Error in {json_file}, but couldn't find original file {original_name}")
        except Exception as e:
            logger.error(f"Error reading {json_file}: {e}")
    
    # Reprocess the files with errors
    if files_to_reprocess:
        logger.info(f"Reprocessing {len(files_to_reprocess)} files with errors...")
        await process_files(files_to_reprocess, llm_strategy, output_dir)
        return len(files_to_reprocess)
    else:
        logger.info("No files with errors found")
        return 0


def process_sachanlagen_output(output_dir):
    """
    Process all extracted Sachanlagen JSON files in the output directory.
    For each company, find the 'Aktiva' table and extract the largest Sachanlagen value.
    Generate a CSV report with company names and their Sachanlagen values.
    
    Args:
        output_dir (str): Directory containing the extracted JSON files
        
    Returns:
        str: Path to the generated CSV file
    """
    logger.info(f"Processing extracted Sachanlagen data in {output_dir}...")
    
    # Prepare data structure for CSV
    csv_data = []
    total_files = 0
    success_count = 0
    aktiva_table_count = 0
    fallback_single_table_count = 0
    no_table_count = 0
    
    # German number format conversion utility function
    def convert_german_number(num_str, file_name=None):
        if not num_str:
            return 0
        try:
            # First, remove all characters except numbers, commas, periods, and minus sign
            import re
            cleaned = re.sub(r'[^\d.,\-]', '', num_str)
            
            # Handle parentheses that indicate negative numbers (e.g., "(18.394)")
            if num_str.strip().startswith('(') and num_str.strip().endswith(')'):
                cleaned = '-' + cleaned
                
            # Replace German decimal comma with dot and remove thousand separators
            decimal_str = cleaned.replace('.', '').replace(',', '.')
            
            # Convert to Decimal for precision
            return Decimal(decimal_str)
        except Exception as e:
            file_info = f" in file '{file_name}'" if file_name else ""
            logger.warning(f"Failed to convert number '{num_str}'{file_info}: {[type(e)]}")
            return Decimal('0')
    
    # Process each JSON file in the output directory
    for json_file in os.listdir(output_dir):
        if not json_file.endswith('.json'):
            continue
            
        total_files += 1
        json_path = os.path.join(output_dir, json_file)
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Get company name from data or filename
            company_name = None
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                company_name = data[0].get('company_name')
            
            if not company_name:
                # Extract company name from filename by removing _cleaned suffix
                company_name = os.path.splitext(json_file)[0]
                if company_name.endswith('_cleaned'):
                    company_name = company_name[:-8]  # Remove '_cleaned' suffix
            
            # Find Aktiva table
            aktiva_tables = []
            for item in data:
                if isinstance(item, dict) and 'table_name' in item:
                    if item['table_name'].lower() == 'aktiva':
                        aktiva_tables.append(item)
            
            if not aktiva_tables:
                # Fallback: If no Aktiva table but exactly one table exists, use that one | this strategy has 70% accuracy
                if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict) and 'table_name' in data[0]:
                    aktiva_tables = data
                    fallback_single_table_count += 1
                    logger.info(f"No 'Aktiva' table found in {json_file}, but using the only table: '{data[0]['table_name']}'")
                else:
                    no_table_count += 1
                    logger.warning(f"No 'Aktiva' table found in {json_file}")
                    continue
            else:
                aktiva_table_count += 1
                
            success_count += 1
            
            # Find largest Sachanlagen value across all years
            largest_value = Decimal('0')
            largest_table = None
            
            for table in aktiva_tables:
                values = table.get('values', {})
                is_teuro = table.get('is_Teuro', False)
                
                for key, value_str in values.items():
                    if key.startswith('Sachanlagen'):
                        value = convert_german_number(value_str, json_file)
                        
                        # Handle Teuro conversion if needed
                        if is_teuro and value < 50000:
                            original_value = value
                            value *= 1000
                            logger.info(f"Teuro conversion in {json_file}, table '{table['table_name']}': "
                                        f"Original value {original_value} converted to {value}")
                        
                        if value > largest_value:
                            largest_value = value
                            largest_table = table
            
            # Add to CSV data - floor the Sachanlagen value to the nearest whole number
            csv_data.append({
                'company_name': company_name,
                'sachanlagen': str(int(largest_value)),  # Convert to integer to floor the value
                'table_name': largest_table['table_name'] if largest_table else 'N/A',
                'is_teuro': str(largest_table.get('is_Teuro', False)) if largest_table else 'False'
            })
            
        except Exception as e:
            logger.error(f"Error processing {json_file}: {e}")
    
    # Generate CSV output filename based on output directory name
    csv_filename = f"{os.path.basename(output_dir)}.csv"
    csv_path = os.path.join(os.path.dirname(output_dir), csv_filename)
    
    # Write CSV file
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['company_name', 'sachanlagen', 'table_name', 'is_teuro']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)
    
    logger.info(f"CSV report generated: {csv_path}")
    logger.info(f"Processing summary: {success_count} of {total_files} files had usable tables")
    logger.info(f"  - {aktiva_table_count} files had 'Aktiva' tables")
    logger.info(f"  - {fallback_single_table_count} files had no 'Aktiva' table but used a single available table")
    logger.info(f"  - {no_table_count} files had no usable tables")
    
    return csv_path


async def main():
    parser = argparse.ArgumentParser(
        description="Extract data from markdown files using LLM"
    )
    parser.add_argument("input", help="Input file or directory path")
    parser.add_argument(
        "--output",
        "-o",
        help="Output directory for extracted data",
        default=None,
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
    parser.add_argument(
        "--only-process",
        action="store_true",
        help="Only process existing output directory to generate CSV summary (skip extraction)",
        default=False,
    )
    parser.add_argument(
        "--log-level",
        help="Set the logging level (default: INFO)",
        default="INFO",
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))

    # Determine output directory
    output_dir = args.output
    if output_dir is None:
        # If no output directory is specified, try to extract category from input path
        category = extract_category_from_input_path(args.input)
        if category:
            output_dir = f"sachanlagen_{category}"
        else:
            output_dir = "sachanlagen_default"
        logger.info(f"Output directory automatically set to: {output_dir}")

    # Check if the input path exists
    if not os.path.exists(args.input):
        # If --only-process is set and the output directory exists, we'll proceed
        # even if the input path doesn't exist
        if args.only_process and os.path.exists(output_dir):
            logger.info(f"Input path {args.input} not found, but proceeding with --only-process using {output_dir}")
        else:
            logger.error(f"Error: {args.input} does not exist")
            return

    # Ensure output directory exists
    output_dir = ensure_output_directory(output_dir)

    # If only processing is requested, just run the process_sachanlagen_output function and exit
    if args.only_process:
        logger.info(f"Only processing existing output in {output_dir}, skipping extraction phase")
        csv_path = process_sachanlagen_output(output_dir)
        logger.info(f"Sachanlagen data processing complete. CSV report available at: {csv_path}")
        return

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
            logger.warning(f"No {args.ext} files found in {args.input}")
            return

        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            files_to_process = files_to_process[: args.limit]

        logger.info(f"Processing {len(files_to_process)} files...")
    else:
        logger.error(f"Error: {args.input} is not a valid file or directory")
        return

    # Check for and reprocess files with errors
    if os.path.isdir(args.input):
        input_dir = args.input
    else:
        input_dir = os.path.dirname(args.input)
        
    
    # If --only-recheck is specified, skip the initial processing
    if args.only_recheck:
        logger.info("Only rechecking files with errors, skipping initial processing.")
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy)
    else:
        # Process all files and do error checking
        await process_files(files_to_process, llm_strategy, output_dir)
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy)
    
    # Process the extracted data and generate CSV summary
    csv_path = process_sachanlagen_output(output_dir)
    logger.info(f"Sachanlagen data processing complete. CSV report available at: {csv_path}")
        
if __name__ == "__main__":
    asyncio.run(main())



