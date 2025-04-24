import argparse
import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional  # Added Any, Optional
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field


# Configure logging
def setup_logging(log_level: str = "INFO"):
    """Sets up the basic logging configuration with a configurable log level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # Set log level for LiteLLM and Botocore
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)


class Company(BaseModel):
    company_name: str = Field(..., description="Name des Unternehmens.")
    company_url: str = Field(..., description="URL des Unternehmens.")
    products: List[str] = Field(
        ...,
        description="Produkte, die das Unternehmen vertreibt.(in Pluralform)",
    )
    machines: List[str] = Field(
        ...,
        description="(Optional)Maschinen, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
    )
    process_type: List[str] = Field(
        ...,
        description="(Optional)Produktionsprozesse, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
    )
    lohnfertigung: bool = Field(
        ..., description="Ob das Unternehmen Lohnfertigung anbietet"
    )


def load_prompt(file_path: str) -> str:
    """
    Load the prompt from a file.

    Args:
        file_path (str): The path to the prompt file.

    Returns:
        str: The prompt loaded from the file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logging.error(f"Prompt file not found: {file_path}")
        return ""
    except IOError as e:
        logging.error(f"Error reading prompt file {file_path}: {e}")
        return ""

prompt = load_prompt("prompts/extract_company_products.md")


def ensure_output_directory(directory="llm_extracted_data") -> str:
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


dispatcher = MemoryAdaptiveDispatcher(
    memory_threshold_percent=70.0,
    check_interval=2.0,
    max_session_permit=3,
)
rate_limiter = RateLimiter(
    base_delay=(30, 60), max_delay=60, max_retries=3, rate_limit_codes=[429, 503]
)


def _get_output_filename(file_path: str, output_dir: str) -> str:
    """Generates the output JSON filename based on the input file path."""
    basename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(basename)[0]
    return os.path.join(output_dir, f"{name_without_ext}_extracted.json")


def _filter_files_to_process(file_paths: List[str], output_dir: str, overwrite: bool) -> List[str]:
    """Filters the list of file paths based on existing output files and overwrite flag."""
    if overwrite:
        return file_paths

    filtered_file_paths = []
    for path in file_paths:
        output_file = _get_output_filename(path, output_dir)
        if os.path.exists(output_file):
            logger.debug(f"Skipping {path} as output already exists at {output_file}")
            continue
        filtered_file_paths.append(path)

    if not filtered_file_paths and file_paths: # Check if initial list was not empty
        logger.info("All files already have output files. Use --overwrite to reprocess.")

    skipped_count = len(file_paths) - len(filtered_file_paths)
    logger.debug(f"file path {len(file_paths)} | filtered {len(filtered_file_paths)}")
    logger.info(f"Skipped {skipped_count} files as output already exists. Use --overwrite to reprocess.")
    return filtered_file_paths


def _save_result(result_content: Any, output_dir: str, source_url: str):
    """Saves the extracted content to a JSON file."""
    parsed_url = urlparse(source_url)
    netloc = parsed_url.netloc

    if not netloc and parsed_url.path: # Handle file URLs
        basename = os.path.basename(parsed_url.path)
        name_without_ext = os.path.splitext(basename)[0]
    elif netloc: # Handle web URLs
        if netloc.startswith("www."):
            netloc = netloc[4:]
        name_without_ext = netloc
    else: # Fallback if URL parsing fails unexpectedly
        logger.warning(f"Could not determine filename from URL: {source_url}. Using fallback.")
        # Create a fallback name, e.g., based on hash or timestamp if needed
        # For now, let's just use a generic name, but this might cause collisions
        name_without_ext = f"unknown_source_{hash(source_url)}"

    output_file = os.path.join(output_dir, f"{name_without_ext}_extracted.json")

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            if isinstance(result_content, str):
                # Attempt to parse string as JSON, otherwise write as string
                try:
                    parsed_json = json.loads(result_content)
                    json.dump(parsed_json, f, indent=2, ensure_ascii=False)
                except json.JSONDecodeError:
                    f.write(result_content) # Write as plain string if not valid JSON
            else:
                # Assume it's already a dict/list suitable for JSON
                json.dump(result_content, f, indent=2, ensure_ascii=False)
        logger.info(f"Extracted data saved to {output_file}")
    except IOError as e:
        logger.error(f"Failed to write output file {output_file}: {e}")
    except TypeError as e:
         logger.error(f"Failed to serialize result to JSON for {output_file}: {e}")


def _is_relevant_extraction(content: Any) -> bool:
    """
    Returns True if at least one of 'products', 'machines', or 'process_type' is a non-empty list.
    """
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            return False
    if isinstance(content, list) and content:
        for entry in content:
            if isinstance(entry, dict):
                for field in ["products", "machines", "process_type"]:
                    field_value = entry.get(field)
                    if isinstance(field_value, list) and field_value:
                        return True
        return False
    return False


async def process_files(file_paths: List[str], llm_strategy: LLMExtractionStrategy, output_dir: str, overwrite: bool = False) -> List[Dict]:
    """
    Process one or more files using a specified LLM extraction strategy and save the results.

    Args:
        file_paths (list of str): List of file paths to be processed.
        llm_strategy (LLMExtractionStrategy): The language model strategy to use for extraction.
        output_dir (str): Directory where the extracted data and combined results will be saved.
        overwrite (bool, optional): Whether to overwrite existing output files. Defaults to False.

    Returns:
        List[Dict]: A list of extracted content (as dictionaries) from each file.
    """

    # Filter files first
    actual_files_to_process = _filter_files_to_process(file_paths, output_dir, overwrite)

    if not actual_files_to_process:
        return [] # Return early if no files need processing

    # Convert file paths to URLs with file:// protocol
    file_urls = [f"file://{os.path.abspath(path)}" for path in actual_files_to_process]

    logger.info(f"Processing {len(actual_files_to_process)} files...")

    config = CrawlerRunConfig(
        cache_mode=CacheMode.WRITE_ONLY,
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
        # Use actual_files_to_process for indexing results correctly
        for idx, result in enumerate(results): # type: ignore
            original_file_path = actual_files_to_process[idx]
            if result.success and result.extracted_content:
                logger.debug(f"Extracted content for {original_file_path}: {result.extracted_content}")
                is_relevant = _is_relevant_extraction(result.extracted_content)
                if is_relevant:
                    _save_result(result.extracted_content, output_dir, result.url)
                    if isinstance(result.extracted_content, str):
                        try:
                            extracted_data.append(json.loads(result.extracted_content))
                        except json.JSONDecodeError:
                            extracted_data.append({"raw_content": result.extracted_content})
                    else:
                        extracted_data.append(result.extracted_content)
                else:
                    logger.info(f"Skipping output for {original_file_path} as extracted content is empty or irrelevant.")
            else:
                logger.info(f"Skipping error output for {original_file_path} as input is irrelevant or empty.")

        # Show usage stats
        llm_strategy.show_usage()
        return extracted_data


def _find_original_file(error_json_file: str, input_dir: str, ext: str) -> Optional[str]:
    """Finds the original source file corresponding to an error JSON file.

    Args:
        error_json_file (str): The path to the JSON file indicating an error.
        input_dir (str): The directory containing the original source files.
        ext (str): The file extension of the original files.

    Returns:
        Optional[str]: The path to the original file, or None if not found.
    """
    original_name = os.path.basename(error_json_file).replace('_extracted.json', ext)
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file == original_name:
                return os.path.join(root, file)
    logger.warning(f"Could not find original file {original_name} for error file {error_json_file}")
    return None


def _find_error_files(output_dir: str) -> List[str]:
    """Scans the output directory for JSON files indicating processing errors.

    Args:
        output_dir (str): The directory containing the output JSON files.

    Returns:
        List[str]: A list of paths to JSON files that indicate errors.
    """
    error_files = []
    if not os.path.isdir(output_dir):
        logger.error(f"Output directory {output_dir} not found.")
        return error_files

    for json_file in os.listdir(output_dir):
        if not json_file.endswith("_extracted.json"):
            continue

        json_path = os.path.join(output_dir, json_file)
        if not os.path.isfile(json_path):
            continue

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            has_error = False
            if isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], dict) and data[0].get('error') is True:
                    has_error = True
            elif isinstance(data, dict) and data.get('error') is True:
                has_error = True

            if has_error:
                error_files.append(json_path)
        except json.JSONDecodeError:
            logger.warning(f"Could not decode JSON from {json_path}. It might indicate an incomplete process or error.")
            # Optionally treat decode errors as files needing reprocessing
            # error_files.append(json_path)
        except Exception as e:
            logger.error(f"Error reading or processing {json_path}: {e}")

    return error_files


async def check_and_reprocess_error_files(output_dir: str, input_dir: str, ext: str, llm_strategy: LLMExtractionStrategy) -> int:
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

    error_json_files = _find_error_files(output_dir)

    files_to_reprocess = []
    for error_file_path in error_json_files:
        original_file = _find_original_file(error_file_path, input_dir, ext)
        if original_file:
            files_to_reprocess.append(original_file)
            logger.info(f"Found error marker in {os.path.basename(error_file_path)}, will reprocess {original_file}")
        # else: The warning is logged inside _find_original_file

    # Reprocess the files with errors
    if files_to_reprocess:
        logger.info(f"Reprocessing {len(files_to_reprocess)} files with errors...")
        # Always overwrite error files
        await process_files(files_to_reprocess, llm_strategy, output_dir, overwrite=True)
        return len(files_to_reprocess)
    else:
        logger.info("No files with errors found needing reprocessing.")
        return 0


def run_extract_llm(
    input_path: str,
    output_dir: str = "llm_extracted_data",
    ext: str = ".md",
    limit: Optional[int] = None,
    only_recheck: bool = False,
    overwrite: bool = False,
    log_level: str = "INFO",
    llm_strategy: Optional[LLMExtractionStrategy] = None,
) -> str:
    """
    Run the LLM extraction process programmatically.

    Args:
        input_path (str): Input file or directory path.
        output_dir (str): Output directory for extracted data.
        ext (str): File extension to process (default: .md).
        limit (Optional[int]): Limit number of files to process (default: all).
        only_recheck (bool): Only recheck files with errors in the output directory.
        overwrite (bool): Overwrite existing output files instead of skipping them.
        log_level (str): Set the logging level (default: INFO).
        llm_strategy (Optional[LLMExtractionStrategy]): Custom LLM extraction strategy to use. If None, a default is created.

    Returns:
        str: The output directory path where results are stored.
    """
    setup_logging(log_level)
    global logger
    logger = logging.getLogger(__name__)

    if output_dir == "llm_extracted_data":
        logger.warning("Output directory is set to default 'llm_extracted_data'.")

    output_dir = ensure_output_directory(output_dir)

    if llm_strategy is None:
        temperature = 0.7
        max_tokens = 1000
        llm_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="bedrock/amazon.nova-pro-v1:0",
            ),
            extraction_type="schema",
            schema=Company.model_json_schema(),
            instruction=prompt,
            chunk_token_threshold=4096,
            overlap_rate=0.1,
            input_format="markdown",
            apply_chunking=False,
            extra_args={"temperature": temperature, "max_tokens": max_tokens},
        )

    files_to_process = []
    if os.path.isfile(input_path):
        files_to_process = [input_path]
    elif os.path.isdir(input_path):
        for root, _, files in os.walk(input_path):
            for file in files:
                if file.endswith(ext):
                    files_to_process.append(os.path.join(root, file))
    else:
        logger.error(f"Error: {input_path} is not a valid file or directory")
        raise FileNotFoundError(f"Input path '{input_path}' does not exist or is not a valid file/directory.")

    if not files_to_process:
        logger.warning(f"No {ext} files found in {input_path}")
        return output_dir

    if limit is not None and limit > 0:
        files_to_process = files_to_process[:limit]

    logger.info(f"Found {len(files_to_process)} files to potentially process...")

    if os.path.isdir(input_path):
        input_dir = input_path
    else:
        input_dir = os.path.dirname(input_path)

    async def _run():
        if only_recheck:
            logger.info("Only rechecking files with errors, skipping initial processing.")
            await check_and_reprocess_error_files(output_dir, input_dir, ext, llm_strategy)
        else:
            await process_files(files_to_process, llm_strategy, output_dir, overwrite)
            await check_and_reprocess_error_files(output_dir, input_dir, ext, llm_strategy)

    asyncio.run(_run())
    return output_dir


async def main():
    parser = argparse.ArgumentParser(
        description="Extract data from markdown files (domain_content_*) using LLM"
    )
    parser.add_argument("input", help="Input file or directory path")
    parser.add_argument(
        "--output",
        "-o",
        help="Output directory for extracted data",
        default="llm_extracted_data",
    )
    parser.add_argument(
        "--ext", "-e", help="File extension to process (default: .md)", default=".md"
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
        action="store_true",
        help="Only recheck files with errors in the output directory",
    )
    parser.add_argument(
        "--overwrite",
        "-w",
        action="store_true",
        help="Overwrite existing output files instead of skipping them",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )

    args = parser.parse_args()

    setup_logging(args.log_level)
    global logger
    logger = logging.getLogger(__name__)

    if args.output == "llm_extracted_data":
        logger.warning("Output directory is set to default 'llm_extracted_data'.")

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
        schema=Company.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=4096,
        overlap_rate=0.1,
        input_format="markdown",
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
            # Do not create any output files if no relevant input
            return

        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            files_to_process = files_to_process[: args.limit]

        logger.info(f"Found {len(files_to_process)} files to potentially process...")
    else:
        logger.error(f"Error: {args.input} is not a valid file or directory")
        raise FileNotFoundError(f"Input path '{args.input}' does not exist or is not a valid file/directory.")

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
        await process_files(files_to_process, llm_strategy, output_dir, args.overwrite)
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy)

    # Print the output directory path for downstream use
    print(output_dir)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--run-extract-llm":
        # For direct function call testing
        # Example: python extract_llm.py --run-extract-llm input_dir output_dir
        _, _, input_path, output_dir = sys.argv
        print(run_extract_llm(input_path, output_dir))
    else:
        asyncio.run(main())
