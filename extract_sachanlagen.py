import os
import json
import asyncio
import logging
from pydantic import BaseModel, Field, RootModel
from typing import List, Protocol, Dict, Any, Optional, Callable, TypeVar, Union, AsyncIterator
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig
import argparse
import re
import csv
from decimal import Decimal
from dataclasses import dataclass

# Setup logging
logger = logging.getLogger(__name__)

# Define interfaces/protocols for dependency injection
class LoggingService(Protocol):
    """Protocol for logging service"""
    def configure(self, log_level: int) -> None: ...
    def info(self, msg: str, *args, **kwargs) -> None: ...
    def warning(self, msg: str, *args, **kwargs) -> None: ...
    def error(self, msg: str, *args, **kwargs) -> None: ...
    def debug(self, msg: str, *args, **kwargs) -> None: ...

class FileService(Protocol):
    """Protocol for file operations"""
    def ensure_directory(self, directory: str) -> str: ...
    def list_files(self, directory: str, extension: str = None) -> List[str]: ...
    def read_file(self, file_path: str, encoding: str = "utf-8") -> str: ...
    def write_file(self, file_path: str, content: Union[str, dict, list], is_json: bool = False) -> None: ...
    def file_exists(self, file_path: str) -> bool: ...
    def extract_company_name(self, file_path: str) -> str: ...
    def extract_category(self, input_path: str) -> str: ...
    def walk_directory(self, directory: str) -> List[tuple]: ...

class LLMStrategyFactory(Protocol):
    """Protocol for LLM strategy factory"""
    def create_llm_strategy(self, 
                          provider: str, 
                          schema: dict, 
                          instruction: str, 
                          temperature: float = 0.7, 
                          max_tokens: int = 1000) -> LLMExtractionStrategy: ...

class DispatcherFactory(Protocol):
    """Protocol for dispatcher factory"""
    def create_dispatcher(self) -> MemoryAdaptiveDispatcher: ...

class RateLimiterFactory(Protocol):
    """Protocol for rate limiter factory"""
    def create_rate_limiter(self) -> RateLimiter: ...

class WebCrawlerFactory(Protocol):
    """Protocol for web crawler factory"""
    async def create_crawler(self) -> AsyncWebCrawler: ...

class CsvWriter(Protocol):
    """Protocol for CSV writer"""
    def write_csv(self, data: List[Dict[str, Any]], fieldnames: List[str], csv_path: str) -> str: ...

# Implementations of the protocols
class DefaultLoggingService:
    """Default implementation of LoggingService"""
    def configure(self, log_level=logging.INFO):
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
        logging.getLogger('LiteLLM').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        
        logger.debug("Logging configured with level: %s", 
                    logging.getLevelName(log_level))
    
    def info(self, msg: str, *args, **kwargs) -> None:
        logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        logger.error(msg, *args, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        logger.debug(msg, *args, **kwargs)

class DefaultFileService:
    """Default implementation of FileService"""
    def __init__(self, logging_service: LoggingService):
        self.logging_service = logging_service

    def ensure_directory(self, directory: str) -> str:
        """Ensure the directory exists"""
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory
    
    def list_files(self, directory: str, extension: str = None) -> List[str]:
        """List files in directory with optional extension filter"""
        files = os.listdir(directory)
        if extension:
            files = [f for f in files if f.endswith(extension)]
        return files
    
    def read_file(self, file_path: str, encoding: str = "utf-8") -> str:
        """Read file contents"""
        with open(file_path, 'r', encoding=encoding) as f:
            return f.read()
    
    def write_file(self, file_path: str, content: Union[str, dict, list], is_json: bool = False) -> None:
        """Write content to file"""
        mode = 'w'
        with open(file_path, mode, encoding='utf-8') as f:
            if is_json:
                json.dump(content, f, indent=2, ensure_ascii=False)
            else:
                f.write(content)
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists"""
        return os.path.exists(file_path)
    
    def extract_company_name(self, file_path: str) -> str:
        """
        Extract company name from HTML comment at the beginning of the file.
        The comment format should be: <!--original_filename: Company Name-->
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read first few lines of the file to look for the comment
                content = ''.join(f.readline() for _ in range(10))
                
            # Look for the comment pattern
            match = re.search(r'<!--\s*original_filename:\s*([^>]+?)\s*-->', content)
            if match:
                self.logging_service.debug(f"Extracted company name from {file_path}: {match.group(1).strip()}")
                return match.group(1).strip()
        except Exception as e:
            self.logging_service.error(f"Error extracting company name from {file_path}: {e}")
        
        return ""
    
    def extract_category(self, input_path: str) -> str:
        """
        Extract category from input path if it contains 'bundesanzeiger_local_'
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
            self.logging_service.info(f"Detected category from input path: {category}")
            return category
        
        return ""
    
    def walk_directory(self, directory: str) -> List[tuple]:
        """Wrapper for os.walk"""
        return list(os.walk(directory))

class DefaultLLMStrategyFactory:
    """Default implementation of LLMStrategyFactory"""
    def create_llm_strategy(self, 
                          provider: str = "bedrock/amazon.nova-pro-v1:0", 
                          schema: dict = None, 
                          instruction: str = "", 
                          temperature: float = 0.7, 
                          max_tokens: int = 1000) -> LLMExtractionStrategy:
        """Create an LLM extraction strategy"""
        return LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider=provider,
            ),
            extraction_type="schema",
            schema=schema,
            instruction=instruction,
            chunk_token_threshold=4096,
            overlap_rate=0.1,
            input_format="html",
            apply_chunking=False,
            extra_args={"temperature": temperature, "max_tokens": max_tokens},
        )

class DefaultDispatcherFactory:
    """Default implementation of DispatcherFactory"""
    def create_dispatcher(self) -> MemoryAdaptiveDispatcher:
        """Create a memory adaptive dispatcher"""
        return MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=1.0,
            max_session_permit=5,
        )

class DefaultRateLimiterFactory:
    """Default implementation of RateLimiterFactory"""
    def create_rate_limiter(self) -> RateLimiter:
        """Create a rate limiter"""
        return RateLimiter(
            base_delay=(10, 60), 
            max_delay=60, 
            max_retries=3, 
            rate_limit_codes=[429, 503]
        )

class DefaultWebCrawlerFactory:
    """Default implementation of WebCrawlerFactory"""
    async def create_crawler(self) -> AsyncWebCrawler:
        """Create a web crawler"""
        return AsyncWebCrawler()

class DefaultCsvWriter:
    """Default implementation of CsvWriter"""
    def __init__(self, logging_service: LoggingService):
        self.logging_service = logging_service
    
    def write_csv(self, data: List[Dict[str, Any]], fieldnames: List[str], csv_path: str) -> str:
        """Write data to CSV file"""
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        
        self.logging_service.info(f"CSV report generated: {csv_path}")
        return csv_path

# Service container for dependency injection
@dataclass
class ServiceContainer:
    """Container for all services"""
    logging_service: LoggingService
    file_service: FileService
    llm_strategy_factory: LLMStrategyFactory
    dispatcher_factory: DispatcherFactory
    rate_limiter_factory: RateLimiterFactory
    web_crawler_factory: WebCrawlerFactory
    csv_writer: CsvWriter
    
    @classmethod
    def create_default(cls, log_level=logging.INFO):
        """Create a default container with all services"""
        logging_service = DefaultLoggingService()
        logging_service.configure(log_level)
        
        file_service = DefaultFileService(logging_service)
        llm_strategy_factory = DefaultLLMStrategyFactory()
        dispatcher_factory = DefaultDispatcherFactory()
        rate_limiter_factory = DefaultRateLimiterFactory()
        web_crawler_factory = DefaultWebCrawlerFactory()
        csv_writer = DefaultCsvWriter(logging_service)
        
        return cls(
            logging_service=logging_service,
            file_service=file_service,
            llm_strategy_factory=llm_strategy_factory,
            dispatcher_factory=dispatcher_factory,
            rate_limiter_factory=rate_limiter_factory,
            web_crawler_factory=web_crawler_factory,
            csv_writer=csv_writer
        )

# Extract category function for backward compatibility
def extract_category_from_input_path(input_path: str) -> str:
    """
    Extract category from input path if it contains 'bundesanzeiger_local_'
    This function is maintained for backward compatibility.
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
    logging.getLogger('LiteLLM').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    # List all active loggers at configuration time
    #active_loggers = [name for name in logging.root.manager.loggerDict]
    #logger.info("Active loggers in the program: %s", active_loggers)
    logger.debug("Logging configured with level: %s", 
                 logging.getLevelName(log_level))

async def process_files(files: List[str], llm_strategy: LLMExtractionStrategy, output_dir: str, 
                    service_container: ServiceContainer = None) -> List[str]:
    """
    Process each file in the list using the provided extraction strategy
    
    Args:
        files: List of file paths to process
        llm_strategy: LLM extraction strategy to use
        output_dir: Directory where output files should be stored
        service_container: Container with services for DI (optional)
    
    Returns:
        List of processed files
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    file_service = service_container.file_service
    
    # Ensure output directory exists
    file_service.ensure_directory(output_dir)
    
    # Create webcrawler (used for HTML processing)
    crawler = await service_container.web_crawler_factory.create_crawler()
    
    logging_service.info(f"Processing {len(files)} files...")
    
    # Processing statistics
    processed_files = []
    error_count = 0
    
    for i, file_path in enumerate(files):
        output_filename = os.path.basename(file_path).replace('.html', '.json')
        output_path = os.path.join(output_dir, output_filename)
        
        # Skip already processed files
        if file_service.file_exists(output_path):
            logging_service.info(f"Already processed [{i+1}/{len(files)}]: {file_path}")
            processed_files.append(output_path)
            continue
        
        logging_service.info(f"Processing [{i+1}/{len(files)}]: {file_path}")
        
        try:
            # Extract company name from file comment
            company_name = file_service.extract_company_name(file_path)
            
            # Process the file
            content = file_service.read_file(file_path)
            
            # Use the webcrawler's extraction capability to process HTML with LLM
            run_config = CrawlerRunConfig(
                urls=[file_path],
                extraction_strategy=llm_strategy,
                cache_mode=CacheMode.BYPASS,
            )
            
            # Process single file
            results = await crawler.process([content], run_config=run_config)
            
            # Get extracted data or error information
            if results and len(results) > 0 and results[0].extraction_result:
                # Success case - write extraction result
                extraction_data = results[0].extraction_result
                file_service.write_file(output_path, extraction_data, is_json=True)
                processed_files.append(output_path)
                logging_service.info(f"Successfully processed {file_path}")
            else:
                # Error case - write error information
                error_message = "No extraction results available"
                if results and len(results) > 0 and results[0].error:
                    error_message = str(results[0].error)
                
                error_data = {"error": True, "message": error_message, "company": company_name}
                file_service.write_file(output_path, error_data, is_json=True)
                processed_files.append(output_path)
                logging_service.error(f"Failed to process {file_path}: {error_message}")
                error_count += 1
                
        except Exception as e:
            logging_service.error(f"Error processing {file_path}: {str(e)}")
            error_data = {"error": True, "message": str(e)}
            file_service.write_file(output_path, error_data, is_json=True)
            processed_files.append(output_path)
            error_count += 1
    
    logging_service.info(f"Processing complete. Processed {len(files)} files with {error_count} errors.")
    return processed_files

async def check_and_reprocess_error_files(output_dir: str, input_dir: str, file_ext: str, 
                                        llm_strategy: LLMExtractionStrategy,
                                        service_container: ServiceContainer = None) -> int:
    """
    Check for files with errors in the output directory and reprocess them
    
    Args:
        output_dir: Directory containing output JSON files
        input_dir: Directory containing input files
        file_ext: Extension of input files to match (e.g., .html)
        llm_strategy: LLM extraction strategy to use
        service_container: Container with services for DI (optional)
        
    Returns:
        Number of files reprocessed
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    file_service = service_container.file_service
    
    logging_service.info(f"Checking for files with errors in {output_dir}...")
    
    # Get list of JSON files in output directory
    json_files = [f for f in file_service.list_files(output_dir) if f.endswith('.json')]
    logging_service.debug(f"Found {len(json_files)} JSON files in {output_dir}")
    
    # Check each JSON file for error flag
    error_files = []
    for json_file in json_files:
        json_path = os.path.join(output_dir, json_file)
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check for error flag in the data
            has_error = False
            if isinstance(data, list):
                has_error = any(isinstance(item, dict) and item.get('error', False) for item in data)
            elif isinstance(data, dict):
                has_error = data.get('error', False)
            
            if has_error:
                # Convert JSON filename to source filename
                source_filename = json_file.replace('.json', file_ext)
                error_files.append(source_filename)
                logging_service.info(f"Found error in {json_file}")
        except Exception as e:
            logging_service.error(f"Error reading {json_file}: {str(e)}")
    
    if not error_files:
        logging_service.info("No files with errors found")
        return 0
    
    logging_service.info(f"Found {len(error_files)} files with errors. Looking for originals...")
    
    # Find the original files matching the error files
    files_to_reprocess = []
    for error_filename in error_files:
        found = False
        for dirpath, dirnames, filenames in file_service.walk_directory(input_dir):
            if error_filename in filenames:
                source_path = os.path.join(dirpath, error_filename)
                files_to_reprocess.append(source_path)
                found = True
                logging_service.info(f"Found source file for {error_filename}: {source_path}")
                break  # Stop after finding the first match
        
        if not found:
            logging_service.warning(f"Error in {error_filename.replace(file_ext, '.json')}, but couldn't find original file {error_filename}")
    
    if not files_to_reprocess:
        logging_service.info("No source files found to reprocess")
        return 0
    
    logging_service.info(f"Reprocessing {len(files_to_reprocess)} files...")
    
    # Reprocess the files
    await process_files(files_to_reprocess, llm_strategy, output_dir, service_container)
    
    return len(files_to_reprocess)

async def process_directory(input_dir: str, output_dir: str, file_pattern: str, 
                          llm_strategy: LLMExtractionStrategy,
                          service_container: ServiceContainer = None) -> List[str]:
    """
    Process all files in a directory matching the pattern and write results to output directory
    
    Args:
        input_dir: Input directory containing files to process
        output_dir: Output directory for processed results
        file_pattern: File pattern to match (glob pattern)
        llm_strategy: LLM extraction strategy to use
        service_container: Container with services for DI (optional)
        
    Returns:
        List of processed files
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    file_service = service_container.file_service
    
    # Ensure directories exist
    file_service.ensure_directory(input_dir)
    file_service.ensure_directory(output_dir)
    
    # Find all HTML files in the input directory
    files_to_process = []
    for dirpath, dirnames, filenames in file_service.walk_directory(input_dir):
        for filename in filenames:
            if filename.endswith(file_pattern):
                file_path = os.path.join(dirpath, filename)
                files_to_process.append(file_path)
    
    logging_service.info(f"Found {len(files_to_process)} files to process in {input_dir}")
    
    return await process_files(files_to_process, llm_strategy, output_dir, service_container)

def extract_values_from_json(json_file: str, service_container: ServiceContainer = None) -> Dict[str, Any]:
    """
    Extract values from a JSON file into a dictionary
    
    Args:
        json_file: Path to JSON file
        service_container: Container with services for DI (optional)
        
    Returns:
        Dictionary with extracted values
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    
    row_data = {}
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Ignore error files
        if isinstance(data, dict) and data.get('error'):
            logging_service.warning(f"Skipping error file: {json_file}")
            return {}
            
        # Extract company name from filename
        file_basename = os.path.basename(json_file)
        company_name = os.path.splitext(file_basename)[0]
        row_data['company_name'] = company_name
        
        # Add source file reference
        row_data['source_file'] = json_file
        
        # Extract table data
        for item in data:
            if not isinstance(item, dict):
                continue
                
            table_name = item.get('table_name', '')
            is_teuro = item.get('is_Teuro', False)
            values = item.get('values', {})
            
            # Only process Sachanlagen values
            for key, value in values.items():
                if key.startswith('Sachanlagen') or key.startswith('Technische_Anlagen'):
                    # Clean up value
                    if isinstance(value, str):
                        # Remove non-numeric characters
                        value = re.sub(r'[^\d.,]', '', value)
                        # Replace comma with dot for decimal
                        value = value.replace(',', '.')
                        
                        try:
                            numeric_value = float(value) if value else 0
                            
                            # Convert to thousands of Euro if not already
                            if is_teuro:
                                value_in_teuro = numeric_value
                            else:
                                value_in_teuro = numeric_value / 1000
                                
                            # Add to row data with table name prefix
                            field_name = f"{table_name}_{key}"
                            row_data[field_name] = value_in_teuro
                            
                        except ValueError:
                            logging_service.warning(f"Could not convert value '{value}' to number in {json_file}")
                            row_data[f"{table_name}_{key}"] = None
                    else:
                        row_data[f"{table_name}_{key}"] = value
        
        return row_data
        
    except Exception as e:
        logging_service.error(f"Error extracting values from {json_file}: {e}")
        return {}

def prepare_csv_data(json_files: List[str], service_container: ServiceContainer = None) -> List[Dict[str, Any]]:
    """
    Prepare CSV data from a list of JSON files
    
    Args:
        json_files: List of JSON files to process
        service_container: Container with services for DI (optional)
        
    Returns:
        List of dictionaries with data for CSV export
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    
    all_rows = []
    all_fields = set(['company_name', 'source_file'])  # Always include these fields
    
    # First pass - extract all data and collect all possible field names
    for json_file in json_files:
        row_data = extract_values_from_json(json_file, service_container)
        if row_data:
            all_rows.append(row_data)
            all_fields.update(row_data.keys())
    
    # Convert set to sorted list for consistent column order
    fieldnames = sorted(list(all_fields))
    
    logging_service.info(f"Prepared {len(all_rows)} rows with {len(fieldnames)} fields")
    
    return all_rows, fieldnames

def export_to_csv(json_dir: str, csv_file: str, service_container: ServiceContainer = None) -> str:
    """
    Export processed JSON files to a CSV file
    
    Args:
        json_dir: Directory containing JSON files
        csv_file: Path to output CSV file
        service_container: Container with services for DI (optional)
        
    Returns:
        Path to generated CSV file
    """
    # Use default container if none provided
    if not service_container:
        service_container = ServiceContainer.create_default()
    
    logging_service = service_container.logging_service
    file_service = service_container.file_service
    csv_writer = service_container.csv_writer
    
    # Find all JSON files in the directory
    json_files = [os.path.join(json_dir, f) for f in file_service.list_files(json_dir) if f.endswith('.json')]
    logging_service.info(f"Found {len(json_files)} JSON files to export in {json_dir}")
    
    # Prepare data for CSV export
    data_rows, fieldnames = prepare_csv_data(json_files, service_container)
    
    if not data_rows:
        logging_service.warning("No data to export")
        return ""
    
    # Export to CSV
    return csv_writer.write_csv(data_rows, fieldnames, csv_file)

def extract_sachanlagen_schema() -> dict:
    """
    Define the extraction schema for Sachanlagen data
    
    Returns:
        Schema dictionary for LLM extraction
    """
    return {
        "title": "Sachanlagen Extraction",
        "description": "Extract Sachanlagen (fixed assets) financial data from balance sheets",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the financial table (e.g., 'Aktiva', 'Passiva', 'GuV')"
                },
                "is_Teuro": {
                    "type": "boolean",
                    "description": "Whether values are in thousands of Euro (T€)"
                },
                "values": {
                    "type": "object",
                    "description": "Key-value pairs of extracted data",
                    "additionalProperties": {
                        "type": "string",
                        "description": "Value for the corresponding key"
                    }
                }
            },
            "required": ["table_name", "is_Teuro", "values"]
        }
    }

async def main():
    """Main function for the Sachanlagen extraction script"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Extract Sachanlagen (fixed assets) data from financial reports'
    )
    parser.add_argument('--input', '-i', required=True, help='Input directory containing HTML files')
    parser.add_argument('--output', '-o', required=True, help='Output directory for processed results')
    parser.add_argument('--pattern', '-p', default='.html', help='File pattern to match (default: .html)')
    parser.add_argument('--csv', '-c', help='Output CSV file (optional)')
    parser.add_argument('--reprocess-errors', '-r', action='store_true', 
                      help='Reprocess files that had errors in previous runs')
    parser.add_argument('--llm-provider', default='bedrock/amazon.nova-pro-v1:0',
                      help='LLM provider to use (default: bedrock/amazon.nova-pro-v1:0)')
    parser.add_argument('--temperature', type=float, default=0.7,
                      help='LLM temperature parameter (default: 0.7)')
    parser.add_argument('--max-tokens', type=int, default=1000,
                      help='LLM max tokens parameter (default: 1000)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Create service container with appropriate log level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    container = ServiceContainer.create_default(log_level)
    
    logging_service = container.logging_service
    logging_service.info("Starting Sachanlagen extraction...")
    
    # Create LLM strategy
    schema = extract_sachanlagen_schema()
    llm_strategy = container.llm_strategy_factory.create_llm_strategy(
        provider=args.llm_provider,
        schema=schema,
        instruction="""
        Extract all references to 'Sachanlagen' from the financial reports.
        Be sure to capture the monetary values associated with these items.
        Return the table name (e.g., Aktiva, Passiva, GuV), whether the value is in thousands of Euro (T€),
        and the key-value pairs for the extracted data.
        """,
        temperature=args.temperature,
        max_tokens=args.max_tokens
    )
    
    # Detect category from input path
    category = container.file_service.extract_category(args.input)
    if category:
        logging_service.info(f"Processing files for category: {category}")
    
    if args.reprocess_errors:
        # Reprocess files that had errors in previous runs
        reprocessed_count = await check_and_reprocess_error_files(
            args.output, 
            args.input, 
            args.pattern,
            llm_strategy,
            container
        )
        logging_service.info(f"Reprocessed {reprocessed_count} files with errors")
    else:
        # Process all files
        processed_files = await process_directory(
            args.input, 
            args.output, 
            args.pattern,
            llm_strategy,
            container
        )
        logging_service.info(f"Processed {len(processed_files)} files")
    
    # Export to CSV if requested
    if args.csv:
        csv_path = export_to_csv(args.output, args.csv, container)
        if csv_path:
            logging_service.info(f"Exported data to {csv_path}")
        else:
            logging_service.error("Failed to export data to CSV")
    
    logging_service.info("Processing complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)



