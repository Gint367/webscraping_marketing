#!/usr/bin/env python3
"""
Master Pipeline Script for Data Extraction and Processing

This script orchestrates the entire data extraction and processing pipeline,
automating the sequential execution of various component scripts that handle
different stages of the process. Users provide a CSV file with company information,
and the script processes this data through all pipeline stages, returning an
enriched CSV with all scraped and analyzed data.

The pipeline includes:
1. Extracting machine assets from financial statements
2. Crawling and scraping keywords
3. Final data integration
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Union

# Configure logger
logger = logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the master pipeline.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Master pipeline for data extraction and processing"
    )
    
    # Required arguments
    parser.add_argument(
        "--input-csv", 
        type=Path, 
        required=True,
        help="Path to input CSV file with company data"
    )
    parser.add_argument(
        "--output-dir", 
        type=Path, 
        required=True,
        help="Directory for output files"
    )
    
    # Optional arguments
    parser.add_argument(
        "--category",
        type=str,
        help="Category to filter companies by (e.g., maschinenbauer)"
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        help="Path to configuration file (JSON)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()

def validate_inputs(input_csv: Path, output_dir: Path) -> None:
    """
    Validate input parameters and create output directories if they don't exist.
    
    Args:
        input_csv: Path to input CSV file
        output_dir: Path to output directory
    
    Raises:
        FileNotFoundError: If input CSV file does not exist
    """
    # Check if input CSV exists
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV file not found: {input_csv}")
    
    # Create output directory if it doesn't exist
    if not output_dir.exists():
        logger.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    elif not output_dir.is_dir():
        raise NotADirectoryError(f"Output path exists but is not a directory: {output_dir}")

def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_path: Path to the configuration file
    
    Returns:
        Dict: Configuration data
    
    Raises:
        FileNotFoundError: If config file does not exist
        json.JSONDecodeError: If config file is not valid JSON
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)

def merge_config_with_args(config: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    """
    Merge configuration from file with command-line arguments.
    Command-line arguments take precedence over config file values.
    
    Args:
        config: Configuration dictionary loaded from file
        args: Parsed command-line arguments
    
    Returns:
        Dict: Merged configuration
    """
    merged_config = config.copy()
    
    # Map args to config
    if args.output_dir:
        merged_config["output_dir"] = str(args.output_dir)
    
    if args.category:
        merged_config["category"] = args.category
    
    if args.input_csv:
        merged_config["input_csv"] = str(args.input_csv)
    
    # Set log level based on verbose flag
    if args.verbose:
        merged_config["log_level"] = "DEBUG"
    
    return merged_config

def setup_logging(log_level: str, log_file: Optional[Path] = None) -> logging.Logger:
    """
    Configure logging with the specified level.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to a log file
    
    Returns:
        logging.Logger: Configured logger
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Configure logging
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = []
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    handlers.append(console_handler)
    
    # Add file handler if log_file is provided
    if log_file:
        file_handler = logging.FileHandler(str(log_file))
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level, 
        format=log_format,
        handlers=handlers
    )
    
    # Get and configure the module logger
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    
    return logger

def run_extracting_machine_pipeline(input_csv: str, output_dir: str, category: Optional[str] = None) -> str:
    """
    Run the extracting machine assets pipeline component.
    
    Args:
        input_csv: Path to input CSV file
        output_dir: Path to output directory
        category: Optional category to filter companies
    
    Returns:
        str: Path to the output file from this pipeline component
    """
    # These will be implemented in future subtasks
    # For now, just log the function call and return a placeholder path
    logger.info(f"Running extracting machine pipeline with input: {input_csv}")
    
    from_category = f" (filtered by category: {category})" if category else ""
    logger.info(f"Processing companies from {input_csv}{from_category}")
    
    # In the real implementation, we would call:
    # 1. get_company_by_category
    # 2. get_bundesanzeiger_html
    # 3. clean_html
    # 4. extract_sachanlagen
    # 5. generate_csv_report
    # 6. merge_csv_with_excel
    
    return f"{output_dir}/extracting_machine_output.csv"

def run_webcrawl_pipeline(input_csv: str, output_dir: str) -> str:
    """
    Run the web crawling and keyword extraction pipeline component.
    
    Args:
        input_csv: Path to input CSV file
        output_dir: Path to output directory
    
    Returns:
        str: Path to the output file from this pipeline component
    """
    # These will be implemented in future subtasks
    # For now, just log the function call and return a placeholder path
    logger.info(f"Running webcrawl pipeline with input: {input_csv}")
    
    # In the real implementation, we would call:
    # 1. crawl_domain
    # 2. extract_llm
    # 3. pluralize_with_llm
    # 4. consolidate
    # 5. fill_process_type
    # 6. convert_to_csv
    
    return f"{output_dir}/webcrawl_output.csv"

def run_integration_pipeline(extracting_output: str, webcrawl_output: str, output_dir: str) -> str:
    """
    Run the final data integration pipeline component.
    
    Args:
        extracting_output: Path to output from extracting machine pipeline
        webcrawl_output: Path to output from webcrawl pipeline
        output_dir: Path to output directory
    
    Returns:
        str: Path to the final output file
    """
    # These will be implemented in future subtasks
    # For now, just log the function call and return a placeholder path
    logger.info(f"Running integration pipeline with inputs: {extracting_output} and {webcrawl_output}")
    
    # In the real implementation, we would call:
    # 1. merge_technische_anlagen_with_keywords
    # 2. enrich_data
    
    return f"{output_dir}/final_output.csv"

def run_pipeline(config: Dict[str, Any]) -> str:
    """
    Run the complete pipeline with all components in sequence.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        str: Path to the final output file
    """
    try:
        input_csv = config.get("input_csv", "")
        output_dir = config.get("output_dir", "")
        category = config.get("category")
        
        if not input_csv or not output_dir:
            raise ValueError("Missing required configuration: input_csv or output_dir")
        
        logger.info("Starting pipeline execution")
        
        # Run extracting machine pipeline
        extracting_output = run_extracting_machine_pipeline(
            input_csv, output_dir, category
        )
        logger.info(f"Completed extracting machine pipeline: {extracting_output}")
        
        # Run webcrawl pipeline
        webcrawl_output = run_webcrawl_pipeline(
            input_csv, output_dir
        )
        logger.info(f"Completed webcrawl pipeline: {webcrawl_output}")
        
        # Run integration pipeline
        final_output = run_integration_pipeline(
            extracting_output, webcrawl_output, output_dir
        )
        logger.info(f"Completed integration pipeline: {final_output}")
        
        logger.info(f"Pipeline execution completed successfully. Final output: {final_output}")
        return final_output
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise

def main() -> None:
    """
    Main function that orchestrates the entire pipeline.
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Validate input parameters
    validate_inputs(args.input_csv, args.output_dir)
    
    # Load configuration (if provided)
    config = {}
    if args.config_file:
        config = load_config(args.config_file)
    
    # Merge config with command-line arguments
    merged_config = merge_config_with_args(config, args)
    
    # Set up logging
    log_level = merged_config.get("log_level", "INFO")
    log_file = None
    if "output_dir" in merged_config:
        log_file = Path(merged_config["output_dir"]) / "pipeline.log"
    
    logger = setup_logging(log_level, log_file)
    
    # Log configuration
    logger.debug(f"Running with configuration: {merged_config}")
    
    # Run the pipeline
    output_file = run_pipeline(merged_config)
    logger.info(f"Pipeline completed. Output file: {output_file}")

if __name__ == "__main__":
    main()
