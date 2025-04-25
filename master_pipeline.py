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
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Import functions from extracting_machines components
# Import functions from integration components
# Import functions from webcrawl components

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
    logger.info("Starting Extracting Machine Assets phase")
    
    # Create necessary output directories
    output_path = Path(output_dir)
    filtered_dir = output_path / "filtered_companies"
    bundesanzeiger_dir = output_path / "bundesanzeiger_html"
    cleaned_html_dir = output_path / "cleaned_html"
    sachanlagen_dir = output_path / "sachanlagen_data"
    report_dir = output_path / "reports"
    for directory in [filtered_dir, bundesanzeiger_dir, cleaned_html_dir, sachanlagen_dir, report_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Filter companies by category if needed
    from_category = f" (filtered by category: {category})" if category else ""
    logger.info(f"Step 1: Processing companies from {input_csv}{from_category}")
    filtered_csv = str(filtered_dir / f"filtered_companies_{category or 'all'}.csv")
    try:
        from extracting_machines.get_company_by_category import (
            extract_companies_by_category,
        )
        filtered_csv = extract_companies_by_category(
            input_file=input_csv,
            output_file=filtered_csv,
            category=category if category is not None else ""
        )
        logger.info(f"Companies filtered successfully: {filtered_csv}")
    except Exception as e:
        logger.error(f"Error filtering companies: {str(e)}")
        raise
    
    # Step 2: Get HTML from Bundesanzeiger
    logger.info("Step 2: Extracting HTML from Bundesanzeiger")
    try:
        from extracting_machines.get_bundesanzeiger_html import (
            main as get_bundesanzeiger_html,
        )
        bundesanzeiger_output = get_bundesanzeiger_html(
            input_csv=filtered_csv,
            base_dir=str(bundesanzeiger_dir)
        )
        logger.info(f"Bundesanzeiger HTML extracted successfully: {bundesanzeiger_output}")
    except Exception as e:
        logger.error(f"Error extracting Bundesanzeiger HTML: {str(e)}")
        raise
    
    # Step 3: Clean HTML
    logger.info("Step 3: Cleaning HTML content")
    try:
        from extracting_machines.clean_html import main as clean_html_main
        cleaned_html_output = clean_html_main(
            input_dir=bundesanzeiger_output,
            output_dir=str(cleaned_html_dir),
            search_word="technische Anlagen",
            verbose=False
        )
        logger.info(f"HTML cleaned successfully: {cleaned_html_output}")
    except Exception as e:
        logger.error(f"Error cleaning HTML: {str(e)}")
        raise
    
    # Step 4: Extract Sachanlagen
    logger.info("Step 4: Extracting Sachanlagen data")
    try:
        from extracting_machines.extract_sachanlagen import run_extraction
        sachanlagen_output = run_extraction(
            input_path=cleaned_html_output,
            output_dir=str(sachanlagen_dir),
            ext=".html",
            overwrite=False,
            log_level="INFO"
        )
        logger.info(f"Sachanlagen data extracted successfully: {sachanlagen_output}")
    except Exception as e:
        logger.error(f"Error extracting Sachanlagen: {str(e)}")
        raise
    
    # Step 5: Generate CSV report
    logger.info("Step 5: Generating CSV report")
    try:
        from extracting_machines.generate_csv_report import (
            extract_values,
            generate_csv_report,
        )
        csv_report = str(report_dir / "machine_report.csv")
        filter_words = ["anschaffungs","ahk", "abschreibung", "buchwert"]
        if sachanlagen_output is None:
            logger.error("Sachanlagen output is None, cannot generate CSV report.")
            raise ValueError("Sachanlagen output is None, cannot generate CSV report.")
        csv_report = generate_csv_report(
            input_dir=sachanlagen_output,
            output_file=csv_report,
            n=3,
            extract_func=lambda data, n: extract_values(data, n, filter_words)
        )
        logger.info(f"CSV report generated successfully: {csv_report}")
    except Exception as e:
        logger.error(f"Error generating CSV report: {str(e)}")
        raise
    
    # Step 6: Merge CSV with Excel
    logger.info("Step 6: Merging CSV with Excel data")
    final_output = str(output_path / f"extracting_machine_output_{category or 'all'}.csv")
    try:
        from extracting_machines.merge_csv_with_excel import (
            main as merge_csv_with_excel,
        )
        final_output = merge_csv_with_excel(
            csv_file_path=csv_report,
            xlsx_file_path=input_csv,
            output_file_path=final_output,
            top_n=3
        )
        logger.info(f"CSV merged with Excel successfully: {final_output}")
    except Exception as e:
        logger.error(f"Error merging CSV with Excel: {str(e)}")
        raise
    
    logger.info("Extracting Machine Assets phase completed successfully")
    if final_output is None:
        logger.error("Final output from Extracting Machine Assets pipeline is None.")
        raise ValueError("Final output from Extracting Machine Assets pipeline is None.")
    return final_output

def run_webcrawl_pipeline(input_csv: str, output_dir: str) -> str:
    """
    Run the web crawling and keyword extraction pipeline component.
    The pipeline now follows this sequence:

    - Crawl domain
    - Extract keywords with LLM
    - Fill process type
    - Consolidate data
    - Pluralize keywords with LLM
    - Convert to CSV
    
    Args:
        input_csv: Path to input CSV file
        output_dir: Path to output directory
    
    Returns:
        str: Path to the output file from this pipeline component
    """
    logger.info("Starting Crawling & Scraping Keywords phase")
    
    # Create necessary output directories
    output_path = Path(output_dir)
    crawl_dir = output_path / "domain_content"
    extract_dir = output_path / "extracted_keywords"
    pluralize_dir = output_path / "pluralized_keywords"
    for directory in [crawl_dir, extract_dir, pluralize_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Crawl domain
    logger.info("Step 1: Crawling company domains")
    try:
        import asyncio

        from webcrawl.crawl_domain import main as crawl_domain_mains
        crawl_output = asyncio.run(
            crawl_domain_mains(
                input_csv_path=input_csv,
                output_dir=str(crawl_dir)
            )
        )
        if not crawl_output:
            raise ValueError("crawl_domain_mains returned None or empty output.")
        logger.info(f"Domains crawled successfully: {crawl_output}")
    except Exception as e:
        logger.error(f"Error crawling domains: {str(e)}")
        raise
    
    # Step 2: Extract keywords with LLM
    logger.info("Step 2: Extracting keywords with LLM")
    try:
        from webcrawl.extract_llm import run_extract_llm
        extract_output = run_extract_llm(
            input_path=crawl_output,
            output_dir=str(extract_dir),
            ext=".md",
            overwrite=False,
            log_level="INFO"
        )
        if not extract_output:
            raise ValueError("extract_llm returned None or empty output.")
        logger.info(f"Keywords extracted successfully: {extract_output}")
    except Exception as e:
        logger.error(f"Error extracting keywords: {str(e)}")
        raise
    
    # Step 3: Fill process type
    logger.info("Step 3: Filling process types")
    try:
        from webcrawl.fill_process_type import run_fill_process_type
        process_type_outputs = run_fill_process_type(
            input_file=extract_output,
            output_dir=str(output_path),
            log_level="INFO"
        )
        if not process_type_outputs or not isinstance(process_type_outputs, list):
            raise ValueError("run_fill_process_type returned None or invalid output.")
        # Use the first output file as the main output
        process_type_output = process_type_outputs[0]
        logger.info(f"Process types filled successfully: {process_type_output}")
    except Exception as e:
        logger.error(f"Error filling process types: {str(e)}")
        raise
    
    # Step 4: Consolidate data
    logger.info("Step 4: Consolidating data")
    consolidated_path = str(output_path / "consolidated_data.json")
    try:
        from webcrawl.consolidate import consolidate_main
        consolidated_output = consolidate_main(
            input_path=process_type_output,
            output_path=consolidated_path,
            log_level="INFO"
        )
        if not consolidated_output:
            raise ValueError("consolidate_main returned None or empty output.")
        logger.info(f"Data consolidated successfully: {consolidated_output}")
    except Exception as e:
        logger.error(f"Error consolidating data: {str(e)}")
        raise
    
    # Step 5: Pluralize keywords with LLM
    logger.info("Step 5: Pluralizing keywords")
    try:
        from webcrawl.pluralize_with_llm import process_directory as pluralize_with_llm
        pluralize_output = pluralize_with_llm(
            input_dir=consolidated_output,
            output_dir=str(pluralize_dir)
        )
        if not pluralize_output:
            raise ValueError("pluralize_with_llm returned None or empty output.")
        logger.info(f"Keywords pluralized successfully: {pluralize_output}")
    except Exception as e:
        logger.error(f"Error pluralizing keywords: {str(e)}")
        raise
    
    # Step 6: Convert to CSV
    logger.info("Step 6: Converting to CSV")
    csv_output = str(output_path / "webcrawl_output.csv")
    try:
        from webcrawl.convert_to_csv import convert_json_to_csv
        final_output = convert_json_to_csv(
            json_file_path=pluralize_output,
            csv_file_path=csv_output,
            omit_config_path=None
        )
        if not final_output:
            raise ValueError("convert_json_to_csv returned None or empty output.")
        logger.info(f"Converted to CSV successfully: {final_output}")
    except Exception as e:
        logger.error(f"Error converting to CSV: {str(e)}")
        raise
    
    logger.info("Crawling & Scraping Keywords phase completed successfully")
    return final_output

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
    logger.info("Starting Final Data Integration phase")
    
    # Create necessary output directories
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Merge technische anlagen with keywords
    logger.info("Step 1: Merging technische anlagen with keywords")
    merged_path = str(output_path / "merged_data.csv")
    try:
        # Import locally to handle potential import errors
        from merge_technische_anlagen_with_keywords import merge_csv_with_excel
        
        merged_output = merge_csv_with_excel(
            csv_path=webcrawl_output,
            base_data_path=extracting_output,
            output_path=merged_path
        )
        logger.info(f"Data merged successfully: {merged_output}")
    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        raise
    
    # Step 2: Enrich data
    logger.info("Step 2: Enriching data with additional information")
    final_output = str(output_path / "final_output.csv")
    try:
        # Import locally to handle potential import errors
        from enrich_data import enrich_data
        
        # Create output directory if it doesn't exist
        final_output_dir = os.path.dirname(final_output)
        if final_output_dir:
            os.makedirs(final_output_dir, exist_ok=True)
            
        # Call enrich_data with the correct parameter usage
        enriched_output = enrich_data(input_file=merged_output)
        
        # Move the enriched file to the desired output location if needed
        if enriched_output != final_output:
            shutil.copy2(enriched_output, final_output)
            enriched_output = final_output
            
        logger.info(f"Data enriched successfully: {enriched_output}")
    except Exception as e:
        logger.error(f"Error enriching data: {str(e)}")
        raise
    
    logger.info("Final Data Integration phase completed successfully")
    return enriched_output

def run_pipeline(config: Dict[str, Any]) -> str:
    """
    Run the complete pipeline with all components in sequence.
    
    Args:
        config: Configuration dictionary containing all necessary parameters
    
    Returns:
        str: Path to the final output file
    """
    try:
        # Extract required configuration parameters
        input_csv = config.get("input_csv", "")
        output_dir = config.get("output_dir", "")
        category = config.get("category")
        
        # Validate required parameters are present
        if not input_csv or not output_dir:
            raise ValueError("Missing required configuration: input_csv or output_dir")
        
        # Create timestamp for this run
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        run_output_dir = Path(output_dir) / f"pipeline_run_{timestamp}"
        
        # Create output directories for each phase
        extracting_output_dir = run_output_dir / "extracting_machine"
        webcrawl_output_dir = run_output_dir / "webcrawl"
        integration_output_dir = run_output_dir / "integration"
        
        for directory in [run_output_dir, extracting_output_dir, webcrawl_output_dir, integration_output_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Starting pipeline execution at {timestamp}")
        logger.info(f"Input CSV: {input_csv}")
        logger.info(f"Output directory: {run_output_dir}")
        if category:
            logger.info(f"Filtering by category: {category}")
        
        # Phase 1: Extracting Machine Assets
        phase_start_time = time.time()
        logger.info("Phase 1: Extracting Machine Assets - Started")
        
        extracting_output = run_extracting_machine_pipeline(
            input_csv=input_csv,
            output_dir=str(extracting_output_dir),
            category=category
        )
        
        phase_duration = time.time() - phase_start_time
        logger.info(f"Phase 1: Extracting Machine Assets - Completed in {phase_duration:.2f} seconds")
        logger.info(f"Extracting Machine Assets output: {extracting_output}")
        
        # Phase 2: Crawling & Scraping Keywords
        phase_start_time = time.time()
        logger.info("Phase 2: Crawling & Scraping Keywords - Started")
        
        webcrawl_output = run_webcrawl_pipeline(
            input_csv=input_csv,
            output_dir=str(webcrawl_output_dir)
        )
        
        phase_duration = time.time() - phase_start_time
        logger.info(f"Phase 2: Crawling & Scraping Keywords - Completed in {phase_duration:.2f} seconds")
        logger.info(f"Webcrawl output: {webcrawl_output}")
        
        # Phase 3: Final Data Integration
        phase_start_time = time.time()
        logger.info("Phase 3: Final Data Integration - Started")
        
        final_output = run_integration_pipeline(
            extracting_output=extracting_output,
            webcrawl_output=webcrawl_output,
            output_dir=str(integration_output_dir)
        )
        
        phase_duration = time.time() - phase_start_time
        logger.info(f"Phase 3: Final Data Integration - Completed in {phase_duration:.2f} seconds")
        logger.info(f"Final output: {final_output}")
        
        # Copy final output to the main output directory with a descriptive name
        final_filename = f"final_export_{category or 'all'}.csv"
        final_destination = Path(output_dir) / final_filename
        
        # Use shutil to copy the file
        import shutil
        if Path(final_output).exists():
            shutil.copy2(final_output, final_destination)
            logger.info(f"Final output copied to: {final_destination}")
        else:
            logger.warning(f"Final output file '{final_output}' does not exist, cannot copy to '{final_destination}'")
        
        logger.info("Pipeline execution completed successfully.")
        logger.info(f"Final output copied to: {final_destination}")
        
        return str(final_destination)
        
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
