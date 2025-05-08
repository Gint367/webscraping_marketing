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
import asyncio  # Moved import to top
import csv  # Add csv import for validation
import json
import logging
import os
import shutil
import sys  # Added for sys.exit
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from tqdm import tqdm  # Add tqdm import

# Import the enhanced ArtifactManager for artifact management
from utils.artifact_manager import ArtifactManager as BaseArtifactManager

# --- Artifact Management ---


class PipelineArtifacts(BaseArtifactManager):
    """
    Collects and manages paths to intermediate and final artifacts produced by each pipeline phase.
    Provides methods to register, list, and retrieve artifacts for UI or downstream use.

    This is a wrapper around the ArtifactManager that preserves compatibility with existing code.
    """

    def __init__(self, storage_root: Optional[str] = None) -> None:
        """
        Initialize the pipeline artifacts manager.

        Args:
            storage_root: Optional root directory for artifact storage.
        """
        super().__init__(storage_root)

    # Override register to match the original signature and behavior
    def register(
        self, phase: str, name: str, path: Union[str, List[str]], description: str = ""
    ) -> None:
        """
        Register an artifact for a pipeline phase.
        Args:
            phase: Name of the pipeline phase (e.g., 'extracting_machine')
            name: Artifact name (e.g., 'filtered_csv')
            path: Path(s) to the artifact file(s)
            description: Optional description of the artifact
        """
        super().register(phase, name, path, description)

    # Original methods are inherited from the parent class


# Configure logger
logger = logging.getLogger("master_pipeline")


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
        help="Path to input file (CSV or Excel). CSV files must contain 'company name', 'location', and 'url' columns.",
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="Directory for output files"
    )

    # Optional arguments
    parser.add_argument(
        "--category",
        type=str,
        help="Category to filter companies by (e.g., maschinenbauer)",
    )
    parser.add_argument(
        "--config-file", type=Path, help="Path to configuration file (JSON)"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--skip-llm-validation",
        action="store_true",
        help="Skip validation of LLM providers (not recommended for production runs)",
    )

    return parser.parse_args()


def validate_inputs(input_csv: Path, output_dir: Path) -> None:
    """
    Validate input parameters and create output directories if they don't exist.

    Args:
        input_csv: Path to input CSV or Excel file
        output_dir: Path to output directory

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If input file doesn't have a supported extension (.csv or .xlsx/.xls)
    """
    # Check if input file exists
    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    # Check if file extension is supported
    if input_csv.suffix.lower() not in [".csv", ".xlsx", ".xls"]:
        raise ValueError(
            f"Unsupported file format: {input_csv.suffix}. Use CSV or Excel files only."
        )

    # Create output directory if it doesn't exist
    if not output_dir.exists():
        logger.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    elif not output_dir.is_dir():
        raise NotADirectoryError(
            f"Output path exists but is not a directory: {output_dir}"
        )


def validate_csv_columns(csv_path: str, required_columns: List[str]) -> bool:
    """
    Validate that a CSV file contains all required columns.

    Args:
        csv_path: Path to the CSV file (string or Path object)
        required_columns: List of column names that must be present

    Returns:
        bool: True if all required columns are present, False otherwise

    Raises:
        FileNotFoundError: If CSV file does not exist
        csv.Error: If CSV file is invalid
    """
    try:
        # Convert Path objects to string if needed
        if not isinstance(csv_path, str):
            csv_path = str(csv_path)

        with open(csv_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Get the header row

            # Check if all required columns are present (case insensitive)
            header_lower = [col.lower() for col in header]
            missing_columns = [
                col for col in required_columns if col.lower() not in header_lower
            ]

            if missing_columns:
                logger.warning(
                    f"CSV file is missing required columns: {', '.join(missing_columns)}"
                )
                return False

            return True
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        raise
    except csv.Error as e:
        logger.error(f"Error reading CSV file {csv_path}: {str(e)}")
        raise


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

    with open(config_path, "r") as f:
        return json.load(f)


def merge_config_with_args(
    config: Dict[str, Any], args: argparse.Namespace
) -> Dict[str, Any]:
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

    # Include skip_llm_validation flag
    merged_config["skip_llm_validation"] = args.skip_llm_validation

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
    logging.basicConfig(level=numeric_level, format=log_format, handlers=handlers)

    # Get and configure the module logger
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    # Set log level for other libraries to reduce noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    # Set log level for HTTPx, which is used by AsyncWebCrawler
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # Set log level for LiteLLM and Botocore
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    return logger


def run_extracting_machine_pipeline(
    input_csv: str, output_dir: str, category: Optional[str] = None
) -> str:
    """
    Run the extracting machine assets pipeline component.

    This pipeline executes several steps:
    1. Filter companies by category (skipped for CSV input files)
    2. Extract HTML from Bundesanzeiger
    3. Clean HTML content
    4. Extract Sachanlagen data
    5. Generate CSV report
    6. Merge CSV with Excel data

    For CSV input files, the filtering step is skipped but the file must contain
    the required columns: "company name", "location", and "url".
    For Excel input files, the complete filtering process is applied.

    Args:
        input_csv: Path to input CSV or Excel file
        output_dir: Path to output directory
        category: Optional category to filter companies

    Returns:
        str: Path to the output file from this pipeline component

    Raises:
        ValueError: If the input CSV file is missing required columns
        FileNotFoundError: If input file doesn't exist
    """

    logger.info("Starting Extracting Machine Assets phase")
    logger.info(
        "PROGRESS:extracting_machine:main:0/6:Starting Extracting Machine Assets phase"
    )  # Progress Start

    # Create necessary output directories
    output_path = Path(output_dir)
    filtered_dir = output_path / "filtered_companies"
    bundesanzeiger_dir = output_path / "bundesanzeiger_html"
    cleaned_html_dir = output_path / "cleaned_html"
    sachanlagen_dir = output_path / "sachanlagen_data"
    report_dir = output_path / "reports"
    for directory in [
        filtered_dir,
        bundesanzeiger_dir,
        cleaned_html_dir,
        sachanlagen_dir,
        report_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    # Artifact management
    artifacts = PipelineArtifacts()

    # Determine if the input is a CSV or Excel file
    input_file_path = Path(input_csv)
    is_csv_input = input_file_path.suffix.lower() == ".csv"

    # Step 1: Filter companies by category if needed
    from_category = f" (filtered by category: {category})" if category else ""
    logger.info(f"Step 1: Processing companies from {input_csv}{from_category}")
    filtered_csv = str(filtered_dir / f"filtered_companies_{category or 'all'}.csv")

    if is_csv_input:
        # Skip step 1 for CSV input, but verify required columns are present
        logger.info("Input is a CSV file. Skipping company filtering step.")
        # Define required columns based on what the pipeline needs
        required_columns = ["company name", "location", "url"]
        try:
            if validate_csv_columns(input_csv, required_columns):
                logger.info(
                    f"CSV validation successful. Required columns present: {', '.join(required_columns)}"
                )
                filtered_csv = input_csv  # Use the input CSV directly
            else:
                logger.error(
                    "CSV missing required columns. Cannot proceed with pipeline."
                )
                raise ValueError(
                    f"Input CSV must contain these columns: {', '.join(required_columns)}"
                )
        except Exception as e:
            logger.error(f"Error validating CSV columns: {str(e)}")
            raise
    else:
        # Process Excel file through the filtering step
        try:
            from extracting_machines.get_company_by_category import (
                extract_companies_by_category,
            )

            filtered_csv = extract_companies_by_category(
                input_file=input_csv,
                output_file=filtered_csv,
                category=category if category is not None else "",
            )
            logger.info(f"Companies filtered successfully: {filtered_csv}")
        except Exception as e:
            logger.error(f"Error filtering companies: {str(e)}")
            raise

    artifacts.register(
        phase="extracting_machine",
        name="filtered_csv",
        path=str(filtered_csv),
        description="Filtered companies CSV (after category filtering or validation)",
    )

    logger.info(
        "PROGRESS:extracting_machine:main:1/6:Completed Step 1 (Company Filtering/Validation)"
    )

    # Step 2: Get HTML from Bundesanzeiger
    logger.info("Step 2: Extracting HTML from Bundesanzeiger")

    try:
        from extracting_machines.get_bundesanzeiger_html import (
            main as get_bundesanzeiger_html,
        )

        bundesanzeiger_output = get_bundesanzeiger_html(
            input_csv=filtered_csv, base_dir=str(bundesanzeiger_dir)
        )
        logger.info(
            f"Bundesanzeiger HTML extracted successfully: {bundesanzeiger_output}"
        )
        artifacts.register(
            phase="extracting_machine",
            name="bundesanzeiger_html",
            path=str(bundesanzeiger_output),
            description="Extracted HTML files from Bundesanzeiger",
        )
    except Exception as e:
        logger.error(f"Error extracting Bundesanzeiger HTML: {str(e)}")
        raise

    logger.info(
        "PROGRESS:extracting_machine:main:2/6:Completed Step 2 (Bundesanzeiger HTML Extraction)"
    )

    # Step 3: Clean HTML
    logger.info("Step 3: Cleaning HTML content")

    try:
        from extracting_machines.clean_html import main as clean_html_main

        cleaned_html_output = clean_html_main(
            input_dir=bundesanzeiger_output,
            output_dir=str(cleaned_html_dir),
            search_word="technische Anlagen",
            verbose=False,
        )
        logger.info(f"HTML cleaned successfully: {cleaned_html_output}")
        artifacts.register(
            phase="extracting_machine",
            name="cleaned_html",
            path=str(cleaned_html_output),
            description="Cleaned HTML files after processing",
        )
    except Exception as e:
        logger.error(f"Error cleaning HTML: {str(e)}")
        raise

    logger.info("PROGRESS:extracting_machine:main:3/6:Completed Step 3 (HTML Cleaning)")

    # Step 4: Extract Sachanlagen
    logger.info("Step 4: Extracting Sachanlagen data")

    try:
        from extracting_machines.extract_sachanlagen import run_extraction

        sachanlagen_output = run_extraction(
            input_path=cleaned_html_output,
            output_dir=str(sachanlagen_dir),
            ext=".html",
            overwrite=False,
            log_level="INFO",
        )
        logger.info(f"Sachanlagen data extracted successfully: {sachanlagen_output}")
        artifacts.register(
            phase="extracting_machine",
            name="sachanlagen_data",
            path=str(sachanlagen_output),
            description="Extracted Sachanlagen data files",
        )
    except Exception as e:
        logger.error(f"Error extracting Sachanlagen: {str(e)}")
        raise

    logger.info(
        "PROGRESS:extracting_machine:main:4/6:Completed Step 4 (Sachanlagen Extraction)"
    )

    # Step 5: Generate CSV report
    logger.info("Step 5: Generating CSV report")

    try:
        from extracting_machines.generate_csv_report import (
            extract_values,
            generate_csv_report,
        )

        csv_report = str(report_dir / "machine_report.csv")
        filter_words = ["anschaffungs", "ahk", "abschreibung", "buchwert"]
        if sachanlagen_output is None:
            logger.error("Sachanlagen output is None, cannot generate CSV report.")
            raise ValueError("Sachanlagen output is None, cannot generate CSV report.")

        csv_report = generate_csv_report(
            input_dir=cleaned_html_output,
            output_file=csv_report,
            n=3,
            extract_func=lambda data, n: extract_values(data, n, filter_words),
        )
        logger.info(f"CSV report generated successfully: {csv_report}")
        artifacts.register(
            phase="extracting_machine",
            name="csv_report",
            path=str(csv_report),
            description="Generated CSV report from Sachanlagen extraction",
        )
    except Exception as e:
        logger.error(f"Error generating CSV report: {str(e)}")
        raise

    logger.info(
        "PROGRESS:extracting_machine:main:5/6:Completed Step 5 (CSV Report Generation)"
    )

    # Step 6: Merge CSV with Excel
    logger.info("Step 6: Merging CSV with Excel data")

    final_output = str(
        output_path / f"extracting_machine_output_{category or 'all'}.csv"
    )
    try:
        from extracting_machines.merge_csv_with_excel import (
            main as merge_csv_with_excel,
        )

        final_output = merge_csv_with_excel(
            csv_file_path=csv_report,
            original_company_file_path=input_csv,
            output_file_path=final_output,
            sachanlagen_path=sachanlagen_output,
            top_n=1,
        )
        logger.info(f"CSV merged with Original Base successfully: {final_output}")
        artifacts.register(
            phase="extracting_machine",
            name="final_output",
            path=str(final_output),
            description="Final merged output CSV for extracting machine phase",
        )
    except Exception as e:
        logger.error(f"Error merging CSV with Base: {str(e)}")
        raise

    logger.info("PROGRESS:extracting_machine:main:6/6:Completed Step 6 (Data Merging)")

    logger.info("Extracting Machine Assets phase completed successfully")
    if final_output is None:
        logger.error("Final output from Extracting Machine Assets pipeline is None.")
        raise ValueError(
            "Final output from Extracting Machine Assets pipeline is None."
        )

    # Optionally, return both the final output and the artifacts registry
    return final_output, artifacts


def run_webcrawl_pipeline(
    extracting_output: str, output_dir: str, category: Optional[str] = None
) -> str:
    """
    Run the web crawling and keyword extraction pipeline component.
    The pipeline now follows this sequence:
    - Crawl domain
    - Extract keywords with LLM
    - Fill process type
    - Pluralize keywords with LLM
    - Consolidate data
    - Convert to CSV

    Args:
        extracting_output: Path to the filtered/processed CSV file from extracting machine pipeline
        output_dir: Path to output directory

    Returns:
        str: Path to the output file from this pipeline component
    """
    logger.info("Starting Crawling & Scraping Keywords phase")
    logger.info(
        "PROGRESS:webcrawl:main:0/6:Starting Crawling & Scraping Keywords phase"
    )  # Progress Start

    # Create necessary output directories
    output_path = Path(output_dir)
    crawl_dir = output_path / "domain_content"
    extract_dir = output_path / "extracted_keywords"
    pluralize_dir = output_path / "pluralized_keywords"
    process_type_dir = output_path / "process_type_filled"
    for directory in [crawl_dir, extract_dir, pluralize_dir, process_type_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    # Artifact management
    artifacts = PipelineArtifacts()
    # Check if extracting_output is a tuple (path, artifacts) and extract just the path
    if isinstance(extracting_output, tuple) and len(extracting_output) > 0:
        extracting_output = extracting_output[0]
    # Step 1: Crawl domain
    logger.info("Step 1: Crawling company domains")
    try:
        from webcrawl.crawl_domain import main as crawl_domain_mains

        crawl_output = asyncio.run(
            crawl_domain_mains(
                input_csv_path=extracting_output, output_dir=str(crawl_dir)
            )
        )
        if not crawl_output:
            raise ValueError("crawl_domain_mains returned None or empty output.")
        logger.info(f"Domains crawled successfully: {crawl_output}")
        artifacts.register(
            phase="webcrawl",
            name="crawl_output",
            path=str(crawl_output),
            description="Crawled domain content",
        )
    except Exception as e:
        logger.error(f"Error crawling domains: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:1/6:Completed Step 1 (Domain Crawling)")

    # Step 2: Extract keywords with LLM
    logger.info("Step 2: Extracting keywords with LLM")
    try:
        from webcrawl.extract_llm import run_extract_llm

        extract_output = run_extract_llm(
            input_path=crawl_output,
            output_dir=str(extract_dir),
            ext=".md",
            overwrite=False,
            log_level="INFO",
        )
        if not extract_output:
            raise ValueError("extract_llm returned None or empty output.")
        logger.info(f"Keywords extracted successfully: {extract_output}")
        artifacts.register(
            phase="webcrawl",
            name="extracted_keywords",
            path=str(extract_output),
            description="Extracted keywords",
        )
    except Exception as e:
        logger.error(f"Error extracting keywords: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:2/6:Completed Step 2 (Keyword Extraction)")

    # Step 3: Fill process type (output to process_type_filled folder)
    logger.info("Step 3: Filling process types")
    try:
        from webcrawl.fill_process_type import run_fill_process_type

        process_type_outputs = run_fill_process_type(
            folder=extract_output,
            output_dir=str(process_type_dir),
            log_level="INFO",
            category=category,
        )
        if not process_type_outputs or not isinstance(process_type_outputs, list):
            raise ValueError("run_fill_process_type returned None or invalid output.")
        # Use the first output file as the main output
        process_type_output = process_type_outputs[0]
        logger.info(f"Process types filled successfully: {process_type_output}")
        artifacts.register(
            phase="webcrawl",
            name="process_type_filled",
            path=str(process_type_dir),
            description="Process type filled",
        )
    except Exception as e:
        logger.error(f"Error filling process types: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:3/6:Completed Step 3 (Process Type Filling)")

    # Step 4: Pluralize keywords with LLM
    logger.info("Step 4: Pluralizing keywords")
    try:
        from webcrawl.pluralize_with_llm import (
            process_file_or_directory as pluralize_with_llm,
        )

        # Always use directory output path since we're processing a directory
        pluralize_output_path = str(pluralize_dir)
        pluralize_output = pluralize_with_llm(
            input_path=str(process_type_dir),  # Use the whole folder for pluralization
            output_path=pluralize_output_path,
        )
        if not pluralize_output:
            raise ValueError("pluralize_with_llm returned None or empty output.")
        logger.info(f"Keywords pluralized successfully: {pluralize_output}")
        artifacts.register(
            phase="webcrawl",
            name="pluralized_keywords",
            path=str(pluralize_output),
            description="Pluralized keywords",
        )
    except Exception as e:
        logger.error(f"Error pluralizing keywords: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:4/6:Completed Step 4 (Keyword Pluralization)")

    # Step 5: Consolidate data
    logger.info("Step 5: Consolidating data")
    consolidated_path = str(output_path / "consolidated_data.json")
    try:
        from webcrawl.consolidate import consolidate_main

        consolidated_output = consolidate_main(
            input_path=pluralize_output, output_path=consolidated_path, log_level="INFO"
        )
        if not consolidated_output:
            raise ValueError("consolidate_main returned None or empty output.")
        logger.info(f"Data consolidated successfully: {consolidated_output}")
        artifacts.register(
            phase="webcrawl",
            name="consolidated_data",
            path=str(consolidated_output),
            description="Consolidated data JSON",
        )
    except Exception as e:
        logger.error(f"Error consolidating data: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:5/6:Completed Step 5 (Data Consolidation)")

    # Step 6: Convert to CSV
    logger.info("Step 6: Converting to CSV")
    csv_output = str(output_path / "webcrawl_output.csv")
    try:
        from webcrawl.convert_to_csv import convert_json_to_csv

        final_output = convert_json_to_csv(
            json_file_path=consolidated_output,
            csv_file_path=csv_output,
            omit_config_path=None,
        )
        if not final_output:
            raise ValueError("convert_json_to_csv returned None or empty output.")
        logger.info(f"Converted to CSV successfully: {final_output}")
        artifacts.register(
            phase="webcrawl",
            name="final_output",
            path=str(final_output),
            description="Webcrawl final CSV",
        )
    except Exception as e:
        logger.error(f"Error converting to CSV: {str(e)}")
        raise
    logger.info("PROGRESS:webcrawl:main:6/6:Completed Step 6 (CSV Conversion)")

    logger.info("Crawling & Scraping Keywords phase completed successfully")
    return final_output, artifacts


def run_integration_pipeline(
    extracting_output: str, webcrawl_output: str, output_dir: str
) -> str:
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
    logger.info(
        "PROGRESS:integration:main:0/2:Starting Final Data Integration phase"
    )  # Progress Start

    # Create necessary output directories
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Artifact management
    artifacts = PipelineArtifacts()

    # Check if extracting_output is a tuple (path, artifacts) and extract just the path
    if isinstance(extracting_output, tuple) and len(extracting_output) > 0:
        extracting_output = extracting_output[0]

    # Check if webcrawl_output is a tuple (path, artifacts) and extract just the path
    if isinstance(webcrawl_output, tuple) and len(webcrawl_output) > 0:
        webcrawl_output = webcrawl_output[0]

    # Step 1: Merge technische anlagen with keywords
    logger.info("Step 1: Merging technische anlagen with keywords")
    merged_path = str(output_path / "merged_data.csv")
    try:
        # Import locally to handle potential import errors
        from merge_pipeline.merge_technische_anlagen_with_keywords import (
            merge_csv_with_excel,
        )

        merged_output = merge_csv_with_excel(
            csv_path=webcrawl_output,
            base_data_path=extracting_output,
            output_path=merged_path,
        )
        logger.info(f"Data merged successfully: {merged_output}")
        artifacts.register(
            phase="integration",
            name="merged_csv",
            path=str(merged_output),
            description="Merged CSV from integration phase",
        )
    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        raise
    logger.info("PROGRESS:integration:main:1/2:Completed Step 1 (Data Merging)")

    # Step 2: Enrich data
    logger.info("Step 2: Enriching data with additional information")
    final_output = str(output_path / "final_output.csv")
    try:
        # Import locally to handle potential import errors
        from merge_pipeline.enrich_data import enrich_data

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
        artifacts.register(
            phase="integration",
            name="enriched_output",
            path=str(enriched_output),
            description="Enriched final output",
        )
    except Exception as e:
        logger.error(f"Error enriching data: {str(e)}")
        raise
    logger.info("PROGRESS:integration:main:2/2:Completed Step 2 (Data Enrichment)")

    logger.info("Final Data Integration phase completed successfully")
    return enriched_output, artifacts


def cleanup_intermediate_outputs(run_output_dir: Path, keep_final: bool = True) -> None:
    """
    Remove intermediate output directories for pipeline phases, keeping only the final output file and log.
    Args:
        run_output_dir: The root directory for this pipeline run (timestamped)
        keep_final: If True, keep the final output file and pipeline.log in the parent output_dir
    """
    logger = logging.getLogger(__name__)
    if not run_output_dir.exists() or not run_output_dir.is_dir():
        logger.warning(
            f"Cleanup: Run output directory does not exist: {run_output_dir}"
        )
        return
    # Remove all subdirectories (phases)
    for item in run_output_dir.iterdir():
        if item.is_dir():
            try:
                shutil.rmtree(item)
                logger.info(f"Cleaned up intermediate directory: {item}")
            except Exception as e:
                logger.warning(f"Failed to remove {item}: {e}")
    # Optionally remove the run_output_dir itself if empty
    try:
        if not any(run_output_dir.iterdir()):
            run_output_dir.rmdir()
            logger.info(f"Removed empty run directory: {run_output_dir}")
    except Exception as e:
        logger.warning(f"Failed to remove run directory {run_output_dir}: {e}")


def validate_llm_providers(check_providers: bool = True, verbose: bool = False) -> bool:
    """
    Validate that all required LLM providers are available and working.

    Args:
        check_providers (bool): Whether to actually test the providers or skip the check
        verbose (bool): Whether to enable verbose logging

    Returns:
        bool: True if all providers are available or check is skipped, False otherwise

    Raises:
        ImportError: If the test_llm_providers module is not available
    """
    if not check_providers:
        logger.info("Skipping LLM provider validation")
        return True

    logger.info("Validating LLM providers...")

    try:
        # Import here to avoid circular imports
        from utils.test_llm_providers import run_all_tests

        # Run the tests
        results = asyncio.run(run_all_tests(verbose))

        # Check if all tests passed
        all_providers_ok = True
        for provider_type, provider_results in results.items():
            for success, message in provider_results:
                if success:
                    logger.info(f"✅ {provider_type} provider is available: {message}")
                else:
                    logger.error(
                        f"❌ {provider_type} provider is not available: {message}"
                    )
                    all_providers_ok = False

        if all_providers_ok:
            logger.info("All LLM providers are available and working correctly.")
        else:
            logger.error("One or more LLM providers failed the connectivity test.")
            logger.error("The pipeline may not work correctly without all providers.")

        return all_providers_ok

    except ImportError:
        logger.error("Could not import test_llm_providers module.")
        logger.error("Please ensure the test_llm_providers.py file is available.")
        return False


def run_pipeline(config: Dict[str, Any]) -> str:
    """
    Run the complete pipeline with all components in sequence.

    Args:
        config: Configuration dictionary containing all necessary parameters

    Returns:
        str: Path to the final output file
    """
    pipeline_start_time = time.time()
    logger.info("=" * 50)
    logger.info("Pipeline Execution Started")
    logger.info("=" * 50)

    phase_status = {}
    final_output = ""  # Initialize final_output
    pipeline_phases = []  # Initialize pipeline_phases

    try:
        # Extract required configuration parameters
        input_csv = config.get("input_csv", "")
        output_dir = config.get("output_dir", "")
        # Ensure output_dir is a string
        if isinstance(output_dir, tuple) and len(output_dir) > 0:
            output_dir = output_dir[0]
        category = config.get("category")

        # Validate required parameters are present
        if not input_csv or not output_dir:
            raise ValueError("Missing required configuration: input_csv or output_dir")

        # Create timestamp for this run
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        # Use job directory directly instead of creating a pipeline_run subdirectory
        run_output_dir = Path(output_dir)

        # Create output directories for each phase
        extracting_output_dir = run_output_dir / "extracting_machine"
        webcrawl_output_dir = run_output_dir / "webcrawl"
        integration_output_dir = run_output_dir / "integration"

        for directory in [
            extracting_output_dir,
            webcrawl_output_dir,
            integration_output_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting pipeline execution at {timestamp}")
        logger.info(f"Input CSV: {input_csv}")
        logger.info(f"Output directory: {run_output_dir}")
        if category:
            logger.info(f"Filtering by category: {category}")

        # Define pipeline phases
        pipeline_phases = [
            (
                "Phase 1: Extracting Machine Assets",
                run_extracting_machine_pipeline,
                extracting_output_dir,
                {"input_csv": input_csv, "category": category},
            ),
            (
                "Phase 2: Crawling & Scraping Keywords",
                run_webcrawl_pipeline,
                webcrawl_output_dir,
                {"extracting_output": None, "category": category},
            ),
            (
                "Phase 3: Final Data Integration",
                run_integration_pipeline,
                integration_output_dir,
                {},
            ),  # Inputs depend on previous phases
        ]

        phase_outputs = {}

        # Execute phases with tqdm progress bar
        for phase_name, phase_func, phase_output_dir, phase_args in tqdm(
            pipeline_phases, desc="Pipeline Progress"
        ):
            phase_start_time = time.time()
            logger.info("-" * 50)
            logger.info(f"{phase_name} - Started")
            logger.info("-" * 50)

            try:
                # Update arguments for integration phase
                if phase_name == "Phase 2: Crawling & Scraping Keywords":
                    phase_args["extracting_output"] = phase_outputs.get(
                        "Phase 1: Extracting Machine Assets"
                    )
                    # Category is already set in the phase_args dictionary when defining pipeline_phases
                    if not phase_args["extracting_output"]:
                        raise ValueError("Missing required input for Webcrawl phase.")
                elif phase_name == "Phase 3: Final Data Integration":
                    phase_args["extracting_output"] = phase_outputs.get(
                        "Phase 1: Extracting Machine Assets"
                    )
                    phase_args["webcrawl_output"] = phase_outputs.get(
                        "Phase 2: Crawling & Scraping Keywords"
                    )
                    if (
                        not phase_args["extracting_output"]
                        or not phase_args["webcrawl_output"]
                    ):
                        raise ValueError(
                            "Missing required inputs for Integration phase."
                        )

                # Call the phase function
                output = phase_func(output_dir=str(phase_output_dir), **phase_args)

                phase_outputs[phase_name] = (
                    output  # Store output for potential use in later phases
                )
                if phase_name == "Phase 3: Final Data Integration":
                    # Extract file path from tuple if necessary
                    if isinstance(output, tuple) and len(output) > 0:
                        final_output = output[0]  # Extract just the path component
                    else:
                        final_output = output  # Capture the final output path

                phase_duration = time.time() - phase_start_time
                logger.info(f"{phase_name} - Completed in {phase_duration:.2f} seconds")
                logger.info(f"{phase_name} output: {output}")
                phase_status[phase_name] = "Success"
            except Exception as e:
                phase_duration = time.time() - phase_start_time
                logger.error(
                    f"{phase_name} - Failed after {phase_duration:.2f} seconds: {str(e)}",
                    exc_info=True,
                )
                phase_status[phase_name] = f"Failed: {str(e)}"
                raise  # Re-raise the exception to stop the pipeline

        # Copy final output to the main output directory with a descriptive name
        final_filename = f"final_export_{category or 'all'}_{timestamp}.csv"

        # Ensure final_output is a string path, not a tuple
        if isinstance(final_output, tuple) and len(final_output) > 0:
            final_output_path = final_output[0]
        else:
            final_output_path = final_output

        final_destination = Path(output_dir) / final_filename

        if (
            final_output_path and Path(final_output_path).exists()
        ):  # Check if final_output is set and exists
            shutil.copy2(final_output_path, final_destination)
            logger.info(f"Final output successfully copied to: {final_destination}")
        elif not final_output_path:
            logger.error(
                "Final output path was not generated by the integration phase."
            )
            raise ValueError(
                "Final output path was not generated by the integration phase."
            )
        else:
            logger.error(
                f"Final output file from integration phase not found: {final_output_path}"
            )
            raise FileNotFoundError(
                f"Final output file from integration phase not found: {final_output_path}"
            )

        # --- Cleanup intermediate outputs ---
        # cleanup_intermediate_outputs(run_output_dir)
        # --- End cleanup ---

        pipeline_duration = time.time() - pipeline_start_time
        logger.info("=" * 50)
        logger.info(
            f"Pipeline Execution Completed Successfully in {pipeline_duration:.2f} seconds"
        )
        logger.info("=" * 50)
        logger.info("--- Pipeline Summary ---")
        for phase, status in phase_status.items():
            logger.info(f"{phase}: {status}")
        logger.info(f"Final Output File: {final_destination}")
        logger.info("----------------------")

        return str(final_destination)

    except Exception as e:
        pipeline_duration = time.time() - pipeline_start_time
        logger.error(
            f"Pipeline execution failed after {pipeline_duration:.2f} seconds: {str(e)}",
            exc_info=True,
        )
        logger.info("=" * 50)
        logger.info("Pipeline Execution Failed")
        logger.info("=" * 50)
        logger.info("--- Pipeline Summary ---")
        # Ensure all phases are reported, even if not started
        if pipeline_phases:
            all_phase_names = [p[0] for p in pipeline_phases]
            for phase_name in all_phase_names:
                status = phase_status.get(phase_name, "Not Started")
                logger.info(f"{phase_name}: {status}")
        else:
            logger.info("Pipeline failed before phases could be defined.")
        logger.info("----------------------")
        raise


def main() -> None:
    """
    Main function that orchestrates the entire pipeline.
    """
    # Parse command-line arguments
    args = parse_arguments()

    # Validate input parameters
    validate_inputs(args.input_csv, args.output_dir)

    # Validate LLM providers first to ensure they're accessible
    if not args.skip_llm_validation:
        # Set up a temporary logger for validation messages if global logger is not configured yet
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        temp_logger = logging.getLogger("master_pipeline")
        temp_logger.info("Validating LLM providers before starting pipeline...")

        # Validate LLM providers
        llm_validation_passed = validate_llm_providers(
            check_providers=True, verbose=args.verbose
        )

        if not llm_validation_passed:
            temp_logger.error(
                "LLM provider validation failed. Pipeline cannot continue."
            )
            sys.exit(1)

        temp_logger.info("LLM provider validation passed. Proceeding with pipeline.")
    else:
        print("LLM provider validation skipped due to --skip-llm-validation flag.")

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
