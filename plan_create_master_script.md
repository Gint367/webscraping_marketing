# Development Plan for Pipeline Automation

## Overview
This document outlines the development plan for creating a master script that automates the entire data extraction and processing pipeline. The goal is to enable users to provide a CSV file with company information (name, location, URL) and have the script automatically process this data through all pipeline stages, returning an enriched CSV with all the scraped and analyzed data.

## Current Pipeline Structure
The current pipeline consists of multiple independent scripts that need to be run sequentially, with manual intervention between steps. The scripts are organized into several key phases:

1. **Extracting Machine Assets from Financial Statements**
   - Extract companies by category
   - Extract HTML from Bundesanzeiger
   - Clean HTML content
   - Extract Sachanlagen values with LLM
   - Generate CSV report
   - Merge CSV with Excel data

2. **Crawling & Scraping Keywords**
   - Crawl domains
   - Extract keywords with LLM
   - Standardize keywords (pluralize)
   - Consolidate data
   - Populate process_type
   - Convert to CSV

3. **Final Data Integration**
   - Merge technical equipment with keywords
   - Enrich data

## Required Changes

Each script in the pipeline needs to be modified to:
- Accept command-line parameters for input/output paths
- Return standardized output paths or status information
- Handle errors gracefully
- Support being called programmatically from another script

## Implementation Tasks

### 1. Script Modifications

#### Extracting Machine Assets
- [ ] Modify `get_company_by_category.py` to accept parameters and return output path
- [ ] Modify `get_bundesanzeiger_html.py` to accept parameters and return output directory
- [ ] Modify `clean_html.py` to accept parameters and return output directory
- [ ] Modify `extract_sachanlagen.py` to accept parameters and return output directory/file
- [ ] Modify `generate_csv_report.py` to accept parameters and return output file path
- [ ] Modify `merge_csv_with_excel.py` to accept parameters for Excel file and return output path

#### Crawling & Scraping Keywords
- [ ] Modify `crawl_domain.py` to accept parameters and return output directory
- [ ] Modify `extract_llm.py` to handle failures internally and return output directory
- [ ] Modify `check_llm_failures.py` (optional) to accept parameters and return failure list
- [ ] Modify `copy_failed_llm.py` (optional) to accept parameters and work with the master script
- [ ] Modify `pluralize_with_llm.py` to accept parameters and return output directory
- [ ] Modify `consolidate.py` to accept parameters and return output file path
- [ ] Modify `fill_process_type.py` to accept parameters and return output file path
- [ ] Modify `convert_to_csv.py` to accept parameters and return output file path

#### Final Data Integration
- [ ] Modify `merge_technische_anlagen_with_keywords.py` to accept parameters and return output file
- [ ] Modify `enrich_data.py` to accept parameters and return output file path

### 2. Master Script Development

- [ ] Create `master_pipeline.py` with the following components:
  - [ ] Command-line argument parsing for input CSV and category
  - [ ] Configuration management for API keys, paths, and settings
  - [ ] Sequential execution of all pipeline steps
  - [ ] Comprehensive error handling and logging
  - [ ] Progress tracking and reporting
  - [ ] Output file path generation

### 3. Configuration Management

- [ ] Create a `config.py` or `config.json` file to store:
  - [ ] API keys for LLMs (Amazon Bedrock, ChatGPT)
  - [ ] Default paths and directories
  - [ ] Processing parameters (e.g., matching thresholds, LLM temperatures)
  - [ ] Retry configurations

### 4. Testing and Validation

- [ ] Create test cases with small sample data
- [ ] Implement validation checks between pipeline steps
- [ ] Add unit tests for critical functions
- [ ] Create an end-to-end test case

### 5. Documentation

- [ ] Update README.md with master script usage instructions
- [ ] Document configuration options and requirements
- [ ] Create a troubleshooting guide for common issues

## Script Modification Details

Below are the specific modifications needed for each script to make it compatible with the master pipeline:

### get_company_by_category.py
- Modify to accept input CSV and category parameters
- Return the path to the generated output CSV
- Handle the case where no filter is needed if CSV already contains appropriate data

### get_bundesanzeiger_html.py
- Ensure it accepts input CSV and output directory parameters
- Add retry logic for failed requests
- Return the path to the output directory
- Add error handling for authentication or connection issues

### clean_html.py
- Accept input directory parameter
- Return the path to the cleaned output directory
- Add progress reporting

### extract_sachanlagen.py
- Accept input directory and output directory parameters
- Add internal retry mechanism for LLM failures
- Return the path to the output CSV
- Include proper error handling for API issues

### generate_csv_report.py
- Accept input directory parameter
- Return the path to the generated CSV file
- Add error handling for parsing issues

### merge_csv_with_excel.py
- Accept input CSV and Excel file parameters
- Remove hardcoded Excel path dependencies
- Return the path to the merged output CSV

### crawl_domain.py
- Accept input CSV/Excel and output directory parameters
- Add retry logic for failed crawls
- Return the path to the output directory
- Add progress reporting and error handling

### extract_llm.py
- Accept input directory and output directory parameters
- Implement internal failure detection and retry
- Return the path to the output directory
- Add proper error handling for API issues

### pluralize_with_llm.py
- Accept input directory and output directory parameters
- Return the path to the output directory
- Add proper error handling for API issues

### consolidate.py
- Accept input directory parameter
- Return the path to the consolidated JSON file
- Add error handling for file access issues

### fill_process_type.py
- Accept input file and output directory parameters
- Return the path to the enhanced JSON file
- Add proper error handling for API issues

### convert_to_csv.py
- Accept input JSON file parameter
- Return the path to the output CSV file
- Add error handling for conversion issues

### merge_technische_anlagen_with_keywords.py
- Accept CSV file and base data file parameters
- Return the path to the final merged CSV
- Add error handling for matching issues

### enrich_data.py
- Accept input file parameter
- Return the path to the enriched output CSV
- Add error handling for calculation issues

## Master Script Functionality

The master script should:

1. Parse command-line arguments for:
   - Input CSV path
   - Category (if filtering is needed)
   - Output directory
   - Configuration file path (optional)
   - Debug/verbose mode flags

2. Validate the input CSV has required columns:
   - Company name
   - Location
   - URL (if web crawling is needed)

3. Set up logging with appropriate level based on verbose flag

4. Execute each pipeline step in sequence:
   - Pass output from previous steps to subsequent steps
   - Handle errors at each step
   - Log progress and status

5. Implement a progress tracking mechanism that:
   - Records which companies have completed each step
   - Allows resuming from failure points
   - Provides status updates

6. Return a final enriched CSV file with all extracted data

## Error Handling Strategy

The master script should implement these error handling strategies:

1. **Graceful degradation**: If a step fails for a specific company, continue with other companies
2. **Retry logic**: For transient errors (network issues, rate limits), retry with exponential backoff
3. **Checkpoint system**: Save progress after each major step to allow resuming
4. **Detailed logging**: Record errors with context for troubleshooting
5. **Failure summaries**: Generate a report of any companies that couldn't be fully processed

## Notes for Implementation

- When modifying scripts, maintain backward compatibility when possible so they can still be run individually
- Consider using a config file to store API keys and other sensitive information
- Add progress bars or status indicators for long-running processes
- Consider implementing multiprocessing for steps that can be parallelized
- Ensure proper cleanup of temporary files
- Validate outputs at each step to catch issues early

This development plan provides a structured approach to implementing the automated pipeline. By following these tasks sequentially, we can create a robust master script that handles the entire data extraction and processing workflow with minimal manual intervention.
