# Data Extraction and Processing Pipeline

## Overview

This project aim to help a personalized email marketing campaign by combining web scraping, financial data extraction, and LLM analysis to create enriched company profiles. It estimates the size of a company's machine park using the balance sheet item **"Technische Anlagen und Maschinen"**, and extracts machine-related keywords from the company’s website. These insights help tailor outreach messages to each prospect. The Pipeline process can be run via CLI with `master_pipeline.py`. and can also be run via UI with `streamlit_app/app.py`

## Prerequisites

The pipeline requires Python 3.10+, access to the Bundesanzeiger portal, and API credentials for LLM(Amazon Bedrock & Chatgpt 4o-mini).


## Interactive Web UI for Data Extraction & Enrichment

This project includes a powerful Streamlit-based web application that provides an intuitive, multi-section interface for managing the entire data extraction and enrichment pipeline. The UI is designed for both technical and non-technical users, enabling seamless operation of complex workflows with just a few clicks.


> ![Streamlit App Main Interface](./docs/screenshots/monitor_jobstableandphase.png)

### Key UI Features

#### 1. Sidebar Navigation

- **Navigation Panel:** Instantly switch between the main sections: Input, Monitoring, and Output.
- **Persistent State:** The app remembers your last selected page and input method for a smooth user experience.

#### 2. Input Section

- **Flexible Data Input:**  
  - **File Upload:** Upload company lists in CSV or Excel format. The app previews the file, validates required columns (company name, location, url), and provides clear feedback on any issues.
  - **Manual Entry:** Use an interactive table editor to add or edit company data directly in the browser.
- **Input Format Guidance:** Expandable help section explains required columns and provides example data.
- **Validation & Feedback:** Real-time validation ensures your data is ready for processing before you proceed.
- **Start Processing:** A single button triggers the pipeline, with clear enable/disable logic based on input validity.


> ![Streamlit App Input Section](./docs/screenshots/input_fileupload.png)

#### 3. Monitoring Section

- **Job Progress Dashboard:**  
  - **Live Job Table:** View all active and historical jobs, including status, phase, and progress.
  - **Auto-Refresh:** Progress and logs update automatically at configurable intervals.
  - **Job Actions:** Cancel running jobs or delete completed/failed jobs with confirmation dialogs.
- **Detailed Status & Logs:**  
  - **Phase Tracking:** See which pipeline phase each job is in, with human-readable descriptions.
  - **Log Viewer:** Browse real-time logs for each job in a scrollable, auto-updating panel.
  - **Auto-Refresh Controls:** Easily adjust refresh intervals or pause auto-refresh for focused review.

> ![Streamlit App Monitoring Section](./docs/screenshots/monitor_jobstableandphase.png)
 ![Streamlit App Monitoring Section_2](./docs/screenshots/monitor_logview.png)

#### 4. Output Section

- **Artifact Browser:**  
  - **Job Selection:** Choose from all completed jobs to view their output artifacts.
  - **List & Tree Views:** Browse output files and folders in either a flat list or hierarchical tree.
  - **Quick Navigation:** Jump directly to key phases or folders using a dropdown.
  - **Breadcrumbs:** Always see your current location within the output directory.
  - **File Actions:**  
    - **Preview:** Instantly preview supported file types (e.g., text, CSV) within the app.
    - **Download:** Download individual files or entire folders (folders are zipped on-the-fly).
- **Final Output Highlight:** The main output file for each job is prominently displayed with a dedicated download button.

> ![Streamlit App Output Section](./docs/screenshots/output_downloadfiles.png)

#### 5. Logging & Error Handling

- **Comprehensive Logging:** All actions and errors are logged for troubleshooting and auditability.

---

**To use the Streamlit app:**
1. Install the required dependencies (`pip install -r requirements.txt`).
2. Run the app with `streamlit run streamlit_app/app.py`.
3. Open the provided local URL in your browser.


## Phase 1: Extracting Machine Assets from Financial Statements

**Folder Path:** `extracting_machine/`

### Step 0: Extract Companies by Category

```bash
python get_company_by_category.py <input_excel.xlsx> 'Category'
```

- **Input:**
  - `<input_excel.xlsx>`: Excel file containing company information with columns for name, location, and category
  - `'Category'`: Specific category name (in quotes) to filter companies by
- **Output:**
  - `company_<category>_BA.csv`: CSV file with filtered companies matching the specified category
- **Usage:** Used to create an initial dataset of companies within a specific industry category for further processing
- **Example:** `python get_company_by_category.py companies_database.xlsx 'Maschinenbau'`

### Step 1: Extract HTML from Bundesanzeiger

```bash
python get_bundesanzeiger_html.py <input_csv> [output_dir]
```

- **Input:**
  - `<input_csv>`: CSV file containing columns for company name and location (output from Step 0)
  - `[output_dir]`: Optional directory path where HTML files will be saved (defaults to a timestamped folder)
- **Output:**
  - Folder structure with sanitized company names containing HTML files of annual financial statements
  - Each company folder includes metadata preserving the original company name
- **Usage:** Extracts financial statement data from the Bundesanzeiger portal using company name and location
- **Example:** `python get_bundesanzeiger_html.py company_Maschinenbau_BA.csv bundesanzeiger_output`

### Step 2: Clean HTML Content

```bash
python clean_html.py <input_dir>
```

- **Input:**
  - `<input_dir>`: Directory containing the HTML files extracted in Step 1
- **Output:**
  - JSON files in a `cleaned_<input_dir>` subfolder with extracted table data
  - Each JSON contains filtered tables that mention "technische anlagen"
- **Usage:** Processes raw HTML to extract and filter relevant tables containing machine asset information
- **Example:** `python clean_html.py bundesanzeiger_output`

### Step 2b: Extract Sachanlagen Values with LLM

```bash
python extract_sachanlagen.py <input_dir> [--output <output_dir>] [--only-process]
```

- **Input:**
  - `<input_dir>`: Directory containing HTML files from financial statements
  - `--output <output_dir>`: Optional output directory (defaults to sachanlagen_<category>)
  - `--only-process`: Optional flag to only process existing output files and generate CSV (skip extraction)
- **Output:**
  - JSON files containing extracted Sachanlagen values for each company
  - CSV summary file with company name and largest Sachanlagen value
- **Usage:** Extracts property, plant, and equipment values using LLM, handles Teuro conversion for values under 50,000
- **Example:**
  - Full extraction: `python extract_sachanlagen.py bundesanzeiger_local_maschinenbau_output`
  - Process only: `python extract_sachanlagen.py bundesanzeiger_local_maschinenbau_output --only-process`

### Step 3: Generate CSV Report

```bash
python generate_csv_report.py <input_dir>
```

- **Input:**
  - `<input_dir>`: Directory containing the cleaned JSON files from Step 2 (typically `cleaned_<input_dir>`)
- **Output:**
  - `machine_report_<category>_<timestamp>.csv`: CSV report with structured machine asset information
  - Contains columns for company name, asset values, depreciation, and other financial metrics
- **Usage:** Transforms JSON table data into a structured CSV report for analysis
- **Example:** `python generate_csv_report.py cleaned_bundesanzeiger_output`

### Step 4: Merge CSV with Excel Data

```bash
python merge_csv_with_excel.py <csv_file_path> [--original_company_file <original_company_file_path>] [--output <output_file_path>] [--top_n <top_n>] [--sachanlagen <sachanlagen_path>] [--sheet_name <sheet_name>]
```

- **Input:**
  - `<csv_file_path>`: The CSV report generated in Step 3 (e.g., `machine_report_Maschinenbau_20250321.csv`)
  - `[--original_company_file <original_company_file_path>]`: Optional path to the master company data file (Excel or CSV, defaults to `input_excel.xlsx`).
  - `[--output <output_file_path>]`: Optional path for the output merged CSV file. If not provided, it defaults to a timestamped name like `merged_data_<YYYYMMDD>.csv`.
  - `[--top_n <top_n>]`: Optional number of top machines to consider for each company (defaults to 1).
  - `[--sachanlagen <sachanlagen_path>]`: Optional path to the CSV file containing Sachanlagen data form step 2b. If provided, this data will be merged.
  - `[--sheet_name <sheet_name>]`: Optional sheet name if the original company file is an Excel file (defaults to `Sheet1`).
- **Output:**
  - Merged CSV file (e.g., `merged_data_20250520.csv` or the specified output file path). Combined dataset with machine asset data (and optionally Sachanlagen data) joined to the original company data.
- **Usage:** Enriches the extracted machine data with additional company information from the master dataset and optionally merges Sachanlagen data.
- **Example:**
  - Basic usage: `python merge_csv_with_excel.py machine_report_Maschinenbau_20250321.csv`
  - With custom company file and output: `python merge_csv_with_excel.py machine_report_Maschinenbau_20250321.csv --original_company_file master_leads.xlsx --output merged_maschinenbau_report.csv`
  - Including Sachanlagen data: `python merge_csv_with_excel.py machine_report_Maschinenbau_20250321.csv --sachanlagen sachanlagen_report.csv --top_n 3`

## Phase 2: Crawling & Scraping Keywords

**Folder Path:** `webcrawl/`

### Step 1: Crawl Domains

```bash
python crawl_domain.py --excel <input_file> --output <output_dir>
```

- **Input:**
  - `--excel <input_file>`: Excel or CSV file containing company information including URLs to crawl
  - `--output <output_dir>`: Directory where crawled content will be saved
- **Output:**
  - Markdown files in the specified output directory, one per company website
  - Each file contains extracted text content from the company's website
- **Usage:** Crawls company websites to extract content for further LLM-based analysis
- **Example:** `python crawl_domain.py --excel merged_Maschinenbau_20250321.csv --output domain_content_maschinenbau`
- **Notes:** You can use `urls_and_companies = get_company_by_top1machine.py` if continuing from `merge_excel.py`, or `get_company_by_category.py` to get the list by 'Kategorie'.

### Step 2: Extract Keywords with LLM

```bash
python extract_llm.py <input> --output <output_dir>
```

- **Input:**
  - `<input>`: Directory containing markdown files from the domain crawling step
  - `--output <output_dir>`: Directory where extracted keyword data will be saved
- **Output:**
  - JSON files with extracted keywords categorized by type (lohnfertigung, produkt, maschinen, prozess)
  - One JSON file per company with structured keyword data
- **Usage:** Uses a large language model to analyze website content and extract business-relevant keywords.
- **Example:** `python extract_llm.py domain_content_maschinenbau --output llm_extracted_maschinenbau`
- **Notes:** 
  - By default, the script skips files that already have output files to avoid redundant processing.
  - Use the `--overwrite` or `-w` flag to force reprocessing of all files.
  - It will automatically recheck the output files for errors and reprocess them if needed.
  - specify the output folder, else it will be saved in llm_extracted_data.

#### Optional Steps

- **Step 2.5:** Check LLM failures

    ```bash
    python check_llm_failures.py <folder_path>
    ```

  - **Input:**
    - `<folder_path>` : Directory containing the extracted llm markdown.
  - **Output:**
    - `llm_failures.csv`: CSV file listing all LLM-processed files that have issues
    - Contains columns for filename and specific error types (e.g., multi-word entries)
  - **Usage:** Identifies problematic LLM outputs that need reprocessing
  - **Example:** `python check_llm_failures.py llm_extracted_maschinenbau`

- **Step 2.6:** Copy failed LLM files

    ```bash
    python copy_failed_llm.py <csv_file> <source_folder> <destination_folder>
    ```

  - **Input:**
    - `<csv_file>`: CSV file listing failed LLM extractions (from Step 2.5)
    - `<source_folder>`: Original markdown folder containing the source files
    - `<destination_folder>`: Target directory where failed files will be copied
  - **Output:**
    - Copy of all failed markdown files in the destination folder
  - **Usage:** Prepares failed files for reprocessing with the LLM
  - **Example:** `python copy_failed_llm.py llm_failures.csv domain_content_maschinenbau failed_markdown`

### Step 3: Populate process_type

```bash
python fill_process_type.py [--input-file <input_file>] [--folder <folder_path>] [--output-dir <output_directory>] [--log-level <level>]
```

- **Input:**
  - One of the following must be provided:
    - `--input-file <input_file>`: Path to a single JSON file (e.g., `consolidated_pluralized_maschinenbau.json`)
    - `--folder <folder_path>`: Path to a folder containing multiple JSON files (will process all files ending with .json)
  - `--output-dir <output_directory>`: Optional directory for output (defaults to same as input)
  - `--log-level <level>`: Optional logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL; defaults to INFO)
- **Output:**
  - `<input_filename>.json`: Enhanced JSON file with process_type values added or corrected
  - Process types are standardized to German plural forms
- **Usage:** Leverages a large language model (Amazon Nova Pro) to analyze company products and determine appropriate process types
- **Examples:**
  - Single file: `python fill_process_type.py --input-file consolidated_pluralized_maschinenbau.json --output-dir enhanced_data`
  - Multiple files: `python fill_process_type.py --folder pluralized_maschinenbau --output-dir enhanced_data`
  - With log level: `python fill_process_type.py --input-file consolidated_pluralized_maschinenbau.json --log-level DEBUG`

### Step 4: Standardize Keywords

```bash
python pluralize_with_llm.py --input <input_dir> --output <output_dir> [--temperatures <temp_values>] [--log-level <level>]
```

- **Input:**
  - `--input <input_dir>`: Directory containing the JSON files with extracted keywords
  - `--output <output_dir>`: Directory where standardized keywords will be saved
  - `--temperatures <temp_values>`: Optional list of temperature values for LLM retries (default: 0.5 0.1 1.0)
  - `--log-level <level>`: Optional logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL; defaults to INFO)
- **Output:**
  - JSON files with standardized keywords in German plural forms
  - Maintains the same structure as input files but with normalized values
- **Usage:** Standardizes keyword formats and translates terms to German plural forms for consistency
- **Example:** `python pluralize_with_llm.py --input llm_extracted_maschinenbau --output pluralized_maschinenbau`
- **Note:** The script automatically handles compound words containing:
  - Comma-separated lists (e.g., "Pumpen, Ventile, Schläuche")
  - Conjunction phrases (e.g., "Hammer und Meißel")
  - Hyphenated conjunctions (e.g., "Bohr- und Fräswerkzeuge")
  - Mixed separators (e.g., "Schrauben, Muttern und Bolzen" or "Metall- und Kunststoffteile, Gummiteile")
  
  For compound words, the script extracts the most relevant terms. For example, "Schrauben, Muttern und Bolzen" would be processed into individual terms: "Schrauben", "Bolzen", etc.

### Step 5: Consolidate Data

```bash
python consolidate.py <input_folder>
```

- **Input:**
  - `<input_folder>`: Directory containing the pluralized JSON files from Step 3
- **Output:**
  - `consolidated_<input_folder>.json`: Single JSON file with consolidated data for all companies
  - Contains deduplicated and normalized entries optimized for further processing
- **Usage:** Combines multiple JSON entries into a unified dataset, removing duplicates and standardizing formats
- **Example:** `python consolidate.py pluralized_maschinenbau`

### Step 6: Convert to CSV

```bash
python convert_to_csv.py <input_json>
```

- **Input:**
  - `<input_json>`: Enhanced JSON file with process types from Step 5
- **Output:**
  - CSV file with the same base name as the input JSON
  - Contains selected fields from the JSON records with BOM encoding for Excel compatibility
- **Usage:** Transforms the consolidated JSON data into a tabular CSV format for analysis and reporting
- **Example:** `python convert_to_csv.py consolidated_pluralized_maschinenbau.json`

## Phase 3: Finishing Steps

**Folder Path:** `./`

### Merge technische Anlagen & Sachanlagen with scraped keywords

```bash
python merge_technische_anlagen_with_keywords.py --csv <csv_file> --base <base_data_file> [--output <output_file>] [--log-level <level>]
```

- **Input:**
  - `--csv <csv_file>`: CSV file with keyword data (output from convert_to_csv.py)
  - `--base <base_data_file>`: CSV or Excel file containing technical equipment information
  - `--output <output_file>`: Optional output filename (defaults to final_export_<category>.csv)
  - `--log-level <level>`: Optional logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL; defaults to INFO)
- **Output:**
  - CSV file combining keyword data with technical equipment information
  - Contains all columns from both inputs with matching records merged
- **Usage:** Creates a comprehensive dataset linking technical equipment data with extracted keywords
- **Example:** `python merge_technische_anlagen_with_keywords.py --csv archive/consolidated_output/pluralized_maschinenbauer.csv --base merged_maschinenbauer_20250404.csv`
- **Notes:**
  - Implements fuzzy string matching with token_set_ratio at 90% threshold for more accurate company name matching
  - Uses vectorized operations for URL matching to improve performance with large datasets
  - Uses sets to efficiently track matched domains instead of modifying DataFrames

### Enrich Data

```bash
python enrich_data.py --input <input_file> --output <output_file>
```

- **Input:**
  - `--input <input_file>`: Merged dataset from the previous step (implicitly defined in script if not provided)
  - `--output <output_file>`: Optional output filename (defaults to enriched_<input_filename>)
- **Output:**
  - Enhanced CSV with additional derived columns:
    - "Maschinen_Park_var": Calculated machine park value
    - "hours_of_saving": Estimated time savings metric
- **Usage:** Adds calculated metrics useful for email marketing and business analysis
- **Example:** `python enrich_data.py --input final_export_maschinenbau.csv --output enriched_maschinenbau.csv`

## Extra

### Convert Excel to CSV

```bash
python convert_excel_to_csv.py <excel_file> [options]
```

- **Input:**
  - `<excel_file>`: Path to the Excel file (.xlsx or .xls)
  - **Options:**
    - `--list-sheets`: Lists all sheets in the Excel file
    - `--sheet <sheet_name>`: Converts specified sheet to CSV
    - `--all-sheets`: Converts all sheets to separate CSV files
    - `--output-dir <directory>`: Specifies output directory for CSV files
    - `--output <file>`: Specifies output file name (only for single sheet)
    - `--interactive`: Interactive mode for selecting sheets
- **Output:**
  - One or more CSV files depending on selected options
  - Files are named based on sheet names or specified output names
- **Usage:** Utility script for converting Excel data to CSV format for use in the pipeline
- **Examples:**

  ```bash
  python convert_excel_to_csv.py data.xlsx --list-sheets
  python convert_excel_to_csv.py data.xlsx --sheet Sheet1 --output output.csv
  python convert_excel_to_csv.py data.xlsx --all-sheets --output-dir csv_output
  python convert_excel_to_csv.py data.xlsx --interactive
  ```

### Monitor Pipeline Progress

```bash
python monitor_progress.py
```

- **Input:**
  - None (automatically scans the current directory for pipeline files and folders)
- **Output:**
  - Console report with detailed statistics on pipeline progress
  - Includes counts and percentages for each processing stage by category
- **Usage:** Provides a comprehensive overview of the current state of the data processing pipeline
- **Example:** `python monitor_progress.py`
- **Notes:**
  - Sometimes the # of files in LLM extracted is more than whats inside the crawled pages. this is because some sites redirects to other website and causes the file to be created twice.
- **Display Metrics:**
  - Cleaned companies: Number of companies with cleaned HTML files
  - Machine reports: Count of successfully generated machine reports
  - Merged files: Count of successfully merged datasets
  - Domain content: Number of successfully crawled company websites
  - LLM processing: Success and error rates for keyword extraction
  - Pluralized keywords: Count of standardized keyword files
  - Consolidated output: Status of data consolidation
  - Final export and enrichment: Status of final data products
