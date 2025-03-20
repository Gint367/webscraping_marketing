# Data Extraction and Processing Pipeline

## Extracting Machine Assets from Financial Statements
**Folder Path:** `extracting_machine/`

### Step 0: Extract HTML from Bundesanzeiger
```bash
python get_company_by_category.py <input_excel.xlsx> 'Category'
```
- **Input:** excel file with companies, category that are going to be extracted
- **Output:** company_<category>_BA.csv
- **Notes:** this will get list of companies and its locations

### Step 1: Extract HTML from Bundesanzeiger
```bash
python get_bundesanzeiger_html.py <input_csv> [output_dir]
```
- **Input:** CSV with company name and location
- **Output:** Folder with sanitized company name
- **Notes:** Extracts annual financial statement data from Bundesanzeiger. Uses company name and location to handle duplicate company names. Uses the first word of the location to ensure clean search queries. Metadata preserves original company name for later matching.

### Step 2: Clean HTML Content
```bash
python clean_html.py <input_dir>
```
- **Input:** HTML files from Step 1
- **Output:** JSON files
- **Notes:** Extracts available tables from source, filters tables containing "technische anlagen" and outputs as JSON.

### Step 3: Generate CSV Report
```bash
python generate_csv_report.py <input_dir>
```
- **Input:** JSON files on cleaned folder from Step 2
- **Output:** CSV report machine_report_<category>_<timestamp>.csv
- **Notes:** Extracts relevant machine asset information.

### Step 4: Merge CSV with Excel Data
```bash
python merge_csv_with_excel.py <input_file>
```
- **Input:** CSV from Step 3 and Excel dataset
- **Output:** Combined dataset merged_<category>_<timestamp>.csv
- **Notes:** Joins the generated CSV from Step 3 with the larger Excel dataset.

## Crawling & Scraping Keywords
**Folder Path:** `webcrawl/`

### Step 1: Crawl Domains
```bash
python crawl_domain.py --excel <input_file> --output <output_dir>
```
- **Input:** Company URLs
- **Output:** Markdown files of website content
- **Notes:** Crawls company websites and saves content as markdown for LLM extraction. Use `urls_and_companies =  get_company_by_top1machine.py` if continuing from `merge_excel.py`, or `get_company_by_category.py` to get the list by 'Kategorie'.

### Step 2: Extract Keywords with LLM
```bash
python extract_llm.py <input> --output <output_dir>
```
- **Input:** Markdown files from Step 1
- **Output:** Extracted keywords
- **Notes:** Extracts defined keywords (lohnfertigung, produkt, maschinen, prozess) from the markdown files.

#### Optional Steps:
- **Step 2.5:** Check LLM failures
    ```bash
    python check_llm_failures.py
    ```
    - **Output:** llm_failures.csv
    - **Notes:** Identifies values with more than one word per entry.

- **Step 2.6:** Copy failed LLM files
    ```bash
    python copy_failed_llm.py <csv_file> <source_folder> <destination_folder>
    ```
    - **Notes:** Copies failed markdown files for reprocessing with Step 2.

### Step 3: Standardize Keywords
```bash
python pluralize_with_llm.py --input <input_dir> --output <output_dir>
```
- **Input:** Extracted keywords
- **Output:** Standardized keywords
- **Notes:** Standardizes extracted keywords to plural forms. Uses different temperatures for retries to avoid wasting tokens. also translates to german.

### Step 4: Consolidate Data
```bash
python consolidate.py <input_folder>
```
- **Input:** Processed JSON folders
- **Output:** Consolidated JSON
- **Notes:** Combines multiple JSON entries into a single entry per company. Deduplicates fields, chooses longest company name, and prioritizes words containing 'machine'.

### Step 5: Populate process_type
```bash
python fill_process_type.py <input_file> --output-dir [output_directory]
```
- **Input:** JSON file with pluralized product data (e.g., pluralized_aluminiumwerke.json)
- **Output:** JSON file with prefixed name (e.g., v2_pluralized_aluminiumwerke.json)
- **Notes:** Uses a large language model (Amazon Nova Pro) to generate process_type values for companies based on their products and industry category. Fills empty process_type fields and fixes conjugation issues in existing entries. Process types are output in German plural form. Features exponential backoff for API rate limiting.

### Step 6: Convert to CSV
```bash
python convert_to_csv.py <input_json>
```
- **Input:** Consolidated JSON
- **Output:** Final CSV
- **Notes:** Extracts specific fields from JSON records and outputs to CSV. Takes only first three items and uses BOM for better umlaut compatibility with Excel.

## Additional Steps
**Folder Path:** `./`

### Merge Technical Equipment with Keywords
```bash
python merge_technische_anlagen_with_keywords.py --csv <csv_file> --base <base_data_file> --output [final_export_X.csv]
```
- **Input:** 
  - `--csv`: Path to the CSV file with company data
  - `--base`: Path to the base data file (CSV or Excel) with technical equipment information
  - `--output`: (Optional) Path where the merged output file will be saved
- **Output:** Merged dataset as specified in the output path
- **Notes:** Merges the final CSV with Excel data containing "technische anlagen und maschinen". Attempts URL matching when company names don't match directly.

### Enrich Data
```bash
python enrich_data.py
```
- **Input:** Merged dataset
- **Output:** Enhanced dataset
- **Notes:** Adds new columns "Maschinen_Park_var" & "hours_of_saving" for email marketing purposes.

### Monitor Pipeline Progress
```bash
python monitor_progress.py
```
- **Input:** None (automatically detects files in the current directory)
- **Output:** Console report showing progress through various pipeline stages
- **Notes:** Provides a comprehensive overview of the data processing pipeline status for each category. Shows counts and percentages for each step including:
  - Cleaned companies
  - Machine reports extraction
  - Merged files
  - Domain content extraction
  - LLM processing (with error rates)
  - Pluralized keyword files
  - Consolidated output status
  - Final export and data enrichment status
- **Usage:** Run from the main directory containing all the pipeline output files to get a quick status overview
