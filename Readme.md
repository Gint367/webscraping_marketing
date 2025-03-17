# Data Extraction and Processing Pipeline

## Extracting Machine Assets from Financial Statements
**Folder Path:** `extracting_machine/`

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
- **Input:** JSON files from Step 2
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
python extract_llm.py
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
    python copy_failed_llm.py
    ```
    - **Notes:** Copies failed markdown files for reprocessing with Step 2.

### Step 3: Standardize Keywords
```bash
python pluralize_with_llm.py
```
- **Input:** Extracted keywords
- **Output:** Standardized keywords
- **Notes:** Standardizes extracted keywords to plural forms. Uses different temperatures for retries to avoid wasting tokens.

### Step 4: Consolidate Data
```bash
python consolidate.py
```
- **Input:** Processed JSON files
- **Output:** Consolidated JSON
- **Notes:** Combines multiple JSON entries into a single entry per company. Deduplicates fields, chooses longest company name, and prioritizes words containing 'machine'.

### Step 5: Convert to CSV
```bash
python convert_to_csv.py
```
- **Input:** Consolidated JSON
- **Output:** Final CSV
- **Notes:** Extracts specific fields from JSON records and outputs to CSV. Takes only first three items and uses BOM for better umlaut compatibility with Excel.

## Additional Steps
**Folder Path:** `root/`

### Merge Technical Equipment with Keywords
```bash
python merge_technische_anlagen_with_keywords.py
```
- **Input:** Final CSV and Excel with technical equipment data
- **Output:** Merged dataset
- **Notes:** Merges the final CSV with Excel data containing "technische anlagen und maschinen". Attempts URL matching when company names don't match directly.

### Enrich Data
```bash
python enrich_data.py
```
- **Input:** Merged dataset
- **Output:** Enhanced dataset
- **Notes:** Adds new columns "Maschinen_Park_var" & "hours_of_saving" for email marketing purposes.
