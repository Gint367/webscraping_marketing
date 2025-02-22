# Test Documentation for Clean HTML Scraper

## Test Cases Overview

### Basic HTML Cleaning Tests

#### `test_clean_html_empty()`
- Tests handling of empty input
- Verifies that empty HTML or empty string returns None
- Ensures proper error handling for minimal input cases

#### `test_clean_html_no_tables()`
- Tests HTML content without any tables
- Verifies that HTML without tables returns None
- Ensures scraper doesn't process HTML without relevant content

#### `test_clean_html_with_filter()`
- Tests filtering functionality with keyword "technische anlagen"
- Verifies that tables containing the filter word are kept
- Checks if preceding headers are properly included
- Validates basic HTML structure preservation

### Table Content Processing Tests

#### `test_filter_word_rows_basic()`
- Tests basic row filtering functionality using sample HTML
- Validates table name extraction from headers
- Verifies matching row detection
- Ensures proper structure of returned results

#### `test_filter_word_rows_headers()`
- Tests complex table header processing
- Verifies colspan attribute handling
- Validates header normalization
- Checks proper mapping of headers to data cells

### Metadata and Folder Structure Tests

#### `test_get_latest_subfolder()`
- Tests folder traversal and date comparison
- Validates JSON metadata reading
- Verifies correct identification of newest folder
- Tests proper date parsing and comparison

#### `test_get_latest_subfolder_empty()`
- Tests behavior with empty folders
- Verifies proper handling of folders without metadata
- Ensures None is returned for invalid folder structures

#### `test_get_latest_subfolder_invalid_json()`
- Tests error handling for corrupted metadata files
- Verifies graceful handling of invalid JSON
- Ensures system stability with malformed data

## Test Fixtures

### `sample_html()`
- Provides standard HTML test data
- Used for consistent testing across multiple test cases
- Contains known table structures and content

### `temp_company_folder()`
- Creates temporary test directory structure
- Manages cleanup after tests
- Provides isolated environment for folder-related tests

## Usage

Run tests using pytest:
```bash
pytest test_clean_html.py -v
```

For detailed test coverage:
```bash
pytest test_clean_html.py --cov=clean_html
```
