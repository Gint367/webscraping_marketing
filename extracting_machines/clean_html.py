import json
import os
import csv
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Constants
MAX_PRECEDING_ELEMENTS = 3
MIN_WORD_LENGTH = 5
DEFAULT_COLUMN_PREFIX = "Column"
MAX_TABLE_NAME_LENGTH = 100

def clean_html(input_html, filter_word=None):
    """Extracts tables and their preceding headers/paragraphs from the input HTML.
    
    Args:
        input_html (str): The input HTML content
        filter_word (str, optional): Only include tables containing this word
    """
    soup = BeautifulSoup(input_html, 'html.parser')
    
    # Find all tables in the HTML
    tables = soup.find_all('table')
    if not tables:
        return None  # No tables found

    # Create a new BeautifulSoup object for the cleaned HTML
    cleaned_soup = BeautifulSoup('', 'html.parser')
    
    # Process each table
    for table in tables:
        # Skip tables with id='begin_pub' and apply filter_word if specified
        if table.get('id') != 'begin_pub' and (not filter_word or filter_word.lower() in table.text.lower()):
            # Find preceding headers and paragraphs
            preceding_elements = []
            current = table
            count = 0
            
            while count < MAX_PRECEDING_ELEMENTS:
                current = current.find_previous() # Use find_previous instead of find_previous_sibling
                if not current:
                    break
                if current.name == 'table': # Stop if we encounter another table
                    break
                if current.name == 'h3': # Stop if we encounter a section heading (h3)
                    break
                if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']:
                    if current not in preceding_elements: # Avoid duplicates if somehow found again
                        preceding_elements.append(current)
                        count += 1
            
            # Add elements in correct order
            for element in reversed(preceding_elements):
                cleaned_soup.append(element)
            cleaned_soup.append(table)

    return str(cleaned_soup)

def filter_word_rows(input_html, search_word):
    """Extracts rows containing the search word from tables along with their headers.
    
    Args:
        input_html (str): The input HTML content
        search_word (str): Word to search for in table rows
        
    Returns:
        list: List of dictionaries containing table data with matching rows
    """
    soup = BeautifulSoup(input_html, 'html.parser')
    results = []
    
    def meets_length_criteria(word):
        """Check if a word meets the minimum length criteria of 5 characters excluding whitespace"""
        clean_word = word.strip()
        return len(clean_word) >= MIN_WORD_LENGTH
    
    for table in soup.find_all('table'):
        # Get table name from preceding header or paragraph
        table_name = "Unknown Table"
        current = table
        while True:
            current = current.find_previous_sibling()
            if not current:
                break
            if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']:
                table_name = current.text.strip()
                break

        # Get all header rows, focusing on thead first
        header_rows = []
        thead = table.find('thead')
        
        if thead:
            # If the table has a proper thead element, extract headers from it
            for row in thead.find_all('tr'):
                header_cells = []
                for cell in row.find_all(['th', 'td']):
                    text = cell.text.strip()
                    colspan = int(cell.get('colspan', 1))
                    # Handle colspan by duplicating the header text across multiple columns
                    # This ensures alignment with data cells that will appear below this header
                    header_cells.extend([text] * colspan)
                header_rows.append(header_cells)
        else:
            # For tables without thead, try to identify headers from the top rows
            found_data = False
            for row in table.find_all('tr'):
                if row.find_all('th'):
                    # If row contains th elements, treat it as a header row
                    header_cells = []
                    for cell in row.find_all(['th', 'td']):
                        text = cell.text.strip()
                        colspan = int(cell.get('colspan', 1))
                        header_cells.extend([text] * colspan)
                    header_rows.append(header_cells)
                elif not found_data:
                    # If we haven't found data yet and there's no header,
                    # use the first row with content as header
                    cells = [td.text.strip() for td in row.find_all('td')]
                    if any(cells):  # Check if row has any non-empty cells
                        if not header_rows:  # Only use as header if we don't have headers yet
                            header_rows.append(cells)
                        found_data = True  # Mark that we've found data rows

        if not header_rows:
            continue  # Skip tables without identifiable headers

        # Normalize headers: clean up text and handle empty headers
        # This creates consistent header values for mapping to data cells
        normalized_headers = []
        for row in header_rows:
            clean_row = []
            for cell in row:
                # Remove extra whitespace and newlines from header text
                clean_cell = ' '.join(cell.split())
                # For empty headers, generate a placeholder name based on position
                clean_row.append(clean_cell if clean_cell else f"{DEFAULT_COLUMN_PREFIX}{len(clean_row)+1}")
            normalized_headers.append(clean_row)
            
        # Process data rows
        matching_rows = []
        # Get data rows either from tbody or by skipping header rows
        data_rows = (table.find('tbody').find_all('tr') if table.find('tbody') else 
                    table.find_all('tr')[len(header_rows):])
            
        for row in data_rows:
            cells = [td.text.strip() for td in row.find_all('td')]
            row_text = ' '.join(cells)
            
            # Find the position of search word in the text
            match_pos = row_text.lower().find(search_word.lower())
            
            if match_pos >= 0:
                # Check if the match is valid by examining what comes before it
                is_valid_match = True
                
                # Get text before search word
                text_before = row_text[:match_pos].strip()
                
                if text_before:
                    # Check the last word before search word
                    # If it's longer than 5 characters, the match is invalid
                    words_before = text_before.split()
                    if words_before and meets_length_criteria(words_before[-1]):
                        is_valid_match = False
                
                if is_valid_match:
                    # Create row dictionary
                    row_dict = {}
                    
                    # Add all header levels to preserve the table's hierarchical structure
                    # This keeps track of all header rows that this data belongs to
                    for level, header_row in enumerate(normalized_headers, 1):
                        # Limit headers to the actual number of columns in this data row
                        row_dict[f'header{level}'] = header_row[:len(cells)]
                    
                    # Create values dictionary mapping headers to cell values
                    values = {}
                    # Use the last (most specific) header row for column names
                    last_header = normalized_headers[-1] if normalized_headers else []
                    
                    for i, cell in enumerate(cells):
                        if i == 0:
                            # Always use a consistent key for the first column
                            values[f"{DEFAULT_COLUMN_PREFIX}1"] = cell
                        else:
                            # For other columns, try to use the header text as key
                            header_key = last_header[i] if i < len(last_header) else f"{DEFAULT_COLUMN_PREFIX}{i+1}"
                            # Handle duplicate keys by appending a numeric suffix
                            if header_key in values:
                                count = 1
                                while f"{header_key}_{count}" in values:
                                    count += 1
                                header_key = f"{header_key}_{count}"
                            values[header_key] = cell
                    
                    row_dict['values'] = values
                    matching_rows.append(row_dict)
        
        if matching_rows:
            results.append({
                'table_name': table_name[:MAX_TABLE_NAME_LENGTH],
                'header_levels': len(normalized_headers),
                'matching_rows': matching_rows
            })
    
    return results

def get_latest_subfolder(company_folder):
    """Find the subfolder with the latest date from *_metadata.json"""
    latest_date = None
    latest_subfolder = None
    
    for subfolder in os.listdir(company_folder):
        subfolder_path = os.path.join(company_folder, subfolder)
        if not os.path.isdir(subfolder_path):
            continue
            
        # Look for any file ending with _metadata.json
        metadata_files = list(Path(subfolder_path).glob("*metadata.json"))
        if not metadata_files:
            continue
            
        try:
            with open(metadata_files[0], 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                date = datetime.strptime(metadata['date'], '%Y-%m-%dT%H:%M:%S')
                
                if latest_date is None or date > latest_date:
                    latest_date = date
                    latest_subfolder = subfolder_path
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error processing {metadata_files[0]}: {e}")
            continue
    
    print(f"Latest subfolder found: {latest_subfolder}")     
    print(f"Latest date found: {latest_date}")   
    return latest_subfolder


if __name__ == "__main__":
    input_dir = "./bundesanzeiger_local_data"
    output_dir = f"{input_dir}_output"
    os.makedirs(output_dir, exist_ok=True)
    
    search_word = "technische Anlagen"
    
    # Iterate through each company folder in input directory
    for company_folder in os.listdir(input_dir):
        company_path = os.path.join(input_dir, company_folder)
        if not os.path.isdir(company_path):
            continue
            
        # Get the latest subfolder for this company
        latest_subfolder = get_latest_subfolder(company_path)
        if not latest_subfolder:
            print(f"No valid subfolder found for {company_folder}")
            continue
            
        # Look for HTML files in the latest subfolder
        html_files = list(Path(latest_subfolder).glob("*.html"))
        if not html_files:
            print(f"No HTML files found in {latest_subfolder}")
            continue
            
        # Process each HTML file
        for html_file in html_files:
            try:
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Clean and filter the HTML content
                cleaned_html = clean_html(html_content)
                
                # Save the cleaned HTML to a different folder
                cleaned_html_output_dir = os.path.join(output_dir, "cleaned_html")
                os.makedirs(cleaned_html_output_dir, exist_ok=True)
                cleaned_html_file = os.path.join(cleaned_html_output_dir, f"{company_folder}_cleaned.html")
                with open(cleaned_html_file, 'w', encoding='utf-8') as f:
                    f.write(cleaned_html)
                    
                if cleaned_html:
                    filtered_data = filter_word_rows(cleaned_html, search_word)
                    
                    if filtered_data:
                        # Create output JSON file
                        output_file = os.path.join(output_dir, f"{company_folder}_filtered.json")
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
                        print(f"Processed and saved results for {company_folder}")
                    
            except Exception as e:
                print(f"Error processing {html_file}: {e}")


