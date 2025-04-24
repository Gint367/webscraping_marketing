import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

# Constants
MAX_PRECEDING_ELEMENTS = 3
MIN_WORD_LENGTH = 5
DEFAULT_COLUMN_PREFIX = "Column"
MAX_TABLE_NAME_LENGTH = 100


def setup_logging(verbose: bool = False) -> None:
    """
    Configures the logging module for the script.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def clean_html(input_html, filter_word=None, original_filename=None):
    """Extracts tables and their preceding headers/paragraphs from the input HTML.

    Args:
        input_html (str): The input HTML content
        filter_word (str, optional): Only include tables containing this word
        original_filename (str, optional): Original filename to embed in HTML comment
    """
    soup = BeautifulSoup(input_html, "html.parser")

    # Find all tables in the HTML
    tables = soup.find_all("table")
    if not tables:
        return None  # No tables found

    # Create a new BeautifulSoup object for the cleaned HTML
    cleaned_soup = BeautifulSoup("", "html.parser")

    # Add the original filename as a hidden HTML comment if provided
    if original_filename:
        # Use a Comment object instead of new_string to prevent encoding
        from bs4.element import Comment

        filename_comment = Comment(f"original_filename: {original_filename}")
        cleaned_soup.append(filename_comment)

    # Process each table
    for table in tables:
        # Skip tables with id='begin_pub' and apply filter_word if specified
        if table.get("id") != "begin_pub" and ( # type: ignore
            not filter_word or filter_word.lower() in table.text.lower()
        ):
            # Find preceding headers and paragraphs
            preceding_elements = []
            current = table
            count = 0

            while count < MAX_PRECEDING_ELEMENTS:
                current = (
                    current.find_previous()
                )  # Use find_previous instead of find_previous_sibling
                if not current:
                    break
                if current.name == "table":  # type: ignore # Stop if we encounter another table
                    break
                if current.name == "h3":  # type: ignore # Stop if we encounter a section heading (h3)
                    break
                if current.name in ["h1", "h2", "h3", "h4", "h5", "h6", "p"]: # type: ignore
                    if (
                        current not in preceding_elements
                    ):  # Avoid duplicates if somehow found again
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
    soup = BeautifulSoup(input_html, "html.parser")
    results = []

    def meets_length_criteria(word):
        """Check if a word meets the minimum length criteria of 5 characters excluding whitespace"""
        clean_word = word.strip()
        return len(clean_word) >= MIN_WORD_LENGTH

    for table in soup.find_all("table"):
        # Get table name from preceding header or paragraph
        table_name = "Unknown Table"
        current = table
        while True:
            current = current.find_previous_sibling()
            if not current:
                break
            if current.name in ["h1", "h2", "h3", "h4", "h5", "h6", "p"]: # type: ignore
                table_name = current.text.strip()
                break

        # Get all header rows, focusing on thead first
        header_rows = []
        thead = table.find("thead") # type: ignore

        if thead:
            # If the table has a proper thead element, extract headers from it
            for row in thead.find_all("tr"): # type: ignore
                header_cells = []
                for cell in row.find_all(["th", "td"]): # type: ignore
                    text = cell.text.strip()
                    colspan = int(cell.get("colspan", 1)) # type: ignore
                    # Handle colspan by duplicating the header text across multiple columns
                    # This ensures alignment with data cells that will appear below this header
                    header_cells.extend([text] * colspan)
                header_rows.append(header_cells)
        else:
            # For tables without thead, try to identify headers from the top rows
            found_data = False
            for row in table.find_all("tr"): # type: ignore
                if row.find_all("th"): # type: ignore
                    # If row contains th elements, treat it as a header row
                    header_cells = []
                    for cell in row.find_all(["th", "td"]): # type: ignore
                        text = cell.text.strip()
                        colspan = int(cell.get("colspan", 1)) # type: ignore
                        header_cells.extend([text] * colspan)
                    header_rows.append(header_cells)
                elif not found_data:
                    # If we haven't found data yet and there's no header,
                    # use the first row with content as header
                    cells = [td.text.strip() for td in row.find_all("td")] # type: ignore
                    if any(cells):  # Check if row has any non-empty cells
                        if (
                            not header_rows
                        ):  # Only use as header if we don't have headers yet
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
                clean_cell = " ".join(cell.split())
                # For empty headers, generate a placeholder name based on position
                clean_row.append(
                    clean_cell
                    if clean_cell
                    else f"{DEFAULT_COLUMN_PREFIX}{len(clean_row) + 1}"
                )
            normalized_headers.append(clean_row)

        # Process data rows
        matching_rows = []
        # Get data rows either from tbody or by skipping header rows
        data_rows = (
            table.find("tbody").find_all("tr") # type: ignore
            if table.find("tbody") # type: ignore
            else table.find_all("tr")[len(header_rows) :] # type: ignore
        )

        for row in data_rows:
            cells = [td.text.strip() for td in row.find_all("td")] # type: ignore
            row_text = " ".join(cells)

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
                        row_dict[f"header{level}"] = header_row[: len(cells)]

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
                            header_key = (
                                last_header[i]
                                if i < len(last_header)
                                else f"{DEFAULT_COLUMN_PREFIX}{i + 1}"
                            )
                            # Handle duplicate keys by appending a numeric suffix
                            if header_key in values:
                                count = 1
                                while f"{header_key}_{count}" in values:
                                    count += 1
                                header_key = f"{header_key}_{count}"
                            values[header_key] = cell

                    row_dict["values"] = values
                    matching_rows.append(row_dict)

        if matching_rows:
            results.append(
                {
                    "table_name": " ".join(table_name.split())[
                        :MAX_TABLE_NAME_LENGTH
                    ],  # Clean table name and limit length
                    "header_levels": len(normalized_headers),
                    "matching_rows": matching_rows,
                }
            )

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
            with open(metadata_files[0], "r", encoding="utf-8") as f:
                metadata = json.load(f)
                date = datetime.strptime(metadata["date"], "%Y-%m-%dT%H:%M:%S")

                if latest_date is None or date > latest_date:
                    latest_date = date
                    latest_subfolder = subfolder_path
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.error(f"Error processing {metadata_files[0]}: {e}")
            continue

    # print(f"Latest subfolder found: {latest_subfolder}")
    # print(f"Latest date found: {latest_date}")
    return latest_subfolder


def main(
    input_dir: str,
    output_dir: Optional[str] = None,
    search_word: str = "technische Anlagen",
    verbose: bool = False,
) -> str:
    """
    Main entry point for cleaning and filtering HTML files in a directory.
    Args:
        input_dir: Path to the input directory containing company folders.
        output_dir: Path to the output directory. If None, auto-generated.
        search_word: Word to filter rows in tables.
        verbose: Enable verbose logging.
    Returns:
        The output directory path used for storing results.
    """
    setup_logging(verbose)
    if not os.path.exists(input_dir):
        logging.error(f"Input directory '{input_dir}' not found.")
        raise FileNotFoundError(f"Input directory '{input_dir}' not found.")
    input_dir_name = os.path.basename(os.path.normpath(input_dir))
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), f"{input_dir_name}_output")
    logging.info(f"Output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    # Iterate through each company folder in input directory
    for company_folder in os.listdir(input_dir):
        company_path = os.path.join(input_dir, company_folder)
        if not os.path.isdir(company_path):
            continue
        latest_subfolder = get_latest_subfolder(company_path)
        if not latest_subfolder:
            logging.warning(f"No valid subfolder found for {company_folder}")
            continue
        html_files = list(Path(latest_subfolder).glob("*.html"))
        if not html_files:
            logging.warning(f"No HTML files found in {latest_subfolder}")
            continue
        metadata_files = list(Path(latest_subfolder).glob("*metadata.json"))
        company_name = company_folder
        if metadata_files:
            try:
                with open(metadata_files[0], "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                    company_name = metadata.get("company_name", company_folder)
            except Exception as e:
                logging.error(f"Error reading metadata for {company_folder}: {e}")
        for html_file in html_files:
            try:
                with open(html_file, "r", encoding="utf-8") as f:
                    html_content = f.read()
                cleaned_html = clean_html(html_content, original_filename=company_name)
                if cleaned_html:
                    cleaned_html_output_dir = os.path.join(output_dir, "cleaned_html")
                    os.makedirs(cleaned_html_output_dir, exist_ok=True)
                    cleaned_html_file = os.path.join(
                        cleaned_html_output_dir, f"{company_folder}_cleaned.html"
                    )
                    with open(cleaned_html_file, "w", encoding="utf-8") as f:
                        f.write(cleaned_html)
                    filtered_data = filter_word_rows(cleaned_html, search_word)
                    for table in filtered_data:
                        table["company_name"] = company_name
                    if filtered_data:
                        output_file = os.path.join(
                            output_dir, f"{company_folder}_filtered.json"
                        )
                        with open(output_file, "w", encoding="utf-8") as f:
                            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
                        logging.info(f"Processed and saved results for {company_folder}")
            except Exception as e:
                logging.error(f"Error processing {html_file}: {e}")
    return os.path.abspath(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and filter HTML tables in a directory.")
    parser.add_argument("--input_dir", required=True, help="Path to the input directory containing company folders.")
    parser.add_argument("--output_dir", default=None, help="Path to the output directory.")
    parser.add_argument("--search_word", default="technische Anlagen", help="Word to filter rows in tables.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args()
    try:
        output_dir = main(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            search_word=args.search_word,
            verbose=args.verbose,
        )
        logging.info(f"Output directory: {output_dir}")
    except Exception as e:
        logging.error(f"Failed to process HTML: {e}")
        exit(1)
