import logging
import os
import re
from urllib.parse import urlparse

import pandas as pd


def clean_url_to_base_domain(url):
    """
    Clean a URL to get just the base domain, but preserve the /de/ language code if present.

    For example:
    - https://example.com/page/product -> https://example.com/
    - https://example.com/de/page/product -> https://example.com/de/
    - https://example.de/shop/item?id=123 -> https://example.de/

    Args:
        url (str): The URL to clean

    Returns:
        str: The cleaned URL with only the base domain (and /de/ if present)
    """
    try:
        parsed = urlparse(url)

        # Get the base URL (scheme + domain)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        # Check if there's a German language code in the path
        if parsed.path and '/de/' in parsed.path.lower():
            # Extract everything up to and including '/de/'
            match = re.search(r'^(.*?/de/)', parsed.path, re.IGNORECASE)
            if match:
                return base_url + match.group(1)
            else:
                # Fallback if regex doesn't match but '/de/' is in the path
                return base_url + '/de/'

        # No language code found, return just the base domain with trailing slash
        return base_url + '/'
    except Exception as e:
        # If any error occurs, return the original URL
        logging.warning(f"Error cleaning URL {url}: {str(e)}")
        return url


def read_urls_from_excel(file_path, sheet_name="Sheet1", column_identifier_1="URL", machine_park_column="Technische Anlagen und Maschinen in € 2021/22", column_identifier_2="Firma1"):
    """
    Read URLs and company names from an Excel file and clean the URLs to base domains.
    Only include URLs from rows where the 'Maschine Park size' column is not empty.

    Args:
        file_path (str): Path to the Excel file
        sheet_name (str): Name of the sheet containing URLs (default: Sheet1)
        column_name (str): Name of the column containing URLs (default: URL)
        machine_park_column (str): Name of the column for Maschine Park size filtering (default: Maschine Park size)
        company_column (str): Name of the column containing company names (default: Firma1)

    Returns:
        list: List of tuples containing valid URLs('url') and company names('company_name') found in the specified columns
    """
    urls_and_companies = []

    # Check if file exists
    if not os.path.exists(file_path):
        logging.error(f"Excel file not found: {file_path}")
        return urls_and_companies

    try:
        # Read the Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Check if the specified columns exist
        if column_identifier_1 not in df.columns:
            logging.error(f"Column '{column_identifier_1}' not found in sheet '{sheet_name}'")
            return urls_and_companies

        if machine_park_column not in df.columns:
            logging.warning(f"Column '{machine_park_column}' not found in sheet '{sheet_name}'. Using all rows.")
            has_machine_park_column = False
        else:
            has_machine_park_column = True

        if column_identifier_2 not in df.columns:
            logging.error(f"Column '{column_identifier_2}' not found in sheet '{sheet_name}'")
            return urls_and_companies

        # Extract URLs and company names from the columns
        for idx, row in df.iterrows():
            # Skip rows where the machine_park_column is empty
            if has_machine_park_column and pd.isna(row[machine_park_column]):
                continue

            url = row[column_identifier_1]
            company_name = row[column_identifier_2]

            # Skip empty cells
            if pd.isna(url) or not url or pd.isna(company_name) or not company_name:
                continue

            # Convert to string in case it's a number
            url = str(url).strip()
            company_name = str(company_name).strip()

            # Basic URL validation
            if not url.startswith(('http://', 'https://')):
                # Try to fix the URL by adding https:// prefix
                url = 'https://' + url

            # Further validation with urlparse
            parsed = urlparse(url)
            if parsed.netloc:  # Check if domain exists
                # Clean the URL to get just the base domain (with /de/ if present)
                cleaned_url = clean_url_to_base_domain(url)
                urls_and_companies.append((cleaned_url, company_name))
            else:
                logging.warning(f"Invalid URL at row {idx+2}: {url}") # type: ignore

        # Remove duplicates while preserving order
        unique_urls_and_companies = []
        seen = set()
        for url, company in urls_and_companies:
            if url not in seen:
                seen.add(url)
                unique_urls_and_companies.append((url, company))

        logging.info(f"Found {len(unique_urls_and_companies)} valid unique base URLs with company names in '{file_path}' (filtered by '{machine_park_column}')")
        return unique_urls_and_companies

    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")

    return urls_and_companies
