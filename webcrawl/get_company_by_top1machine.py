import logging
import os
import re
from urllib.parse import urlparse

import pandas as pd


def clean_url(url):
    """
    Clean the URL to preserve the scheme and domain.
    If language code like /de/ is present, preserve it.

    Args:
        url (str): The URL to be cleaned

    Returns:
        str: The URL with scheme, domain, and language code if present
    """
    try:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        domain = parsed_url.netloc
        path = parsed_url.path

        # If no scheme is present, assume https
        if not scheme:
            scheme = "https"

        # In case the URL doesn't have proper structure
        if not domain and path:
            # Try to extract domain from path
            parts = path.split('/')
            domain = parts[0]
            path = '/' + '/'.join(parts[1:]) if len(parts) > 1 else ''

        # Check for language code in the path (e.g., /de/, /en/, etc.)
        language_match = re.match(r'^\/([a-z]{2})\/.*$', path)
        if language_match:
            language_code = language_match.group(1)
            return f"{scheme}://{domain}/{language_code}/"

        return f"{scheme}://{domain}"
    except Exception as e:
        print(f"Error cleaning URL {url}: {str(e)}")
        return None


def read_urls_and_companies_by_top1machine(input_file: str = "input_excel.xlsx") -> list[tuple[str, str]]:
    """
    Read URLs and company names from an Excel or CSV file, clean the URLs to base domains,
    and filter based on the 'Top1_Machine' column not being empty. Column selection is case-insensitive.

    Args:
        input_file (str): Path to the Excel or CSV file

    Returns:
        list: List of tuples containing valid URLs and company names found in the specified columns
    """
    try:
        # Determine file type from extension
        file_extension = os.path.splitext(input_file)[1].lower()

        # Read the file based on its extension
        if file_extension == '.csv':
            df = pd.read_csv(input_file)
        elif file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(input_file, sheet_name=0)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}. Use .csv, .xlsx, or .xls")

        # Make columns lower-case for case-insensitive selection
        df.columns = [col.lower() for col in df.columns]
        required_cols = ['url', 'firma1']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in input file.")

        # Filter rows where 'url' is not empty/null
        filtered_df = df[df['url'].notna() & (df['url'] != '')]

        # Select only 'url' and 'firma1' columns
        result_df = filtered_df[['url', 'firma1']].copy()

        # Clean URLs
        result_df['url'] = result_df['url'].apply(clean_url)

        # Rename columns for clarity
        result_df.columns = ['url', 'company_name']

        # Convert to list of tuples
        result_list = list(result_df.itertuples(index=False, name=None))

        logging.info(f"Successfully processed {len(result_list)} entries from {input_file}")
        return result_list

    except Exception as e:
        logging.error(f"Error reading file {input_file}: {str(e)}")
        return []

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Read company URLs and names from a file.")
    parser.add_argument('input_file', type=str, help='Path to the input Excel or CSV file')
    args = parser.parse_args()
    urls_and_companies = read_urls_and_companies_by_top1machine(args.input_file)
    print(f"Found {len(urls_and_companies)} companies with non-empty URLs from {args.input_file}")
    print(urls_and_companies[:5])  # Print first 5 entries as a sample
