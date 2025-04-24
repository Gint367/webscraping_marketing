import argparse
import logging
import os
import re
import time
from urllib.parse import urlparse

import pandas as pd
from fuzzywuzzy import fuzz, process

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants for column selection
BASE_COLUMNS = ['Firma1', 'Ort', 'Top1_Machine', 'URL', 'Maschinen_Park_Size', 'Sachanlagen']


def clean_trailing_symbols(text):
    """Clean trailing symbols like ',' or '.' from the end of a string."""
    if pd.isna(text):
        return text
    text = str(text).strip()
    while text and text[-1] in [',', '.']:
        text = text[:-1].strip()
    return text


def extract_base_domain(url):
    """Extract the base domain from a URL."""
    if pd.isna(url) or url == "":
        return None
    try:
        # Handle URLs that might not start with http/https
        if isinstance(url, str) and not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        parsed = urlparse(str(url))
        domain = parsed.netloc

        # If netloc is empty, the URL might be malformed - try the path
        if not domain and parsed.path:
            domain = parsed.path.split('/')[0]

        # Remove 'www.' prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]

        # Extract the main domain more intelligently
        parts = domain.split('.')
        if len(parts) >= 3 and parts[-1] in ['uk', 'au', 'jp'] and parts[-2] in ['co', 'com', 'org', 'net', 'gov', 'edu']:
            # Handle cases like example.co.uk, example.com.au
            main_domain = '.'.join(parts[-3:])
            return main_domain.lower()
        elif len(parts) >= 2:
            # Handle normal cases like example.com
            main_domain = '.'.join(parts[-2:])
            return main_domain.lower()
        return domain.lower() if domain else None
    except Exception as e:
        logging.error(f"Error extracting domain from URL '{url}': {e}")
        return None


def extract_and_log_domains(df, url_column_name, column_to_create='base_domain', sample_size=5, description=""):
    """
    Helper function to extract base domains from URLs and log sample results.

    Args:
        df: DataFrame containing the URLs
        url_column_name: Name of the column containing URLs
        column_to_create: Name of the column to create with extracted domains
        sample_size: Number of sample URLs to log
        description: Description for the log output

    Returns:
        The DataFrame with the new domain column added
    """
    if url_column_name in df.columns:
        logging.info(f"Extracting domains from '{url_column_name}' column {description}")

        # Log sample URLs
        logging.debug(f"Example URLs {description}:")
        sample_urls = df[url_column_name].dropna().head(sample_size).tolist()
        for url in sample_urls:
            logging.debug(f"  - URL: {url}, Extracted domain: {extract_base_domain(url)}")

        # Extract domains
        df[column_to_create] = df[url_column_name].apply(extract_base_domain)
        return df
    else:
        logging.warning(f"Column '{url_column_name}' not found in DataFrame {description}")
        df[column_to_create] = None
        return df


def extract_industry_from_filename(filename):
    """
    Extract industry name from a CSV filename.
    Expected pattern: something_<industry>.csv or machine_report_<industry>_date.csv

    Args:
        filename: The filename to extract industry from

    Returns:
        The extracted industry name or None if no match found
    """
    # Try to match pattern like pluralized_<industry>.csv
    match = re.search(r'pluralized_([a-zA-Z0-9-]+)\.csv', filename)
    if match:
        return match.group(1)

    # Try to extract any word between underscores before .csv
    match = re.search(r'_([a-zA-Z0-9-]+)(?:_[^_]+)?\.csv$', filename)
    if match:
        return match.group(1)

    return None


def generate_output_path(input_csv_path):
    """
    Generate output path based on input CSV filename, saving to the current working directory.

    Args:
        input_csv_path: Path to the input CSV file

    Returns:
        Generated output path as final_export_<industry>.csv in the current working directory
    """
    filename = os.path.basename(input_csv_path)
    industry = extract_industry_from_filename(filename)

    if industry:
        # Create output path in the current working directory
        output_filename = f"final_export_{industry}.csv"
        return output_filename
    else:
        # Fallback to a default name if we couldn't extract industry
        return "final_export_merged.csv"


def merge_csv_with_excel(csv_path, base_data_path, output_path=None):
    start_time = time.time()

    # Generate output path if not provided
    if output_path is None:
        output_path = generate_output_path(csv_path)
        logging.info(f"Output path not provided. Automatically generated: {output_path}")

    # Read CSV file
    csv_data = pd.read_csv(csv_path, encoding='utf-8')
    logging.info(f"CSV data loaded with {len(csv_data)} rows")

    # Read base data file (Excel or CSV)
    file_extension = os.path.splitext(base_data_path)[1].lower()

    if file_extension == '.xlsx' or file_extension == '.xls':
        base_data = pd.read_excel(base_data_path, sheet_name=0)
        logging.info(f"Excel data loaded with {len(base_data)} rows")
    elif file_extension == '.csv':
        base_data = pd.read_csv(base_data_path, encoding='utf-8')
        logging.info(f"CSV base data loaded with {len(base_data)} rows")
    else:
        logging.error(f"Unsupported file format: {file_extension}. Only .xlsx, .xls, and .csv are supported.")
        raise ValueError(f"Unsupported file format: {file_extension}. Only .xlsx, .xls, and .csv are supported.")

    # Convert Maschinen_Park_Size column to string type to prevent type incompatibility issues
    if 'Maschinen_Park_Size' in base_data.columns:
        base_data['Maschinen_Park_Size'] = base_data['Maschinen_Park_Size'].astype(str)
        logging.debug("Converted Maschinen_Park_Size column in base data to string type")

    # Select necessary columns from base data (do not filter by 'Top1_Machine')
    filtered_data = base_data[BASE_COLUMNS]

    # Rename the 'Top1_Machine' column to the specified name
    filtered_data = filtered_data.rename(columns={
        'Top1_Machine': 'technische Anlagen und Maschinen 2021/22'
    })

    # Clean trailing symbols from company names in base data
    original_firma1 = filtered_data['Firma1'].copy()
    filtered_data['Firma1'] = filtered_data['Firma1'].apply(clean_trailing_symbols)

    # Log summary of changes made
    changed_rows = filtered_data[original_firma1 != filtered_data['Firma1']]
    if not changed_rows.empty:
        logging.info(f"Cleaned trailing symbols in {len(changed_rows)} company names:")
        for idx, row in changed_rows.iterrows():
            logging.debug(f"  - '{original_firma1[idx]}' → '{row['Firma1']}'") # type: ignore

    # Convert relevant columns to lowercase for case-insensitive merge
    csv_data['company_name_lower'] = csv_data['Company name'].str.lower()
    filtered_data['firma1_lower'] = filtered_data['Firma1'].str.lower()

    # Extract base domains from URLs for matching
    logging.info("Extracting base domains from URLs...")

    # Check which URL column exists in the CSV data
    url_column = None
    for possible_column in ['Website', 'Company URL', 'Company Url', 'URL', 'website', 'url']:
        if possible_column in csv_data.columns:
            url_column = possible_column
            break

    # Extract domains from both dataframes
    if url_column:
        csv_data = extract_and_log_domains(csv_data, url_column, description="from CSV data")
    else:
        logging.warning("No URL column found in CSV data. Tried: Website, Company URL, Company Url, URL, website, url")
        csv_data['base_domain'] = None

    # Extract domains from filtered data
    filtered_data = extract_and_log_domains(filtered_data, 'URL', description="from base data")

    # Log domain matching stats
    logging.info("Domain stats:")
    csv_domains = csv_data['base_domain'].dropna().unique()
    base_domains = filtered_data['base_domain'].dropna().unique()
    common_domains = set(csv_domains).intersection(set(base_domains))
    logging.info(f"  - CSV unique domains: {len(csv_domains)}")
    logging.info(f"  - Base data unique domains: {len(base_domains)}")
    logging.info(f"  - Common domains: {len(common_domains)}")

    # Create result tracking variables
    match_stats = {
        'exact_name_matches': 0,
        'fuzzy_name_matches': 0,
        'exact_url_matches': 0,
        'fuzzy_url_matches': 0,
        'unmatched': 0,
        'duplicate_domains': 0
    }

    # First matching attempt: by company name
    logging.info("First matching attempt: by company name...")

    # Create a new DataFrame to store the merged results
    merged_data = csv_data.copy()

    FUZZY_AVAILABLE = True
    if FUZZY_AVAILABLE:
        logging.info("Using fuzzy matching with token_set_ratio (threshold: 0.90)")

        # Process matching company by company
        for idx, row in csv_data.iterrows():
            company_name = row['company_name_lower']
            matched_data = None

            if pd.notna(company_name):
                # Try exact matching first
                exact_matches = filtered_data[filtered_data['firma1_lower'] == company_name]

                if not exact_matches.empty:
                    # If exact match found, use it
                    matched_data = exact_matches.iloc[0]
                    match_stats['exact_name_matches'] += 1
                    logging.debug(f"Exact name match found for '{row['Company name']}'")
                else:
                    # Try fuzzy matching
                    matches = process.extractBests(
                        company_name,
                        filtered_data['firma1_lower'].tolist(),
                        scorer=fuzz.token_set_ratio,  # Using token_set_ratio as requested
                        score_cutoff=90,  # 0.90 threshold converted to percentage
                        limit=3
                    )

                    if matches:
                        # Found fuzzy matches above threshold
                        best_match = matches[0]
                        # Handle either 2-tuple or 3-tuple format from extractBests
                        if len(best_match) >= 2:
                            match_text, match_score = best_match[0], best_match[1]

                            # Get the full row data for the match
                            match_idx = filtered_data[filtered_data['firma1_lower'] == match_text].index[0]
                            matched_data = filtered_data.loc[match_idx]
                            match_stats['fuzzy_name_matches'] += 1

                            logging.debug(f"Fuzzy match found for '{row['Company name']}' → '{matched_data['Firma1']}' (score: {match_score}%)")

                            # If there are multiple good matches, log them
                            if len(matches) > 1:
                                logging.debug(f"  Other potential matches for '{row['Company name']}':")
                                for i, match_data in enumerate(matches[1:], 1):
                                    # Safely handle different tuple lengths
                                    match_text = match_data[0]
                                    match_score = match_data[1] if len(match_data) > 1 else 'N/A'
                                    logging.debug(f"    {i}. '{match_text}' (score: {match_score}%)")

            # If we found a match (exact or fuzzy), copy the data
            if matched_data is not None:
                for col in ['technische Anlagen und Maschinen 2021/22', 'Ort', 'URL', 'Sachanlagen', 'Firma1', 'base_domain']:
                    if col in matched_data:
                        merged_data.at[idx, col] = matched_data[col]

                # Handle Maschinen_Park_Size separately with proper type conversion
                if 'Maschinen_Park_Size' in matched_data:
                    if pd.isna(matched_data['Maschinen_Park_Size']):
                        merged_data.at[idx, 'Maschinen_Park_Size'] = None
                    else:
                        merged_data.at[idx, 'Maschinen_Park_Size'] = str(matched_data['Maschinen_Park_Size'])

                # Log if both technische Anlagen und Maschinen and Sachanlagen are empty
                tech_value = matched_data.get('technische Anlagen und Maschinen 2021/22', None)
                sach_value = matched_data.get('Sachanlagen', None)
                if (pd.isna(tech_value) or tech_value == '' or tech_value is None) and (pd.isna(sach_value) or sach_value == '' or sach_value is None):
                    logging.warning(f"Both 'technische Anlagen und Maschinen 2021/22' and 'Sachanlagen' are empty for matched company: '{matched_data.get('Firma1', 'N/A')}'")

    name_matched = match_stats['exact_name_matches'] + match_stats['fuzzy_name_matches']
    logging.info(f"Matched {name_matched} companies by name (Exact: {match_stats['exact_name_matches']}, Fuzzy: {match_stats['fuzzy_name_matches']})")

    # Add a tracking column for matches instead of using technical data presence
    merged_data['is_matched'] = False
    # Mark companies that were matched by name
    for idx, row in csv_data.iterrows():
        if not pd.isna(merged_data.at[idx, 'Firma1']):
            merged_data.at[idx, 'is_matched'] = True

    # Check for unmatched companies after first pass using the tracking column
    unmatched_idx = ~merged_data['is_matched']
    unmatched_count = unmatched_idx.sum()

    if unmatched_count > 0:
        logging.info(f"Found {unmatched_count} unmatched companies after name matching")
        unmatched_data = merged_data[unmatched_idx]

        # Log information about unmatched companies
        for idx, row in unmatched_data.iterrows():
            company_name = row['Company name'] if 'Company name' in row else 'N/A'
            url = row[url_column] if url_column and url_column in row else 'N/A'
            domain = row['base_domain'] if 'base_domain' in row and pd.notna(row['base_domain']) else row.get('base_domain_x') if 'base_domain_x' in row else 'N/A'
            logging.debug(f"  - Unmatched: Company: '{company_name}', URL: '{url}', Domain: '{domain}'")

        # Second matching attempt: by URL for companies not matched by name
        logging.info("Second matching attempt: by URL for unmatched companies...")

        # IMPROVEMENT 1: Vectorized URL matching approach
        # IMPROVEMENT 2: Use sets to track used matches instead of modifying DataFrame
        used_domains = set()  # Track domains we've already matched
        matched_indices = []  # Track indices that got matched in this round

        # Create a domain-to-company lookup dictionary for faster matching
        domain_lookup = {}
        for idx, row in filtered_data.iterrows():
            domain = row['base_domain']
            if pd.notna(domain) and domain:
                if domain not in domain_lookup:
                    domain_lookup[domain] = []
                domain_lookup[domain].append(idx)

        # Process unmatched companies in a vectorized way
        for idx, row in unmatched_data.iterrows():
            csv_domain = row.get('base_domain')
            if pd.isna(csv_domain) and 'base_domain_x' in row:
                csv_domain = row['base_domain_x']

            if pd.notna(csv_domain) and csv_domain and csv_domain not in used_domains:
                # Try exact domain matching first
                if csv_domain in domain_lookup:
                    match_indices = domain_lookup[csv_domain]
                    if len(match_indices) > 0:  # Found match(es)
                        if len(match_indices) > 1:
                            match_stats['duplicate_domains'] += 1
                            company_names = [filtered_data.loc[i, 'Firma1'] for i in match_indices]
                            logging.warning(f"Multiple matches found for domain '{csv_domain}':")
                            for i, company in enumerate(company_names):
                                logging.warning(f"  - {company} (from {filtered_data.loc[match_indices[i], 'Ort']})")
                            logging.warning(f"  Taking the first match: '{company_names[0]}'")

                        match_idx = match_indices[0]  # Take first match
                        match = filtered_data.loc[match_idx]

                        # Update merged data with match
                        for col in ['technische Anlagen und Maschinen 2021/22', 'Ort', 'URL', 'Sachanlagen']:
                            if col in match:
                                merged_data.at[idx, col] = match[col]

                        # Handle Maschinen_Park_Size separately with proper type conversion
                        if 'Maschinen_Park_Size' in match:
                            if pd.isna(match['Maschinen_Park_Size']):
                                merged_data.at[idx, 'Maschinen_Park_Size'] = None
                            else:
                                merged_data.at[idx, 'Maschinen_Park_Size'] = str(match['Maschinen_Park_Size'])

                        used_domains.add(csv_domain)  # Mark domain as used
                        matched_indices.append(idx)
                        match_stats['exact_url_matches'] += 1
                        continue

                # If no exact match, try fuzzy matching
                if FUZZY_AVAILABLE:
                    # Only try to match with domains not already used
                    available_domains = [d for d in domain_lookup.keys() if d not in used_domains]
                    if available_domains:
                        fuzzy_matches = process.extractBests(
                            csv_domain,
                            available_domains,
                            scorer=fuzz.ratio,
                            score_cutoff=90,
                            limit=1
                        )

                        if fuzzy_matches:
                            best_domain, score = fuzzy_matches[0][0], fuzzy_matches[0][1]
                            match_indices = domain_lookup[best_domain]

                            if len(match_indices) > 1:
                                match_stats['duplicate_domains'] += 1
                                company_names = [filtered_data.loc[i, 'Firma1'] for i in match_indices]
                                logging.warning(f"Multiple matches for fuzzy domain '{best_domain}' (score: {score}%):")
                                for i, company in enumerate(company_names):
                                    logging.warning(f"  - {company} (from {filtered_data.loc[match_indices[i], 'Ort']})")
                                logging.warning(f"  Taking the first match: '{company_names[0]}'")

                            match_idx = match_indices[0]
                            match = filtered_data.loc[match_idx]

                            # Update merged data with match
                            for col in ['technische Anlagen und Maschinen 2021/22', 'Ort', 'URL', 'Maschinen_Park_Size', 'Sachanlagen']:
                                if col in match:
                                    merged_data.at[idx, col] = match[col]

                            used_domains.add(best_domain)  # Mark domain as used
                            matched_indices.append(idx)
                            match_stats['fuzzy_url_matches'] += 1

        url_matches = match_stats['exact_url_matches'] + match_stats['fuzzy_url_matches']
        logging.info(f"Additionally matched {url_matches} companies by URL (Exact: {match_stats['exact_url_matches']}, Fuzzy: {match_stats['fuzzy_url_matches']})")
        if match_stats['duplicate_domains'] > 0:
            logging.warning(f"Found {match_stats['duplicate_domains']} domains with multiple company matches")
    else:
        logging.info("All companies were successfully matched by name. Skipping URL matching.")
        url_matches = 0

    # Save the merged data to a new CSV file with UTF-8-BOM encoding
    logging.info(f"Saving merged data to: {output_path}")
    # Check if output_path already has .csv extension
    if not output_path.lower().endswith('.csv'):
        output_file = f"{output_path}.csv"
    else:
        output_file = output_path

    # Add a column to properly track if a company was matched by name or URL
    # A company is considered matched if it has a value in the Firma1 column or has values in technical data columns
    merged_data['was_matched'] = (
        ~merged_data['Firma1'].isna() |
        ~merged_data['technische Anlagen und Maschinen 2021/22'].isna() |
        ~merged_data['Sachanlagen'].isna() |
        ~merged_data['Maschinen_Park_Size'].isna()
    )

    # Count truly unmatched rows - those that didn't match by name or URL and have no technical data
    match_stats['unmatched'] = (~merged_data['was_matched']).sum()

    # Save reference to unmatched companies for logging later
    # We need to do this before dropping the tracking column
    still_unmatched = pd.DataFrame()  # Initialize as empty DataFrame
    if match_stats['unmatched'] > 0:
        still_unmatched = merged_data[~merged_data['was_matched']]
        unmatched_companies = []
        for idx, row in still_unmatched.iterrows():
            company_name = row.get('Company name', 'N/A')
            unmatched_companies.append(company_name)
    else:
        unmatched_companies = []

    # We keep all rows now, even if they don't have technical data values
    # Remove the temporary tracking column before saving
    merged_data = merged_data.drop('was_matched', axis=1)

    # Drop temporary columns
    drop_cols = ['company_name_lower', 'firma1_lower', 'base_domain', 'Firma1', 'URL',
                 "base_domain_x", "base_domain_y", 'is_matched']
    merged_data = merged_data.drop([col for col in drop_cols if col in merged_data.columns], axis=1)

    merged_data.to_csv(output_file, encoding='utf-8-sig', index=False, sep=',')

    # IMPROVEMENT 3: More detailed statistics
    end_time = time.time()
    elapsed_time = end_time - start_time

    total_csv_rows = len(csv_data)
    # Calculate total matches correctly - matches are the total rows minus unmatched rows
    # URL matches are already a subset of previously unmatched companies
    total_matches = total_csv_rows - match_stats['unmatched']
    # Calculate actual match percentage
    match_percentage = (total_matches / total_csv_rows) * 100

    # Log detailed statistics
    logging.info("\n" + "=" * 50)
    logging.info("MATCHING STATISTICS SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Total rows in CSV file: {total_csv_rows}")
    logging.info(f"Total matches: {total_matches} ({match_percentage:.1f}% of input)")
    logging.info(f"  - Name matches: {name_matched} ({name_matched/total_csv_rows*100:.1f}%)")
    logging.info(f"    - Exact name matches: {match_stats['exact_name_matches']} ({match_stats['exact_name_matches']/total_csv_rows*100:.1f}%)")
    logging.info(f"    - Fuzzy name matches: {match_stats['fuzzy_name_matches']} ({match_stats['fuzzy_name_matches']/total_csv_rows*100:.1f}%)")
    logging.info(f"  - URL matches: {url_matches} ({url_matches/total_csv_rows*100:.1f}%)")
    logging.info(f"    - Exact URL matches: {match_stats['exact_url_matches']} ({match_stats['exact_url_matches']/total_csv_rows*100:.1f}%)")
    logging.info(f"    - Fuzzy URL matches: {match_stats['fuzzy_url_matches']} ({match_stats['fuzzy_url_matches']/total_csv_rows*100:.1f}%)")
    logging.info(f"Unmatched: {match_stats['unmatched']} ({match_stats['unmatched']/total_csv_rows*100:.1f}%)")
    logging.info(f"Duplicate domains found: {match_stats['duplicate_domains']}")
    logging.info(f"Output rows: {len(merged_data)}")

    # Performance statistics
    logging.info(f"Processing time: {elapsed_time:.2f} seconds")
    logging.info(f"Successfully saved {len(merged_data)} rows to {output_file}")
    logging.info("=" * 50)

    # Log unmatched companies if any
    if match_stats['unmatched'] > 0:
        logging.info(f"Unmatched companies ({match_stats['unmatched']}):")
        for idx, row in still_unmatched.iterrows():
            company_name = row.get('Company name', 'N/A')
            logging.info(f"  - {company_name}")

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Merge CSV with Excel/CSV data containing technical equipment information.')

    # Add mandatory arguments
    parser.add_argument('--csv', required=True, help='Path to the CSV file with company keywords data')
    parser.add_argument('--base', required=True, help='Path to the base data file (CSV or Excel) with technical equipment information')
    parser.add_argument('--output', required=False, help='Path where the merged output file will be saved. If not provided, a name will be generated based on the input CSV')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO',
                        help='Set the logging level (default: INFO)')

    # Parse arguments
    args = parser.parse_args()

    # Set the logging level based on the command-line argument
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Call the merge function with the provided arguments
    merge_csv_with_excel(args.csv, args.base, args.output)
