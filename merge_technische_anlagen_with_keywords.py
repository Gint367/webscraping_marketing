import pandas as pd
import os
from urllib.parse import urlparse

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
        print(f"Error extracting domain from URL '{url}': {e}")
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
        print(f"Extracting domains from '{url_column_name}' column {description}")
        
        # Print sample URLs
        print(f"Example URLs {description}:")
        sample_urls = df[url_column_name].dropna().head(sample_size).tolist()
        for url in sample_urls:
            print(f"  - URL: {url}, Extracted domain: {extract_base_domain(url)}")
            
        # Extract domains
        df[column_to_create] = df[url_column_name].apply(extract_base_domain)
        return df
    else:
        print(f"Warning: Column '{url_column_name}' not found in DataFrame {description}")
        df[column_to_create] = None
        return df

def merge_csv_with_excel():
    # Define file paths
    csv_path = 'consolidated_output/pluralized_federn.csv'
    base_data_path = 'merged_federn_20250317.csv'  # Can be either .csv or .xlsx
    output_path = 'final_export_federn.csv'
    
    # Read CSV file
    print(f"Reading CSV file: {csv_path}")
    csv_data = pd.read_csv(csv_path, encoding='utf-8')
    print(f"CSV data loaded with {len(csv_data)} rows")
    
    # Read base data file (Excel or CSV)
    print(f"Reading base data file: {base_data_path}")
    file_extension = os.path.splitext(base_data_path)[1].lower()
    
    if file_extension == '.xlsx' or file_extension == '.xls':
        base_data = pd.read_excel(base_data_path, sheet_name=0)
        print(f"Excel data loaded with {len(base_data)} rows")
    elif file_extension == '.csv':
        base_data = pd.read_csv(base_data_path, encoding='utf-8')
        print(f"CSV base data loaded with {len(base_data)} rows")
    else:
        raise ValueError(f"Unsupported file format: {file_extension}. Only .xlsx, .xls, and .csv are supported.")
    
    # Filter base data to only include rows with values in 'Top1_Machine' column
    filtered_data = base_data[base_data['Top1_Machine'].notna()]
    print(f"Filtered base data to {len(filtered_data)} rows with 'Top1_Machine' values")
    
    # Select necessary columns from base data
    filtered_data = filtered_data[['Firma1', 'Ort', 'Top1_Machine', 'URL', 'Maschinen_Park_Size']] # ADD HERE IF THERES NEW COLUMNS
    
    # Rename the 'Top1_Machine' column to the specified name
    filtered_data = filtered_data.rename(columns={
        'Top1_Machine': 'technische Anlagen und Maschinen 2021/22'
    })
    
    # Clean trailing symbols from company names in base data
    original_firma1 = filtered_data['Firma1'].copy()
    filtered_data['Firma1'] = filtered_data['Firma1'].apply(clean_trailing_symbols)
    
    # Print summary of changes made
    changed_rows = filtered_data[original_firma1 != filtered_data['Firma1']]
    if not changed_rows.empty:
        print(f"Cleaned trailing symbols in {len(changed_rows)} company names:")
        for idx, row in changed_rows.iterrows():
            print(f"  - '{original_firma1[idx]}' → '{row['Firma1']}'")
    
    # Convert relevant columns to lowercase for case-insensitive merge
    csv_data['company_name_lower'] = csv_data['Company name'].str.lower()
    filtered_data['firma1_lower'] = filtered_data['Firma1'].str.lower()
    
    # Extract base domains from URLs for matching
    print("Extracting base domains from URLs...")
    
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
        print("Warning: No URL column found in CSV data. Tried: Website, Company URL, Company Url, URL, website, url")
        csv_data['base_domain'] = None
    
    # Extract domains from filtered data
    filtered_data = extract_and_log_domains(filtered_data, 'URL', description="from base data")
    
    # Print domain matching stats
    print("Domain stats:")
    csv_domains = csv_data['base_domain'].dropna().unique()
    base_domains = filtered_data['base_domain'].dropna().unique()
    common_domains = set(csv_domains).intersection(set(base_domains))
    print(f"  - CSV unique domains: {len(csv_domains)}")
    print(f"  - Base data unique domains: {len(base_domains)}")
    print(f"  - Common domains: {len(common_domains)}")
    
    # First matching attempt: by company name
    print("First matching attempt: by company name...")
    merged_data = pd.merge(
        csv_data,
        filtered_data,
        left_on='company_name_lower',
        right_on='firma1_lower',
        how='left'
    )
    
    # Count first-pass matches
    name_matched = merged_data['technische Anlagen und Maschinen 2021/22'].notna().sum()
    print(f"Matched {name_matched} companies by name")
    
    # Second matching attempt: by URL for companies not matched by name
    print("Second matching attempt: by URL for unmatched companies...")
    unmatched_idx = merged_data['technische Anlagen und Maschinen 2021/22'].isna()
    unmatched_data = merged_data[unmatched_idx]
    
    # Copy the filtered_data DataFrame to avoid modifying the original
    remaining_data = filtered_data.copy()
    url_matches = 0
    duplicate_domain_count = 0
    
    # Create a copy of merged_data for updating
    merged_data_updated = merged_data.copy()
    
    # For each unmatched row in the CSV data, try to find a match by URL
    for idx, row in unmatched_data.iterrows():
        csv_domain = row['base_domain_x'] # base_domain_x is from when pandas merges DataFrames with same column names.
        if pd.notna(csv_domain) and csv_domain:
            # Find matching rows in the base data by domain
            matches = remaining_data[remaining_data['base_domain'] == csv_domain]
            
            if not matches.empty:
                # Check if multiple companies share the same domain
                if len(matches) > 1:
                    duplicate_domain_count += 1
                    print(f"WARNING: Multiple matches found for domain '{csv_domain}':")
                    for i, match_row in matches.iterrows():
                        print(f"  - {match_row['Firma1']} (from {match_row['Ort']})")
                    print(f"  Taking the first match: '{matches.iloc[0]['Firma1']}'")
                
                # Take the first match (we can enhance this later if needed)
                match = matches.iloc[0]
                
                # Print match details for debugging
                print(f"URL match found: CSV domain '{csv_domain}' -> Base data company '{match['Firma1']}' (domain: {match['base_domain']})")
                
                # Update the merged data with the match
                for col in ['technische Anlagen und Maschinen 2021/22', 'Ort', 'URL', 'Maschinen_Park_Size']:
                    if col in match:
                        merged_data_updated.at[idx, col] = match[col]
                
                # Remove the used match from remaining_data to prevent duplicate matches
                remaining_data = remaining_data[remaining_data.index != match.name]
                url_matches += 1
    
    print(f"Additionally matched {url_matches} companies by URL")
    if duplicate_domain_count > 0:
        print(f"WARNING: Found {duplicate_domain_count} domains with multiple company matches")
    
    # Drop the temporary columns
    drop_cols = ['company_name_lower', 'firma1_lower', 'base_domain','Firma1','URL',"base_domain_x", "base_domain_y"]
    merged_data = merged_data.drop([col for col in drop_cols if col in merged_data.columns], axis=1)
    
    # Drop rows with missing values in 'technische Anlagen und Maschinen 2021/22' column
    merged_data = merged_data.dropna(subset=['technische Anlagen und Maschinen 2021/22'])
    
    # Save the merged data to a new CSV file with UTF-8-BOM encoding
    print(f"Saving merged data to: {output_path}")
    merged_data.to_csv(output_path, encoding='utf-8-sig', index=False,  sep=',')
    print(f"Successfully saved {len(merged_data)} rows to {output_path}")
    
    # Print statistics
    total_matched_rows = merged_data['technische Anlagen und Maschinen 2021/22'].notna().sum()
    print(f"Total matched: {total_matched_rows} companies (Name: {name_matched}, URL: {url_matches}) out of {len(csv_data)} from the CSV file")
    
    # Identify and save unmatched rows
    unmatched_rows = merged_data[merged_data['technische Anlagen und Maschinen 2021/22'].isna()]
    print(f"Found {len(unmatched_rows)} unmatched rows")
    if not unmatched_rows.empty:
        print("Unmatched rows:")
        print(unmatched_rows)
    
if __name__ == "__main__":
    merge_csv_with_excel()
