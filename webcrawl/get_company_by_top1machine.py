import pandas as pd
from urllib.parse import urlparse
import re
import os

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

def read_urls_and_companies_by_top1machine(input_file="input_excel.xlsx"):
    """
    Read URLs and company names from an Excel or CSV file, clean the URLs to base domains,
    and filter based on the 'Top1_Machine' column not being empty.
    
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
        
        # Filter rows where 'Top1_Machine' is not empty/null
        filtered_df = df[df['Top1_Machine'].notna() & (df['Top1_Machine'] != '')]
        #print(f"Filtered DataFrame shape: {filtered_df.shape}")
        #print(f"Total rows after filtering for non-empty Top1_Machine: {len(filtered_df)}")
        
        # Select only 'URL' and 'Firma1' columns
        result_df = filtered_df[['URL', 'Firma1']].copy()
        #print(f"Result DataFrame shape: {result_df.shape}")
        
        # Remove rows with empty URLs
        result_df = result_df[result_df['URL'].notna()]
        
        # Clean URLs
        result_df['URL'] = result_df['URL'].apply(clean_url)
        
        # Rename columns for clarity
        result_df.columns = ['url', 'company_name']
        
        # Convert to list of tuples
        result_list = list(result_df.itertuples(index=False, name=None))
        
        return result_list
        
    except Exception as e:
        print(f"Error reading file {input_file}: {str(e)}")
        return []

# Example usage
if __name__ == "__main__":
    # Test with Excel file
    excel_urls_and_companies = read_urls_and_companies_by_top1machine('input_excel_merged_20250310.xlsx')
    print(f"Found {len(excel_urls_and_companies)} companies with non-empty Top1_Machine from Excel")
    
    # Test with CSV file
    csv_urls_and_companies = read_urls_and_companies_by_top1machine('merged_kunststoffteile_20250314.csv')
    print(f"Found {len(csv_urls_and_companies)} companies with non-empty Top1_Machine from CSV")
    print(csv_urls_and_companies[:5])  # Print first 5 entries as a sample
