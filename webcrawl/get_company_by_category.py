import pandas as pd
import re
from urllib.parse import urlparse

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

def read_urls_and_companies(input_file = "input_excel.xlsx", category = "Maschinenbauer"):
    """
    Read URLs and company names from an Excel file, clean the URLs to base domains,
    and filter based on the 'Kategorie' column.
    
    Args:
        input_file (str): Path to the Excel file
        category (str): Category to filter by
    
    Returns:
        list: List of tuples containing valid URLs ('url') and company names ('company_name') found in the specified columns
    """
    try:
        # Read the first sheet of the Excel file
        df = pd.read_excel(input_file, sheet_name=0)
        
        # Filter rows where 'Kategorie' matches the specified category
        filtered_df = df[df['Kategorie'] == category]
        print(f"Filtered DataFrame shape: {filtered_df.shape}")
        print(f"Total rows after filtering({category}): {len(filtered_df)}")
        
        # Select only 'URL' and 'Firma1' columns
        result_df = filtered_df[['URL', 'Firma1']].copy()
        print(f"Result DataFrame shape: {result_df.shape}")
        
        # Remove rows with empty URLs
        result_df = result_df[result_df['URL'].notna()]
        
        # Clean URLs
        result_df['URL'] = result_df['URL'].apply(clean_url)
        
        # Rename columns for clarity
        result_df.columns = ['Firma1', 'company name']
        
        # Convert to list of tuples
        result_list = list(result_df.itertuples(index=False, name=None))#
        
        return result_list
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return []

# Example usage:
# urls_and_companies = read_urls_and_companies('path_to_excel_file.xlsx', 'desired_category')
# print(urls_and_companies)
if __name__ == "__main__":
    urls_and_companies = read_urls_and_companies('input_excel.xlsx', 'Maschinenbauer')
    print(urls_and_companies)
