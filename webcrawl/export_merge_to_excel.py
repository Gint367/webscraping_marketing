import pandas as pd
import os
import re
import argparse
from urllib.parse import urlparse

def clean_url(url):
    """Clean and standardize URLs for better matching."""
    if pd.isna(url) or not isinstance(url, str):
        return ""
    
    # Remove http/https prefix and trailing slashes
    url = url.lower().strip()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'/$', '', url)
    
    # Get domain only
    try:
        domain = urlparse('http://' + url).netloc
        if domain:
            return domain
    except:
        pass
    
    return url

def merge_csv_to_excel(excel_path, csv_path, output_path=None):
    """
    Merge data from CSV to Excel based on matching criteria.
    First tries to match by URL, then by company name if no URL match is found.
    
    Args:
        excel_path: Path to the Excel file
        csv_path: Path to the CSV file with company data
        output_path: Path to save the enriched Excel file (if None, will modify original)
    
    Returns:
        Path to the saved file
    """
    if output_path is None:
        # Create output filename by adding '_enriched' before file extension
        file_name, file_ext = os.path.splitext(excel_path)
        output_path = f"{file_name}_enriched{file_ext}"
    
    print(f"Loading CSV data from {csv_path}...")
    csv_data = pd.read_csv(csv_path)
    
    # Clean URLs in CSV for better matching
    csv_data['Clean_Company_Url'] = csv_data['Company Url'].apply(clean_url)
    
    print(f"Loading Excel data from {excel_path}...")
    excel_data = pd.read_excel(excel_path)
    
    # Clean URLs in Excel for better matching
    excel_data['Clean_URL'] = excel_data['URL'].apply(clean_url)
    
    # Create new columns in Excel for the CSV data
    csv_columns = [col for col in csv_data.columns if col not in ['Company name', 'Company Url']]
    for col in csv_columns:
        excel_data[col] = None
    
    print("Matching and merging data...")
    matches_by_url = 0
    matches_by_name = 0
    
    # Iterate through Excel rows to find matches
    for i, excel_row in excel_data.iterrows():
        match_found = False
        
        # Try matching by URL first
        if not pd.isna(excel_row['Clean_URL']) and excel_row['Clean_URL'] != "":
            matches = csv_data[csv_data['Clean_Company_Url'] == excel_row['Clean_URL']]
            
            if len(matches) > 0:
                match_found = True
                matches_by_url += 1
                # Copy CSV data to Excel row
                for col in csv_columns:
                    excel_data.at[i, col] = matches.iloc[0][col]
        
        # If no match by URL, try matching by company name
        if not match_found:
            company_name = str(excel_row['Firma1']).strip().lower() if not pd.isna(excel_row['Firma1']) else ""
            
            if company_name:
                # Try exact match first
                exact_matches = csv_data[csv_data['Company name'].str.lower() == company_name]
                
                if len(exact_matches) > 0:
                    match_found = True
                    matches_by_name += 1
                    # Copy CSV data to Excel row
                    for col in csv_columns:
                        excel_data.at[i, col] = exact_matches.iloc[0][col]
                else:
                    # Try partial match if no exact match
                    for j, csv_row in csv_data.iterrows():
                        csv_company = str(csv_row['Company name']).lower()
                        if csv_company in company_name or company_name in csv_company:
                            match_found = True
                            matches_by_name += 1
                            # Copy CSV data to Excel row
                            for col in csv_columns:
                                excel_data.at[i, col] = csv_row[col]
                            break
    
    # Drop the temporary columns used for matching
    excel_data.drop(['Clean_URL'], axis=1, inplace=True)
    
    print(f"Saving enriched Excel to {output_path}...")
    excel_data.to_excel(output_path, index=False)
    
    print(f"Matching summary:")
    print(f"  - Matches by URL: {matches_by_url}")
    print(f"  - Matches by company name: {matches_by_name}")
    print(f"  - Total matches: {matches_by_url + matches_by_name}")
    print(f"  - Total Excel rows: {len(excel_data)}")
    
    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSV data into Excel based on matching criteria")
    parser.add_argument("--excel", required=True, help="Path to the Excel file")
    parser.add_argument("--csv", default="/home/novoai/Documents/scraper/consolidated_output/llm_extracted_data.csv", 
                        help="Path to the CSV file (default: consolidated_output/llm_extracted_data.csv)")
    parser.add_argument("--output", help="Output path for enriched Excel (default: adds '_enriched' to original filename)")
    
    args = parser.parse_args()
    
    result_path = merge_csv_to_excel(args.excel, args.csv, args.output)
    print(f"Process completed. Enriched Excel saved to: {result_path}")
