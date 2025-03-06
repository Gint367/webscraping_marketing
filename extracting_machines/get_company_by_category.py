#!/usr/bin/env python3
import pandas as pd
import argparse
import os
import re


def clean_company_name(name):
    """
    Clean company names by fixing quotation marks issues.
    
    Args:
        name (str): Company name that might contain quotation marks
    
    Returns:
        str: Cleaned company name
    """
    if isinstance(name, str):
        # Fix pattern like: """PERO"" - Aktiengesellschaft P. Erbel"
        # Convert to: "PERO" - Aktiengesellschaft P. Erbel
        name = re.sub(r'^"{3}([^"]+)"" - (.+)"$', r'"\1" - \2', name)
        
        # Handle other potential quotation mark issues
        name = re.sub(r'"{2,}', '"', name)  # Replace multiple quotes with single quote
        
        # Remove enclosing quotes if present
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
    
    return name


def extract_companies_by_category(input_file, category, output_file=None):
    """
    Extract companies from an Excel file based on a specific category.
    
    Args:
        input_file (str): Path to the Excel file
        category (str): Category to filter by
        output_file (str, optional): Name of the output CSV file. If not provided, 
                                    defaults to 'company_{category}_BA.csv'
    
    Returns:
        str: Path to the output file
    """
    try:
        # Read the first sheet of the Excel file
        df = pd.read_excel(input_file, sheet_name=0)
        
        # Filter rows where 'Kategorie' matches the specified category
        filtered_df = df[df['Kategorie'] == category]
        
        # Select only 'Firma1' and 'Ort' columns
        result_df = filtered_df[['Firma1', 'Ort']].copy()
        
        # Rename columns to 'company name' and 'location'
        result_df.columns = ['company name', 'location']
        
        # Clean company names
        result_df['company name'] = result_df['company name'].apply(clean_company_name)
        
        # Determine output file name
        if output_file is None:
            output_file = f'company_{category}_BA.csv'
        
        # Save to CSV
        result_df.to_csv(output_file, index=False)
        
        print(f"Extracted {len(result_df)} companies in category '{category}'")
        print(f"Results saved to {output_file}")
        
        return output_file
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract companies by category from Excel file')
    parser.add_argument('input_file', help='Path to the input Excel file')
    parser.add_argument('category', help='Category to filter by')
    parser.add_argument('--output', '-o', help='Output CSV file name (default: company_<category>_BA.csv)')
    
    args = parser.parse_args()
    
    extract_companies_by_category(args.input_file, args.category, args.output)
