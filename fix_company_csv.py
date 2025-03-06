#!/usr/bin/env python3
import pandas as pd
import re
import os
import sys

def clean_company_name(name):
    """
    Clean company names by fixing quotation marks issues.
    
    Args:
        name (str): Company name that might contain quotation marks
    
    Returns:
        str: Cleaned company name
    """
    if isinstance(name, str):
        # Direct replacement for the problematic PERO entry
        if '"""PERO""' in name:
            print(f"Found PERO issue: '{name}'")
            name = name.replace('"""PERO"" - Aktiengesellschaft P. Erbel"', '"PERO" - Aktiengesellschaft P. Erbel')
            print(f"After direct replacement: '{name}'")
        
        # Try regex replacement if direct replacement didn't work
        if '"""PERO""' in name:
            original = name
            name = re.sub(r'"""PERO"" - Aktiengesellschaft P. Erbel"', '"PERO" - Aktiengesellschaft P. Erbel', name)
            print(f"After regex: '{name}'")
            
            # As a last resort, if still not fixed, use a hardcoded replacement
            if '"""PERO""' in name:
                name = '"PERO" - Aktiengesellschaft P. Erbel'
                print(f"After hardcoded replacement: '{name}'")
        
        # Handle other potential quotation mark issues
        name = re.sub(r'"{2,}', '"', name)  # Replace multiple quotes with single quote
        
        # Remove enclosing quotes if present
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
    
    return name

def fix_csv_file(csv_file):
    """
    Fix company names in an existing CSV file.
    
    Args:
        csv_file (str): Path to the CSV file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"Error: File '{csv_file}' not found")
            return False
        
        # Read the CSV file directly without making a backup first
        df = pd.read_csv(csv_file)
        
        # Check if 'company name' column exists
        if 'company name' not in df.columns:
            print("Error: 'company name' column not found in CSV")
            return False
        
        # Print problematic entries before fixing
        problematic = df[df['company name'].str.contains('PERO', na=False)]
        if not problematic.empty:
            print("Problematic entries found:")
            print(problematic)
            
            # Get the index of the problematic row
            pero_idx = problematic.index[0]
            print(f"PERO entry is at index {pero_idx}")
            
            # Direct fix for the specific known problem
            df.at[pero_idx, 'company name'] = '"PERO" - Aktiengesellschaft P. Erbel'
            
            print(f"Directly fixed to: {df.at[pero_idx, 'company name']}")
        else:
            # Apply the cleaning function to all rows if no specific PERO issue found
            df['company name'] = df['company name'].apply(clean_company_name)
        
        # Create backup of original file
        backup_file = csv_file + '.bak'
        if not os.path.exists(backup_file):
            print(f"Creating backup as '{backup_file}'")
            os.rename(csv_file, backup_file)
        
        # Save fixed data to CSV
        df.to_csv(csv_file, index=False)
        
        # Verify fix worked
        fixed_df = pd.read_csv(csv_file)
        problematic_after = fixed_df[fixed_df['company name'].str.contains('"""PERO""', na=False)]
        if not problematic_after.empty:
            print("ERROR: Still found problematic entries after fixing:")
            print(problematic_after)
            return False
        else:
            print("PERO issue has been successfully fixed!")
            
            # Show the corrected row
            correct_row = fixed_df[fixed_df['company name'].str.contains('PERO', na=False)]
            if not correct_row.empty:
                print("Corrected entry:")
                print(correct_row)
        
        print(f"Successfully fixed '{csv_file}'")
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        # Try to restore original file if exception occurred and backup exists
        if os.path.exists(backup_file) and not os.path.exists(csv_file):
            os.rename(backup_file, csv_file)
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fix_company_csv.py <csv_file>")
        print("Example: fix_company_csv.py company_Maschinenbauer_BA.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    success = fix_csv_file(csv_file)
    sys.exit(0 if success else 1)
