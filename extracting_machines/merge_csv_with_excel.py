import pandas as pd
import numpy as np
from Levenshtein import distance
from datetime import datetime
import argparse
import os
import re

def standardize_company_name(name):
    # Replace underscores with spaces
    return name.replace('_', ' ')

def normalize_company_name(name):
    if not isinstance(name, str):
        return name
    # Remove extra spaces
    name = ' '.join(name.split())
    # Standardize common variations
    name = name.strip()
    name = name.replace('GmbH & Co. KG.', 'GmbH & Co. KG')
    name = name.replace('GmbH & Co.KG.', 'GmbH & Co. KG')
    name = name.replace('GmbH & Co.KG', 'GmbH & Co. KG')
    return name

def categorize_machine_park_size(value: str) -> str:
    """
    Categorizes a numeric value into a Maschinen Park Size range based on defined rules.
    
    Args:
        value (str): Numeric value as string
        
    Returns:
        str: Maschinen Park Size category (e.g., '10-15') or '' for invalid/empty inputs
    """
    # Handle empty or None inputs
    if value is None or value == '':
        return ''
        
    try:
        # Convert to float first, then to int to handle both integers and floats
        val = int(float(value))
            
    except (ValueError, TypeError):
        # Handle any conversion errors (invalid strings, etc.)
        return ''
    
    # Category ranges based on the provided rules
    categories = [
        (300000, 700000, '10-15'),
        (700001, 900000, '15-20'),
        (900001, 1200000, '21-40'),
        (1200001, 1500000, '40-60'),
        (1500001, 1800000, '60-80'),
        (1800001, 2500000, '80-120'),
        (2500001, 5000000, '120-200'),
        (5000001, 10000000, '200-350'),
        (10000001, 99000000, '350-600')
    ]
    
    for lower, upper, category in categories:
        if lower <= val <= upper:
            #print(f"Value: {val}, Category: {category}")
            return category
            
    # For values above the highest category threshold
    return 'No Match'

def process_machine_data(csv_file ="machine_report_maschinenbau_20250307.csv",top_n=2): 
    """
    Process the machine data from the CSV file for analysis and classification.
    
    This function performs several operations:
    1. Reads and standardizes company names in the CSV data
    2. Restructures data to create a row per machine entry
    3. Filters out low-value machines (≤20000)
    4. Identifies the top N machines by value for each company
    5. Calculates machine park size category based on top machine values
    
    Args:
        csv_file (str): Path to the CSV file containing machine data
        top_n (int): Number of top machines to extract per company
        
    Returns:
        DataFrame: A dataframe containing company names, their top N machine values,
                  and the calculated machine park size category
    """
    # Read the CSV file into a pandas DataFrame
    csv_df = pd.read_csv(csv_file)
    
    # Standardize company names by replacing underscores with spaces
    csv_df['Company'] = csv_df['Company'].apply(standardize_company_name)
    
    # Identify columns containing machine data (Machine_1, Machine_2, Machine_3, etc.)
    machine_cols = [col for col in csv_df.columns if 'Machine_' in col]
    
    # Reshape the data from wide to long format using melt
    # This converts multiple machine columns into rows where each row represents one machine
    melted_df = pd.melt(
        csv_df,
        id_vars=['Company'],  # Keep company as identifier
        value_vars=machine_cols,  # Convert these columns to rows
        var_name='Machine_Type',  # Name for the column holding original column names
        value_name='Machine_Value'  # Name for the column holding values
    )
    
    # Convert Machine_Value to numeric data type
    # 'coerce' parameter converts non-numeric values to NaN
    melted_df['Machine_Value'] = pd.to_numeric(melted_df['Machine_Value'], errors='coerce')
    
    # Filter to keep only significant machine values (>20000)
    # This removes small equipment and potential data errors
    filtered_df = melted_df[melted_df['Machine_Value'] > 20000]
    
    # Sort the data by company and machine value (descending)
    # This prepares data for extracting top N machines per company
    sorted_df = filtered_df.sort_values(['Company', 'Machine_Value'], ascending=[True, False])
    
    # For each company, get only the top N machine values
    # Using groupby().head() keeps only the first N rows per company after sorting
    top_n_df = sorted_df.groupby('Company').head(top_n)
    
    # Create the result dataframe with unique company names
    result_df = pd.DataFrame({
        'Company': top_n_df['Company'].unique()
    })
    
    # Add columns for top 1 to top N machine values
    for i in range(top_n):
        values = []
        for company in result_df['Company']:
            # Get data rows for this specific company
            company_data = top_n_df[top_n_df['Company'] == company]
            
            # Extract the ith machine value if available, otherwise use NaN
            # iloc[i] accesses the ith row for this company after sorting
            value = company_data.iloc[i]['Machine_Value'] if len(company_data) > i else np.nan
            values.append(value)
            
        # Add a new column with the extracted values for this rank (Top1, Top2, etc.)
        result_df[f'Top{i+1}_Machine'] = values
    
    # Calculate Maschinen_Park_Size category based on the value of the top machine
    # This uses the categorize_machine_park_size function to map values to categories
    result_df['Maschinen_Park_Size'] = result_df['Top1_Machine'].astype(str).apply(categorize_machine_park_size)
    
    return result_df

def find_best_match(company_name, company_list, threshold=0.85):
    """Find the best matching company name using Levenshtein distance."""
    if not isinstance(company_name, str):
        return None
    
    best_match = None
    best_ratio = 0
    
    for potential_match in company_list:
        if not isinstance(potential_match, str):
            continue
            
        # Calculate similarity ratio (1 - normalized_distance)
        max_len = max(len(company_name), len(potential_match))
        if max_len == 0:
            continue
        
        dist = distance(company_name.lower(), potential_match.lower())
        ratio = 1 - (dist / max_len)
        
        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_match = potential_match
    
    return best_match, best_ratio

def analyze_company_similarities(csv_companies, xlsx_companies):
    """Analyze similarity scores between all companies in both datasets."""
    similarity_matrix = []
    problematic_matches = []
    
    print("\nAnalyzing company name similarities...")
    for csv_company in csv_companies:
        best_match = None
        best_ratio = 0
        
        for xlsx_company in xlsx_companies:
            if not isinstance(csv_company, str) or not isinstance(xlsx_company, str):
                continue
                
            max_len = max(len(csv_company), len(xlsx_company))
            if max_len == 0:
                continue
            
            dist = distance(csv_company.lower(), xlsx_company.lower())
            ratio = 1 - (dist / max_len)
            similarity_matrix.append({
                'csv_company': csv_company,
                'xlsx_company': xlsx_company,
                'similarity': ratio
            })
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = xlsx_company
        Threshold = 0.85
        if best_ratio < Threshold:  # Threshold for problematic matches
            problematic_matches.append({
                'csv_company': csv_company,
                'best_match': best_match,
                'similarity': best_ratio
            })
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(similarity_matrix)
    
    # Calculate statistics
    stats = {
        'total_comparisons': len(df),
        'mean_similarity': df['similarity'].mean(),
        'median_similarity': df['similarity'].median(),
        'min_similarity': df['similarity'].min(),
        'max_similarity': df['similarity'].max(),
        'std_similarity': df['similarity'].std(),
        'problematic_matches': problematic_matches
    }
    
    # Print detailed analysis
    print("\nCompany Name Similarity Analysis:")
    print(f"Total comparisons made: {stats['total_comparisons']}")
    print(f"Mean similarity: {stats['mean_similarity']:.3f}")
    print(f"Median similarity: {stats['median_similarity']:.3f}")
    print(f"Minimum similarity: {stats['min_similarity']:.3f}")
    print(f"Maximum similarity: {stats['max_similarity']:.3f}")
    print(f"Standard deviation: {stats['std_similarity']:.3f}")
    
    if problematic_matches:
        print(f"\nPotentially Problematic Matches (similarity < {Threshold}):")
        for match in sorted(problematic_matches, key=lambda x: x['similarity']):
            print(f"CSV: {match['csv_company']}")
            print(f"Best Match: {match['best_match']}")
            print(f"Similarity: {match['similarity']:.3f}")
            print("-" * 50)
    
    return stats

def load_data(csv_file_path, xlsx_file_path='input_excel.xlsx', sheet_name='Sheet1'):
    """Load and normalize data from CSV and Excel files."""
    try:
        machine_data = process_machine_data(csv_file=csv_file_path)
        xlsx_df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name)

        # Normalize company names
        xlsx_df['Firma1'] = xlsx_df['Firma1'].apply(normalize_company_name)
        machine_data['Company'] = machine_data['Company'].apply(normalize_company_name)
        
        return machine_data, xlsx_df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def create_company_mapping(machine_data, xlsx_df):
    """Create mapping between CSV companies and Excel companies using fuzzy matching."""
    company_mapping = {}
    similarity_scores = []
    xlsx_companies = xlsx_df['Firma1'].dropna().tolist()
    
    # Track matching statistics
    total_companies = len(machine_data['Company'].unique())
    matched_companies = 0
    # Keep track of 5 lowest pairs using a list of tuples (similarity, csv_company, xlsx_company)
    lowest_pairs = [(1.0, '', '')] * 5

    for csv_company in machine_data['Company'].unique():
        best_match, ratio = find_best_match(csv_company, xlsx_companies, 0.83)
        if best_match:
            company_mapping[csv_company] = best_match
            similarity_scores.append(ratio)
            matched_companies += 1
            
            # Update lowest pairs list
            lowest_pairs.append((ratio, csv_company, best_match))
            # Sort by similarity and keep only 5 lowest
            lowest_pairs.sort(key=lambda x: x[0])
            lowest_pairs = lowest_pairs[:5]

    # Print matching statistics
    if similarity_scores:
        avg_similarity = sum(similarity_scores) / len(similarity_scores)
        print("\nMatching Statistics:")
        print(f"Total companies processed: {total_companies}")
        print(f"Successfully matched: {matched_companies}")
        print(f"Average similarity score: {avg_similarity:.2f}")
        print("\n5 Lowest Similarity Pairs:")
        for similarity, csv_company, xlsx_company in lowest_pairs:
            print(f"Score: {similarity:.3f} | {csv_company} -> {xlsx_company}")

    return company_mapping

def merge_datasets(xlsx_df, machine_data, company_mapping, top_n):
    """Merge Excel and CSV data using the company mapping."""
    # Create a new column with mapped company names
    machine_data['Mapped_Company'] = machine_data['Company'].map(company_mapping)

    # Merge the dataframes using the mapped companies
    merged_df = pd.merge(
        xlsx_df,
        machine_data,
        left_on='Firma1',
        right_on='Mapped_Company',
        how='left'
    )

    # Only keep specific columns from excel (Firma1, URL, Ort) and CSV (machine values, Park Size)
    machine_cols = [f'Top{i+1}_Machine' for i in range(top_n)]
    columns_to_keep = ['Firma1', 'URL', 'Ort'] + machine_cols + ['Maschinen_Park_Size']
    
    # Filter columns
    merged_df = merged_df[columns_to_keep]
    
    # Filter rows - only keep those with at least one machine value
    has_machine_value = False
    for col in machine_cols:
        has_machine_value = has_machine_value | merged_df[col].notna()
    
    # Apply the filter
    filtered_df = merged_df[has_machine_value]
    
    return filtered_df

def save_merged_data(merged_df, csv_file_path='machine_report.csv'):
    """Save the merged dataframe to a CSV file with date in filename."""
    current_date = datetime.now().strftime('%Y%m%d')
    
    # Extract company name from the input CSV filename
    base_filename = os.path.basename(csv_file_path)
    #print(f"Processing file: {base_filename}")
    company_name = ""
    
    # Extract company name from machine_report_COMPANY_DATETIME.csv pattern
    if "machine_report_" in base_filename:
        # Remove "machine_report_" prefix
        name_without_prefix = base_filename.replace("machine_report_", "")
        
        # Extract just the company name part (between machine_report_ and first _)
        parts = name_without_prefix.split('_')
        if len(parts) > 0:
            company_name = parts[0]
            
        #print(f"Extracted company name: {company_name}")
    
    if not company_name:
        # Fallback to generic name if extraction failed
        output_file_path = f"merged_data_{current_date}.csv"
    else:
        output_file_path = f"merged_{company_name}_{current_date}.csv"
    
    merged_df.to_csv(output_file_path, index=False)
    #print(f"Merged data saved to {output_file_path}")
    return output_file_path

def main(csv_file_path, top_n=2):
    """
    Main function to merge machine data CSV with Excel file.
    
    Args:
        csv_file_path (str): Path to the CSV file containing machine data
        top_n (int): Number of top machines to include (default: 2)
    """
    try:
        # Step 1: Load and prepare data
        machine_data, xlsx_df = load_data(csv_file_path)
        
        # Step 2: Analyze similarities for debugging
        analyze_company_similarities(
            machine_data['Company'].unique(),
            xlsx_df['Firma1'].dropna().unique()
        )
        
        # Step 3: Create mapping between company names
        company_mapping = create_company_mapping(machine_data, xlsx_df)
        
        # Step 4: Merge datasets using the mapping
        merged_df = merge_datasets(xlsx_df, machine_data, company_mapping, top_n)
        
        # Step 5: Save the result
        output_file = save_merged_data(merged_df, csv_file_path)
        print(f"Successfully merged and saved the data to {output_file}!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Merge machine data CSV with Excel file.')
    parser.add_argument('csv_file', type=str, help='Path to the CSV file containing machine data')
    parser.add_argument('--top_n', type=int, default=1, help='Number of top machines to include (default: 1)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Call merge function with command line arguments
    main(csv_file_path=args.csv_file, top_n=args.top_n)
