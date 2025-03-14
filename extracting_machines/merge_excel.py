import pandas as pd
import numpy as np
from Levenshtein import distance
from datetime import datetime

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

def process_machine_data(csv_file ="machine_report_maschinenbau_20250307.csv",top_n=2): 
    """
    Process the machine data from the CSV file, standardize company names,
    then only keep rows with machine values > 20000,
    and extract top N machine values for each company.
    
    """
    # Read the CSV file
    csv_df = pd.read_csv(csv_file)
    
    # Standardize company names
    csv_df['Company'] = csv_df['Company'].apply(standardize_company_name)
    
    # Identify machine columns (Machine_1, Machine_2, Machine_3)
    machine_cols = [col for col in csv_df.columns if 'Machine_' in col]
    
    # Create a mapping of company to park size before melting
    park_size_mapping = dict(zip(csv_df['Company'], csv_df['Maschinen Park Size']))
    
    # Melt the dataframe to convert machine columns into rows
    melted_df = pd.melt(
        csv_df,
        id_vars=['Company'],
        value_vars=machine_cols,
        var_name='Machine_Type',
        value_name='Machine_Value'
    )
    
    # Convert Machine_Value to numeric, handling any non-numeric values
    melted_df['Machine_Value'] = pd.to_numeric(melted_df['Machine_Value'], errors='coerce')
    
    # Filter for machine values > 20000
    filtered_df = melted_df[melted_df['Machine_Value'] > 20000]
    
    # Sort values by company and machine value
    sorted_df = filtered_df.sort_values(['Company', 'Machine_Value'], ascending=[True, False])
    
    # Get top N machine values for each company
    top_n_df = sorted_df.groupby('Company').head(top_n)
    
    # Pivot the results to create columns for Top1 to TopN machines
    result_df = pd.DataFrame({
        'Company': top_n_df['Company'].unique()
    })
    
    # Add columns for top 1 to top N machine values
    for i in range(top_n):
        values = []
        for company in result_df['Company']:
            company_data = top_n_df[top_n_df['Company'] == company]
            value = company_data.iloc[i]['Machine_Value'] if len(company_data) > i else np.nan
            values.append(value)
        result_df[f'Top{i+1}_Machine'] = values
    
    # Add the Maschinen Park Size column
    result_df['Maschinen_Park_Size'] = result_df['Company'].map(park_size_mapping)
    
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

def merge_with_xlsx(top_n=2):
    try:
        csv_file_path = 'machine_report_maschinenbauer_20250312.csv'
        machine_data = process_machine_data(csv_file=csv_file_path,top_n=top_n)
        xlsx_file_path = 'input_excel.xlsx'
        sheet_name = 'Sheet1'  # Change this to the actual sheet name if needed
        xlsx_df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name)

        # Normalize company names
        xlsx_df['Firma1'] = xlsx_df['Firma1'].apply(normalize_company_name)
        machine_data['Company'] = machine_data['Company'].apply(normalize_company_name)

        # Analyze similarities before matching
        analyze_company_similarities(
            machine_data['Company'].unique(),
            xlsx_df['Firma1'].dropna().unique()
        )
        
        # Create a mapping dictionary using fuzzy matching
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

        # Keep all original columns from xlsx_df plus the machine columns and Maschinen Park Size from the merge
        machine_cols = [f'Top{i+1}_Machine' for i in range(top_n)]
        columns_to_keep = list(xlsx_df.columns) + machine_cols + ['Maschinen_Park_Size']
        merged_df = merged_df[columns_to_keep]
        
        # Save the merged dataframe to a new XLSX file
        current_date = datetime.now().strftime('%Y%m%d')
        output_file_path = xlsx_file_path.replace('.xlsx', f'_merged_{current_date}.xlsx')
        merged_df.to_excel(output_file_path, index=False)
        print(f"Merged data saved to {output_file_path}")
        print("Successfully merged and saved the data!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    # You can change this number to get different number of top machines
    merge_with_xlsx(top_n=1)
