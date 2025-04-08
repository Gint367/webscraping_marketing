import pandas as pd
import numpy as np
from Levenshtein import distance
from datetime import datetime
import argparse
import os
from fuzzywuzzy import fuzz


def standardize_company_name(name):
    # Replace underscores with spaces
    return name.replace("_", " ")


def normalize_company_name(name):
    if not isinstance(name, str):
        return name
    # Remove extra spaces
    name = " ".join(name.split())
    # Standardize common variations
    name = name.strip()
    name = name.replace("GmbH & Co. KG.", "GmbH & Co. KG")
    name = name.replace("GmbH & Co.KG.", "GmbH & Co. KG")
    name = name.replace("GmbH & Co.KG", "GmbH & Co. KG")
    return name

def standardize_for_comparison(name):
    """Standardize company name for better comparison."""
    if not isinstance(name, str):
        return ""
        
    # Convert to lowercase
    name = name.lower()
    
    # Standardize & and and
    name = name.replace('&', 'and')
    

        
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
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
    if value is None or value == "":
        return ""

    try:
        # Convert to float first, then to int to handle both integers and floats
        val = int(float(value))

    except (ValueError, TypeError):
        # Handle any conversion errors (invalid strings, etc.)
        return ""

    # Category ranges based on the provided rules
    categories = [
        (300000, 700000, "10-15"),
        (700001, 900000, "15-20"),
        (900001, 1200000, "21-40"),
        (1200001, 1500000, "40-60"),
        (1500001, 1800000, "60-80"),
        (1800001, 2500000, "80-120"),
        (2500001, 5000000, "120-200"),
        (5000001, 10000000, "200-350"),
        (10000001, 99000000, "350-600"),
    ]

    for lower, upper, category in categories:
        if lower <= val <= upper:
            # print(f"Value: {val}, Category: {category}")
            return category

    # For values above the highest category threshold
    return "No Match"


def process_machine_data(csv_file="machine_report_maschinenbau_20250307.csv", top_n=2):
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
    csv_df["Company"] = csv_df["Company"].apply(standardize_company_name)

    # Identify columns containing machine data (Machine_1, Machine_2, Machine_3, etc.)
    machine_cols = [col for col in csv_df.columns if "Machine_" in col]

    # Reshape the data from wide to long format using melt
    # This converts multiple machine columns into rows where each row represents one machine
    melted_df = pd.melt(
        csv_df,
        id_vars=["Company"],  # Keep company as identifier
        value_vars=machine_cols,  # Convert these columns to rows
        var_name="Machine_Type",  # Name for the column holding original column names
        value_name="Machine_Value",  # Name for the column holding values
    )

    # Convert Machine_Value to numeric data type
    # 'coerce' parameter converts non-numeric values to NaN
    melted_df["Machine_Value"] = pd.to_numeric(
        melted_df["Machine_Value"], errors="coerce"
    )

    # Filter to keep only significant machine values (>20000)
    # This removes small equipment and potential data errors
    filtered_df = melted_df[melted_df["Machine_Value"] > 20000]

    # Sort the data by company and machine value (descending)
    # This prepares data for extracting top N machines per company
    sorted_df = filtered_df.sort_values(
        ["Company", "Machine_Value"], ascending=[True, False]
    )

    # For each company, get only the top N machine values
    # Using groupby().head() keeps only the first N rows per company after sorting
    top_n_df = sorted_df.groupby("Company").head(top_n)

    # Create the result dataframe with unique company names
    result_df = pd.DataFrame({"Company": top_n_df["Company"].unique()})

    # Add columns for top 1 to top N machine values
    for i in range(top_n):
        values = []
        for company in result_df["Company"]:
            # Get data rows for this specific company
            company_data = top_n_df[top_n_df["Company"] == company]

            # Extract the ith machine value if available, otherwise use NaN
            # iloc[i] accesses the ith row for this company after sorting
            value = (
                company_data.iloc[i]["Machine_Value"]
                if len(company_data) > i
                else np.nan
            )
            values.append(value)

        # Add a new column with the extracted values for this rank (Top1, Top2, etc.)
        result_df[f"Top{i + 1}_Machine"] = values

    # Calculate Maschinen_Park_Size category based on the value of the top machine
    # This uses the categorize_machine_park_size function to map values to categories
    result_df["Maschinen_Park_Size"] = (
        result_df["Top1_Machine"].astype(str).apply(categorize_machine_park_size)
    )

    return result_df


def find_best_match(company_name, company_list, threshold=0.85):
    """Find the best matching company name fuzzy matching algorithm."""
    if not isinstance(company_name, str):
        return None

    best_match = None
    best_ratio = 0
    
    std_company = standardize_for_comparison(company_name)
    
    for potential_match in company_list:
        if not isinstance(potential_match, str):
            continue
            
        # Standardize the potential match too
        std_potential = standardize_for_comparison(potential_match)
        
        # Use token_set_ratio for better handling of company name variations
        ratio = fuzz.token_set_ratio(std_company, std_potential)
        
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = potential_match
    
    # Only return the match if it meets the threshold
    if best_ratio >= threshold * 100:  # Convert threshold to same scale as fuzz scores
        return best_match, best_ratio/100
    else:
        return None, best_ratio/100


def analyze_company_similarities(machine_data, xlsx_df):
    """Analyze similarity scores between all companies in both datasets."""
    similarity_matrix = []
    problematic_matches = []

    # Get only the company name columns
    csv_companies = machine_data["Company"].unique()
    xlsx_companies = xlsx_df["Firma1"].dropna().unique()

    print("\nAnalyzing company name similarities...")
    for csv_company in csv_companies:
        best_match = None
        best_ratio = 0

        for xlsx_company in xlsx_companies:
            if not isinstance(csv_company, str) or not isinstance(xlsx_company, str):
                continue

            # Use the same standardization and matching algorithm as find_best_match
            std_company = standardize_for_comparison(csv_company)
            std_potential = standardize_for_comparison(xlsx_company)
            
            # Use token_set_ratio for better handling of company name variations
            ratio = fuzz.token_set_ratio(std_company, std_potential) / 100  # Convert to 0-1 scale
            
            similarity_matrix.append(
                {
                    "csv_company": csv_company,
                    "xlsx_company": xlsx_company,
                    "similarity": ratio,
                }
            )

            if ratio > best_ratio:
                best_ratio = ratio
                best_match = xlsx_company
        
        Threshold = 0.85
        if best_ratio < Threshold:  # Threshold for problematic matches
            problematic_matches.append(
                {
                    "csv_company": csv_company,
                    "best_match": best_match,
                    "similarity": best_ratio,
                }
            )

    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(similarity_matrix)

    # Calculate statistics
    stats = {
        "total_comparisons": len(df),
        "mean_similarity": df["similarity"].mean(),
        "median_similarity": df["similarity"].median(),
        "min_similarity": df["similarity"].min(),
        "max_similarity": df["similarity"].max(),
        "std_similarity": df["similarity"].std(),
        "problematic_matches": problematic_matches,
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
        for match in sorted(problematic_matches, key=lambda x: x["similarity"]):
            print(f"CSV: {match['csv_company']}")
            print(f"Best Match: {match['best_match']}")
            print(f"Similarity: {match['similarity']:.3f}")
            print("-" * 50)

    return stats


def load_data(csv_file_path, xlsx_file_path="input_excel.xlsx", sheet_name="Sheet1"):
    """Load and normalize data from CSV and Excel files."""
    try:
        machine_data = process_machine_data(csv_file=csv_file_path)
        xlsx_df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name)

        # Normalize company names
        xlsx_df["Firma1"] = xlsx_df["Firma1"].apply(normalize_company_name)
        machine_data["Company"] = machine_data["Company"].apply(normalize_company_name)

        return machine_data, xlsx_df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise


def create_company_mapping(machine_data, xlsx_df):
    """Create mapping between CSV companies and Excel companies using fuzzy matching."""
    company_mapping = {}
    similarity_scores = []
    xlsx_companies = xlsx_df["Firma1"].dropna().tolist()

    # Track matching statistics
    total_companies = len(machine_data["Company"].unique())
    matched_companies = 0
    # Keep track of 5 lowest pairs using a list of tuples (similarity, csv_company, xlsx_company)
    lowest_pairs = [(1.0, "", "")] * 5
    threshold = 0.85
    for csv_company in machine_data["Company"].unique():
        best_match, ratio = find_best_match(csv_company, xlsx_companies, threshold)
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
        print("\nMatching Statistics for Merging technische anlagen with Excel:")
        print(f"Total companies processed: {total_companies}")
        print(f"Successfully matched: {matched_companies}")
        print(f"Average similarity score: {avg_similarity:.2f}")
        print(f"\n5 Lowest Similarity Pairs | {threshold}")
        for similarity, csv_company, xlsx_company in lowest_pairs:
            print(f"Score: {similarity:.3f} | {csv_company} -> {xlsx_company}")

    return company_mapping


def merge_datasets(xlsx_df, machine_data, company_mapping, top_n):
    """Merge Excel and CSV data using the company mapping."""
    # Create a new column with mapped company names
    machine_data["Mapped_Company"] = machine_data["Company"].map(company_mapping)

    # Merge the dataframes using the mapped companies
    merged_df = pd.merge(
        xlsx_df, machine_data, left_on="Firma1", right_on="Mapped_Company", how="left"
    )

    # Only keep specific columns from excel (Firma1, URL, Ort) and CSV (machine values, Park Size)
    machine_cols = [f"Top{i + 1}_Machine" for i in range(top_n)]
    columns_to_keep = ["Firma1", "URL", "Ort"] + machine_cols + ["Maschinen_Park_Size"]

    # Filter columns
    merged_df = merged_df[columns_to_keep]

    # Note: We're not filtering rows here anymore
    return merged_df


def save_merged_data(merged_df, csv_file_path="machine_report.csv"):
    """Save the merged dataframe to a CSV file with date in filename."""
    current_date = datetime.now().strftime("%Y%m%d")

    # Extract company name from the input CSV filename
    base_filename = os.path.basename(csv_file_path)
    # print(f"Processing file: {base_filename}")
    company_name = ""

    # Extract company name from machine_report_COMPANY_DATETIME.csv pattern
    if "machine_report_" in base_filename:
        # Remove "machine_report_" prefix
        name_without_prefix = base_filename.replace("machine_report_", "")

        # Extract just the company name part (between machine_report_ and first _)
        parts = name_without_prefix.split("_")
        if len(parts) > 0:
            company_name = parts[0]

        # print(f"Extracted company name: {company_name}")

    if not company_name:
        print(
            f"Warning: Could not extract company name from filename '{base_filename}'."
        )
        # Fallback to generic name if extraction failed
        output_file_path = f"merged_data_{current_date}.csv"
    else:
        output_file_path = f"merged_{company_name}_{current_date}.csv"

    merged_df.to_csv(output_file_path, index=False)
    # print(f"Merged data saved to {output_file_path}")
    return output_file_path


def load_sachanlagen_data(sachanlagen_path):
    """Load Sachanlagen data from CSV file."""
    try:
        sachanlagen_df = pd.read_csv(sachanlagen_path)
        # Ensure column names are correct
        if 'company_name' not in sachanlagen_df.columns or 'sachanlagen' not in sachanlagen_df.columns:
            print(f"Warning: Required columns not found in {sachanlagen_path}")
            return pd.DataFrame()
        
        # Normalize company names
        sachanlagen_df["company_name"] = sachanlagen_df["company_name"].apply(normalize_company_name)
        return sachanlagen_df
    except Exception as e:
        print(f"Error loading Sachanlagen data: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error


def create_sachanlagen_mapping(sachanlagen_df, xlsx_df):
    """Create mapping between Sachanlagen companies and Excel companies using fuzzy matching."""
    sachanlagen_mapping = {}
    xlsx_companies = xlsx_df["Firma1"].dropna().tolist()
    
    # Track matching statistics
    total_companies = len(sachanlagen_df["company_name"].unique())
    matched_companies = 0
    
    for sachanlagen_company in sachanlagen_df["company_name"].unique():
        best_match, ratio = find_best_match(sachanlagen_company, xlsx_companies, 0.85)
        if best_match:
            sachanlagen_mapping[sachanlagen_company] = best_match
            matched_companies += 1
    
    # Print matching statistics
    print(f"\nMatching Statistics for Sachanlagen anlagen with Excel:")
    print(f"Total Sachanlagen companies: {total_companies}")
    print(f"Successfully matched: {matched_companies}")
    
    return sachanlagen_mapping


def merge_with_sachanlagen(merged_df, sachanlagen_df, mapping):
    """
    Add Sachanlagen data to the merged dataframe.

    Args:
        merged_df (pd.DataFrame): The merged dataframe with company information
        sachanlagen_df (pd.DataFrame): DataFrame containing Sachanlagen data
        mapping (dict): Mapping from Sachanlagen company names to Excel company names

    Returns:
        pd.DataFrame: Merged dataframe with Sachanlagen values added
    """
    if sachanlagen_df.empty:
        merged_df['Sachanlagen'] = None
        return merged_df

    # Create a copy to avoid modifying the original
    result_df = merged_df.copy()
    result_df['Sachanlagen'] = None
    
    # Keep track of companies we've already processed
    processed_companies = set()

    # For each company in the mapping, add its Sachanlagen value
    for sachanlagen_company, excel_company in mapping.items():
        # Check if this Excel company is already in our result dataframe
        if excel_company in result_df['Firma1'].values:
            sachanlagen_value = sachanlagen_df.loc[sachanlagen_df['company_name'] == sachanlagen_company, 'sachanlagen']
            if not sachanlagen_value.empty:
                # Convert to string to match the expected format in tests
                result_df.loc[result_df['Firma1'] == excel_company, 'Sachanlagen'] = sachanlagen_value.values[0]
                processed_companies.add(excel_company)
        else:
            # This company isn't in our result dataframe yet - need to add a new row
            sachanlagen_value = sachanlagen_df.loc[sachanlagen_df['company_name'] == sachanlagen_company, 'sachanlagen']
            if not sachanlagen_value.empty:
                # Create a new row with this company's info
                new_row = pd.DataFrame({
                    'Firma1': [excel_company],
                    'URL': [None],
                    'Ort': [None],
                    'Sachanlagen': [sachanlagen_value.values[0]]
                })
                
                # Add empty values for other columns
                for col in result_df.columns:
                    if col not in new_row.columns:
                        new_row[col] = None
                        
                # Add the new row to our result dataframe
                result_df = pd.concat([result_df, new_row], ignore_index=True)
                processed_companies.add(excel_company)
    
    return result_df


def main(csv_file_path, top_n=1, sachanlagen_path=None):
    """
    Main function to orchestrate the merging of CSV machine data with Excel company data.

    Args:
        csv_file_path (str): Path to the CSV file containing machine data
        top_n (int, optional): Number of top machines to extract. Defaults to 1.
        sachanlagen_path (str, optional): Path to the Sachanlagen CSV file. Defaults to None.

    Returns:
        str: Path to the output CSV file
    """
    # Load the CSV and Excel data
    machine_data, xlsx_df = load_data(csv_file_path)
    
    # Step 1: Analyze companies to see how good the similarities are
    similarities = analyze_company_similarities(machine_data, xlsx_df)
    good_matches = len(machine_data["Company"].unique()) - len(similarities["problematic_matches"])
    print(f"Found {good_matches} good matches out of {len(machine_data['Company'].unique())} machine companies")

    # Step 2: Create a mapping from machine company names to Excel company names
    mapping = create_company_mapping(machine_data, xlsx_df)
    
    # Step 3: Merge the datasets using the mapping (without filtering)
    merged_df = merge_datasets(xlsx_df, machine_data, mapping, top_n)
    
    # Add Sachanlagen data if available
    if sachanlagen_path and os.path.exists(sachanlagen_path):
        sachanlagen_df = load_sachanlagen_data(sachanlagen_path)
        # Create mapping for Sachanlagen companies
        sachanlagen_mapping = create_sachanlagen_mapping(sachanlagen_df, xlsx_df)
        # Merge with Sachanlagen data
        merged_df = merge_with_sachanlagen(merged_df, sachanlagen_df, sachanlagen_mapping)
    
    # Filter rows - keep those with at least one machine value OR a sachanlagen value
    machine_cols = [f"Top{i + 1}_Machine" for i in range(top_n)]
    has_machine_value = False
    for col in machine_cols:
        has_machine_value = has_machine_value | merged_df[col].notna()
    
    has_sachanlagen_value = merged_df['Sachanlagen'].notna() if 'Sachanlagen' in merged_df.columns else False
    
    # Apply the filter - keep rows with either machine values or sachanlagen values
    filtered_df = merged_df[has_machine_value | has_sachanlagen_value]
    
    # Step 4: Save the merged data to a CSV file
    output_file = save_merged_data(filtered_df, csv_file_path)
    
    print(f"Merged data saved to {output_file}")
    return output_file


if __name__ == "__main__":
    # Set up command line argument parser
    parser = argparse.ArgumentParser(
        description="Merge machine data CSV with Excel file."
    )
    parser.add_argument(
        "csv_file", type=str, help="Path to the CSV file containing machine data"
    )
    parser.add_argument(
        "--top_n",
        type=int,
        default=1,
        help="Number of top machines to include (default: 1)",
    )
    parser.add_argument(
        "--sachanlagen",
        type=str,
        help="Path to Sachanlagen CSV file",
        default=None,
    )

    # Parse arguments
    args = parser.parse_args()

    # Call merge function with command line arguments
    main(csv_file_path=args.csv_file, top_n=args.top_n, sachanlagen_path=args.sachanlagen)
