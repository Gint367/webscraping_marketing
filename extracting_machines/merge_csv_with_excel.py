import argparse
import logging
import os
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

# Set up module-specific logger
logger = logging.getLogger("extracting_machines.merge_csv_with_excel")


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
    name = name.replace("&", "and")

    # Replace underscores with spaces
    name = name.replace("_", " ")

    # Remove extra whitespace
    name = " ".join(name.split())

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
            # Return the category if the value falls within the range
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
    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/{len(csv_df)}:Starting machine data processing from {csv_file}"
    )  # Progress Start

    # Check for required column
    if "Company" not in csv_df.columns:
        raise ValueError("Input CSV must contain a 'Company' column")
    if csv_df.empty:
        logger.info(
            f"PROGRESS:extracting_machine:merge_data:0/0:Input CSV {csv_file} is empty, returning empty DataFrame"
        )  # Progress End (Empty)
        return pd.DataFrame(
            columns=["Company"]
            + [f"Top{i + 1}_Machine" for i in range(top_n)]
            + ["Maschinen_Park_Size"]
        )

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

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:{len(result_df)}/{len(csv_df)}:Finished processing machine data, found {len(result_df)} companies with top machines"
    )  # Progress End
    return result_df


def find_best_match(company_name, company_list, threshold=0.85):
    """Find the best matching company name fuzzy matching algorithm."""
    if not isinstance(company_name, str):
        return None, 0

    # Standardize the input company name
    std_company = standardize_for_comparison(company_name)

    # Early optimization: Pre-standardize all potential matches
    std_potential_matches = {
        company: standardize_for_comparison(company)
        for company in company_list
        if isinstance(company, str)
    }

    # First check for exact matches (much faster than fuzzy matching)
    for potential_match, std_potential in std_potential_matches.items():
        if std_company == std_potential and std_company:  # Avoid matching empty strings
            return potential_match, 1.0

    # If no exact match found, do fuzzy matching
    best_match = None
    best_ratio = 0

    for potential_match, std_potential in std_potential_matches.items():
        # Use token_set_ratio for better handling of company name variations
        ratio = fuzz.token_set_ratio(std_company, std_potential)

        if ratio > best_ratio:
            best_ratio = ratio
            best_match = potential_match

            # If we found a perfect match, no need to check other companies
            if ratio == 100:  # fuzz returns 0-100 scale
                break

    # Only return the match if it meets the threshold
    if best_ratio >= threshold * 100:  # Convert threshold to same scale as fuzz scores
        return best_match, best_ratio / 100
    else:
        return None, best_ratio / 100


def analyze_company_similarities(machine_data, company_df):
    """Analyze similarity scores between all companies in both datasets."""
    similarity_matrix = []
    problematic_matches = []

    # Get only the company name columns
    csv_companies = machine_data["Company"].unique()
    company_list = company_df["Firma1"].dropna().unique()

    # Pre-standardize all company names once to avoid repeated processing
    std_csv_companies = {
        company: standardize_for_comparison(company)
        for company in csv_companies
        if isinstance(company, str)
    }
    std_company_list = {
        company: standardize_for_comparison(company)
        for company in company_list
        if isinstance(company, str)
    }

    logger.info("Analyzing company name similarities...")
    MatchingThreshold = 0.85
    for csv_company, std_company in std_csv_companies.items():
        best_match = None
        best_ratio = 0

        # First check for exact matches - much faster than fuzzy matching
        exact_match_found = False
        for company_name, std_potential in std_company_list.items():
            if (
                std_company == std_potential and std_company
            ):  # Avoid matching empty strings
                best_match = company_name
                best_ratio = 1.0
                exact_match_found = True

                similarity_matrix.append(
                    {
                        "csv_company": csv_company,
                        "company_name": company_name,
                        "similarity": 1.0,
                    }
                )
                break

        # Only perform fuzzy matching if no exact match was found
        if not exact_match_found:
            for company_name, std_potential in std_company_list.items():
                # Use token_set_ratio for better handling of company name variations
                ratio = (
                    fuzz.token_set_ratio(std_company, std_potential) / 100
                )  # Convert to 0-1 scale

                similarity_matrix.append(
                    {
                        "csv_company": csv_company,
                        "company_name": company_name,
                        "similarity": ratio,
                    }
                )

                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = company_name

                    # If we found a perfect match, no need to check other companies
                    if ratio == 1.0:
                        break

        if best_ratio < MatchingThreshold:  # Threshold for problematic matches
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

    # Log detailed analysis
    logger.info("Company Name Similarity Analysis:")
    logger.info(f"Total comparisons made: {stats['total_comparisons']}")
    logger.info(f"Mean similarity: {stats['mean_similarity']:.3f}")
    logger.info(f"Median similarity: {stats['median_similarity']:.3f}")
    logger.info(f"Minimum similarity: {stats['min_similarity']:.3f}")
    logger.info(f"Maximum similarity: {stats['max_similarity']:.3f}")
    logger.info(f"Standard deviation: {stats['std_similarity']:.3f}")

    if problematic_matches:
        logger.info(
            f"Potentially Problematic Matches (similarity < {MatchingThreshold}):"
        )
        for match in sorted(problematic_matches, key=lambda x: x["similarity"]):
            logger.info(f"CSV: {match['csv_company']}")
            logger.info(f"Best Match: {match['best_match']}")
            logger.info(f"Similarity: {match['similarity']:.3f}")
            logger.info("-" * 50)

    return stats


def load_data(
    csv_file_path: str,
    xlsx_file_path: str = "input_excel.xlsx",
    sheet_name: str = "Sheet1",
):
    """
    Load and normalize data from CSV and Excel files.

    Args:
        csv_file_path (str): Path to the CSV file containing machine data
        xlsx_file_path (str): Path to the Excel or CSV file containing company data
        sheet_name (str): Sheet name to use if xlsx_file_path is an Excel file

    Returns:
        Tuple[DataFrame, DataFrame]: Processed machine data and company data

    Raises:
        ValueError: If required columns are missing or file format is invalid
    """
    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/2:Starting data loading from {csv_file_path} and {xlsx_file_path}"
    )  # Progress Start
    try:
        # Process machine data from first input (always CSV)
        machine_data = process_machine_data(csv_file=csv_file_path)
        logger.info(
            f"PROGRESS:extracting_machine:merge_data:1/2:Loaded machine data ({len(machine_data)} companies)"
        )  # Progress Step 1

        # Determine file type based on extension for second input file
        _, file_extension = os.path.splitext(xlsx_file_path)
        file_extension = file_extension.lower()

        # Load the second input file based on its extension
        if file_extension in [".xlsx", ".xls"]:
            logger.info(f"Loading Excel file: {xlsx_file_path}")
            second_df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name)
        elif file_extension == ".csv":
            logger.info(f"Loading CSV file: {xlsx_file_path}")
            second_df = pd.read_csv(xlsx_file_path)
        else:
            raise ValueError(
                f"Unsupported file format: {file_extension}. Only .xlsx, .xls, and .csv are supported."
            )

        # Check for required column (Firma1 or Company)
        if "Firma1" in second_df.columns:
            logger.info("Using 'Firma1' column from input file")
        elif "Company" in second_df.columns:
            logger.info("Using 'Company' column from input file (renaming to 'Firma1')")
            # Rename to Firma1 for consistency with rest of the code
            second_df = second_df.rename(columns={"Company": "Firma1"})
        elif "company name" in second_df.columns:
            logger.info(
                "Using 'company name' column from input file (renaming to 'Firma1')"
            )
            # Rename to Firma1 for consistency with rest of the code
            second_df = second_df.rename(columns={"company name": "Firma1"})
        else:
            raise ValueError(
                "Second input file must contain either 'Firma1' or 'Company' or 'company' column"
            )

        # Ensure Firma1 is string dtype
        second_df["Firma1"] = second_df["Firma1"].astype(str)
        second_df["Firma1"] = second_df["Firma1"].apply(normalize_company_name)
        machine_data["Company"] = machine_data["Company"].apply(normalize_company_name)

        logger.info(
            f"PROGRESS:extracting_machine:merge_data:2/2:Loaded company data ({len(second_df)} rows)"
        )  # Progress End
        return machine_data, second_df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


def create_company_mapping(machine_data, company_df):
    """Create mapping between CSV companies and company data file companies using fuzzy matching."""
    company_mapping = {}
    similarity_scores = []
    company_list = company_df["Firma1"].dropna().tolist()
    unique_csv_companies = machine_data["Company"].unique()
    total_companies = len(unique_csv_companies)

    # Track matching statistics
    matched_companies = 0
    # Keep track of 5 lowest pairs using a list of tuples (similarity, csv_company, company_name)
    lowest_pairs = [(1.0, "", "")] * 5
    threshold = 0.85

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/{total_companies}:Starting company name mapping for {total_companies} companies"
    )  # Progress Start

    for i, csv_company in enumerate(unique_csv_companies):
        best_match, ratio = find_best_match(csv_company, company_list, threshold)
        # Progress Log Inside Loop
        logger.info(
            f"PROGRESS:extracting_machine:merge_data:{i + 1}/{total_companies}:Mapping '{csv_company}' -> '{best_match}' (Score: {ratio:.2f})"
        )
        if best_match:
            company_mapping[csv_company] = best_match
            similarity_scores.append(ratio)
            matched_companies += 1

            # Update lowest pairs list
            lowest_pairs.append((ratio, csv_company, best_match))
            # Sort by similarity and keep only 5 lowest
            lowest_pairs.sort(key=lambda x: x[0])
            lowest_pairs = lowest_pairs[:5]

    # Log matching statistics
    if similarity_scores:
        avg_similarity = sum(similarity_scores) / len(similarity_scores)
        logger.info("Matching Statistics for companies:")
        logger.info(f"Total companies processed: {total_companies}")
        logger.info(f"Successfully matched: {matched_companies}")
        logger.info(f"Average similarity score: {avg_similarity:.2f}")
        logger.info(f"5 Lowest Similarity Pairs | Threshold: {threshold}")
        for similarity, csv_comp, comp_name in lowest_pairs:
            logger.info(f"Score: {similarity:.3f} | {csv_comp} -> {comp_name}")

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:{matched_companies}/{total_companies}:Finished mapping, {matched_companies} matched"
    )  # Progress End
    return company_mapping


def merge_datasets(xlsx_df, machine_data, company_mapping, top_n):
    """Merge Excel and CSV data using the company mapping."""
    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/1:Starting main dataset merge ({len(xlsx_df)} base rows, {len(machine_data)} machine rows)"
    )  # Progress Start
    # Create a new column with mapped company names
    machine_data["Mapped_Company"] = machine_data["Company"].map(company_mapping)

    # Ensure both columns are string type before merging
    xlsx_df["Firma1"] = xlsx_df["Firma1"].astype(str)
    machine_data["Mapped_Company"] = machine_data["Mapped_Company"].astype(str)

    # Merge the dataframes using the mapped companies
    merged_df = pd.merge(
        xlsx_df, machine_data, left_on="Firma1", right_on="Mapped_Company", how="left"
    )
    # Define base columns to keep from the original excel/company file
    base_columns = ["Firma1"]
    if "URL" in xlsx_df.columns:
        base_columns.append("URL")
    if "Ort" in xlsx_df.columns:
        base_columns.append("Ort")
    if "location" in xlsx_df.columns:
        base_columns.append("location")
    if "url" in xlsx_df.columns:
        base_columns.append("url")

    # Define columns to keep from the machine data
    machine_cols = [f"Top{i + 1}_Machine" for i in range(top_n)]
    machine_related_cols = machine_cols + ["Maschinen_Park_Size"]

    # Combine the lists of columns to keep
    columns_to_keep = base_columns + machine_related_cols

    # Filter columns - only select columns that actually exist in the merged_df
    # This prevents KeyErrors if a column (like TopX_Machine) doesn't exist due to lack of data
    existing_columns_to_keep = [
        col for col in columns_to_keep if col in merged_df.columns
    ]
    merged_df = merged_df[existing_columns_to_keep]

    # Note: We're not filtering rows here anymore
    logger.info(
        f"PROGRESS:extracting_machine:merge_data:1/1:Finished main dataset merge, result has {len(merged_df)} rows"
    )  # Progress End
    return merged_df


def save_merged_data(
    merged_df, csv_file_path: str, output_file_path: Optional[str] = None
) -> str:
    """Save the merged dataframe to a CSV file with date in filename, or to a specified output path."""
    current_date = datetime.now().strftime("%Y%m%d")
    
    # If merged_df is empty, create an empty DataFrame with the desired column headers
    if merged_df.empty:
        logger.info("Creating empty DataFrame with headers only")
        merged_df = pd.DataFrame(columns=[
            "Firma1", "location", "url", "Top1_Machine", 
            "Maschinen_Park_Size", "Sachanlagen"
        ])
    
    # Ensure all required columns exist in the DataFrame, add empty columns if missing
    required_columns = ["Firma1", "location", "url", "Top1_Machine", 
                        "Maschinen_Park_Size", "Sachanlagen"]
    # Create a set of existing column names in lowercase for case-insensitive check
    existing_columns_lower = {c.lower() for c in merged_df.columns}

    for req_col in required_columns:
        if req_col.lower() not in existing_columns_lower:
            logger.info(f"Adding missing column (case-insensitive check): {req_col}")
            merged_df[req_col] = np.nan # Use original req_col casing
    
    # Save the DataFrame with the output path
    if output_file_path is not None:
        merged_df.to_csv(output_file_path, index=False)
        return output_file_path
    else:
        output_file = f"merged_data_{current_date}.csv"
        merged_df.to_csv(output_file, index=False)
        return output_file


def load_sachanlagen_data(sachanlagen_path):
    """Load Sachanlagen data from CSV file."""
    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/1:Starting Sachanlagen data loading from {sachanlagen_path}"
    )  # Progress Start
    try:
        sachanlagen_df = pd.read_csv(sachanlagen_path)
        # Create a mapping from lower-case column names to actual column names
        col_map = {col.lower(): col for col in sachanlagen_df.columns}
        # Required columns (case-insensitive)
        required_cols = ["company_name", "sachanlagen"]
        if not all(col in col_map for col in required_cols):
            logger.warning(f"Required columns not found in {sachanlagen_path}")
            logger.info(
                "PROGRESS:extracting_machine:merge_data:0/0:Required columns missing, returning empty DataFrame"
            )  # Progress End (Error)
            return pd.DataFrame()

        # Rename columns to expected lowercase names
        sachanlagen_df = sachanlagen_df.rename(
            columns={
                col_map["company_name"]: "company_name",
                col_map["sachanlagen"]: "sachanlagen",
            }
        )

        # Normalize company names
        sachanlagen_df["company_name"] = sachanlagen_df["company_name"].apply(
            normalize_company_name
        )
        logger.info(
            f"PROGRESS:extracting_machine:merge_data:1/1:Loaded {len(sachanlagen_df)} Sachanlagen entries"
        )  # Progress End
        return sachanlagen_df
    except Exception as e:
        logger.error(f"Error loading Sachanlagen data: {str(e)}")
        logger.info(
            "PROGRESS:extracting_machine:merge_data:0/0:Error loading data, returning empty DataFrame"
        )  # Progress End (Error)
        return pd.DataFrame()  # Return empty dataframe on error


def create_sachanlagen_mapping(sachanlagen_df, company_df):
    """Create mapping between Sachanlagen companies and company data file companies using fuzzy matching."""
    sachanlagen_mapping = {}
    company_list = company_df["Firma1"].dropna().tolist()
    unique_sachanlagen_companies = sachanlagen_df["company_name"].unique()
    total_companies = len(unique_sachanlagen_companies)
    matched_companies = 0
    threshold = 0.85  # Same threshold as main mapping

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/{total_companies}:Starting Sachanlagen mapping for {total_companies} companies"
    )  # Progress Start

    for i, sachanlagen_company in enumerate(unique_sachanlagen_companies):
        best_match, ratio = find_best_match(
            sachanlagen_company, company_list, threshold
        )
        # Progress Log Inside Loop
        logger.info(
            f"PROGRESS:extracting_machine:merge_data:{i + 1}/{total_companies}:Mapping Sachanlagen '{sachanlagen_company}' -> '{best_match}' (Score: {ratio:.2f})"
        )
        if best_match:
            sachanlagen_mapping[sachanlagen_company] = best_match
            matched_companies += 1

    logger.info(
        f"Finished Sachanlagen mapping, {matched_companies} matched"
    )  # Progress End
    return sachanlagen_mapping


def merge_with_sachanlagen(merged_df, sachanlagen_df, sachanlagen_mapping):
    """Merge the main dataframe with Sachanlagen data."""
    if sachanlagen_df.empty or sachanlagen_df is None:
        logger.info("Sachanlagen data is empty or None, adding empty Sachanlagen column.")
        merged_df["Sachanlagen"] = np.nan  # Add empty column if no data
        return merged_df

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/1:Starting Sachanlagen merge ({len(merged_df)} base rows, {len(sachanlagen_df)} Sachanlagen rows)"
    )  # Progress Start

    # Map Sachanlagen company names to the main company list names
    sachanlagen_df["Mapped_Company"] = sachanlagen_df["company_name"].map(
        sachanlagen_mapping
    )

    # Find the actual sachanlagen column in a case-insensitive way
    sachanlagen_col = next(
        (col for col in sachanlagen_df.columns if col.lower() == "sachanlagen"), None
    )
    # Select only necessary columns and drop duplicates based on the mapped name
    sachanlagen_to_merge = sachanlagen_df[["Mapped_Company", sachanlagen_col]].dropna(
        subset=["Mapped_Company"]
    )
    # Rename the column to "Sachanlagen" for consistency
    sachanlagen_to_merge = sachanlagen_to_merge.rename(columns={sachanlagen_col: "Sachanlagen"})
    sachanlagen_to_merge = sachanlagen_to_merge.drop_duplicates(
        subset=["Mapped_Company"], keep="first"
    )

    # Ensure merge keys are string type
    merged_df["Firma1"] = merged_df["Firma1"].astype(str)
    sachanlagen_to_merge["Mapped_Company"] = sachanlagen_to_merge["Mapped_Company"].astype(str)

    # Merge with the main dataframe
    final_merged_df = pd.merge(
        merged_df,
        sachanlagen_to_merge,
        left_on="Firma1",
        right_on="Mapped_Company",
        how="left",
    )

    # Drop the extra Mapped_Company column from the merge
    if "Mapped_Company" in final_merged_df.columns:
        final_merged_df = final_merged_df.drop(columns=["Mapped_Company"])

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:1/1:Finished Sachanlagen merge, result has {len(final_merged_df)} rows"
    )  # Progress End
    return final_merged_df


def main(
    csv_file_path: str,
    original_company_file_path: str = "input_excel.xlsx",
    output_file_path: Optional[str] = None,
    top_n: int = 1,
    sachanlagen_path: Optional[str] = None,
    sheet_name: str = "Sheet1",
) -> Optional[str]:
    """
    Main function to orchestrate the merging of CSV machine data with company data.

    Args:
        csv_file_path (str): Path to the CSV file containing machine data
        company_file_path (str): Path to the company data file (Excel .xlsx/.xls or CSV .csv)
        output_file_path (Optional[str]): Path to save the merged output CSV
        top_n (int, optional): Number of top machines to extract. Defaults to 1.
        sachanlagen_path (str, optional): Path to the Sachanlagen CSV file. Defaults to None.
        sheet_name (str): Excel sheet name (only used if company_file_path is Excel). Defaults to 'Sheet1'.

    Returns:
        str: Path to the output CSV file
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logger.setLevel(logging.INFO)

    logger.info(
        f"PROGRESS:extracting_machine:merge_data:0/1:Starting merge process for {csv_file_path} and {original_company_file_path}"
    )  # Progress Start
    try:
        # Load data with automatic format detection
        machine_data, company_df = load_data(
            csv_file_path, original_company_file_path, sheet_name
        )

        # If company data is empty, we cannot proceed as we need company names for mapping
        if company_df.empty:
            logger.error(
                "Company data file is empty. No output will be created."
            )
            return None
            
        # Machine data can be empty - we'll continue if sachanlagen data exists
        has_sachanlagen = sachanlagen_path and os.path.exists(sachanlagen_path)
        if machine_data.empty and not has_sachanlagen:
            logger.error(
                "Input CSV is empty and no sachanlagen data available. No output will be created."
            )
            return None

        # Initialize merged_df with company data
        merged_df = company_df.copy()
        
        # Only perform machine data matching and merging if machine data is not empty
        if not machine_data.empty:
            similarities = analyze_company_similarities(machine_data, company_df)
            good_matches = len(machine_data["Company"].unique()) - len(
                similarities["problematic_matches"]
            )
            logger.info(
                f"Found {good_matches} good matches out of {len(machine_data['Company'].unique())} machine companies"
            )

            mapping = create_company_mapping(machine_data, company_df)
            logger.debug(f"Mapping created with {len(mapping)} matches")
            merged_df = merge_datasets(company_df, machine_data, mapping, top_n)
        else:
            logger.info("Machine data is empty, skipping machine data merge")

        if sachanlagen_path and os.path.exists(sachanlagen_path):
            sachanlagen_df = load_sachanlagen_data(sachanlagen_path)
            sachanlagen_mapping = create_sachanlagen_mapping(sachanlagen_df, company_df)
            merged_df = merge_with_sachanlagen(
                merged_df, sachanlagen_df, sachanlagen_mapping
            )

        # Check for machine values when machine columns exist
        machine_cols = [f"Top{i + 1}_Machine" for i in range(top_n)]
        
        # Initialize with a Series of False values matching dataframe length
        has_machine_value = pd.Series(False, index=merged_df.index)
        
        # Only check machine columns that actually exist in the merged dataframe
        machine_cols = [col for col in machine_cols if col in merged_df.columns]
        if machine_cols:
            for col in machine_cols:
                has_machine_value = has_machine_value | merged_df[col].notna()
        
        # Case-insensitive check for 'sachanlagen' column
        sachanlagen_col = next(
            (col for col in merged_df.columns if col.lower() == "sachanlagen"), None
        )
        
        # Initialize with a Series of False values matching dataframe length
        has_sachanlagen_value = pd.Series(False, index=merged_df.index)
        if sachanlagen_col:
            has_sachanlagen_value = merged_df[sachanlagen_col].notna()
        
        # Only filter rows if we have either machine data or sachanlagen data
        if machine_cols or sachanlagen_col:
            filtered_df = merged_df[has_machine_value | has_sachanlagen_value]
        else:
            # If neither data source exists, use the merged_df as is (company data only)
            filtered_df = merged_df
            logger.warning("Neither machine data nor sachanlagen data found, output will contain only company information")
        
        # If output is completely empty (not even company data), do not create file
        if filtered_df.empty:
            logger.error("Merged output is empty. No output will be created.")
            return None

        if output_file_path is not None:
            output_file = save_merged_data(filtered_df, csv_file_path, output_file_path)
        else:
            output_file = save_merged_data(filtered_df, csv_file_path)

        logger.info(
            f"PROGRESS:extracting_machine:merge_data:1/1:Merge process completed, output: {output_file}"
        )  # Progress End
        return output_file
    except Exception as e:
        logger.error(f"Failed to merge CSV and company data: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge machine data CSV with company data file."
    )
    parser.add_argument(
        "csv_file", type=str, help="Path to the CSV file containing machine data"
    )
    parser.add_argument(
        "company_file",
        type=str,
        help="Path to the company data file (Excel .xlsx/.xls or CSV .csv)",
    )
    parser.add_argument(
        "--output", type=str, default=None, help="Path to save the merged output CSV"
    )
    parser.add_argument(
        "--top_n",
        type=int,
        default=1,
        help="Number of top machines to include (default: 1)",
    )
    parser.add_argument(
        "--sachanlagen", type=str, help="Path to Sachanlagen CSV file", default=None
    )
    parser.add_argument(
        "--sheet",
        type=str,
        default="Sheet1",
        help="Excel sheet name (only used for Excel files, default: Sheet1)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Configure logging with specified level
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        level=log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logger.setLevel(log_level)

    main(
        csv_file_path=args.csv_file,
        original_company_file_path=args.company_file,
        output_file_path=args.output,
        top_n=args.top_n,
        sachanlagen_path=args.sachanlagen,
        sheet_name=args.sheet,
    )
