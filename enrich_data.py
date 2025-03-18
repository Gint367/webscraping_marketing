import pandas as pd
import re
import os
import argparse
import os.path

# Define the constant for hours calculation
HOURS_MULTIPLIER = 375


def extract_first_number(range_value):
    """
    Extract the first number from a range string like '15-20'
    Returns None if no valid number is found or input is 'No Match'
    """
    if isinstance(range_value, str):
        if range_value == "No Match":
            return None
        # Extract the first number from patterns like '15-20'
        match = re.match(r"(\d+)[-]?\d*", range_value)
        if match:
            return int(match.group(1))
    return None


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Enrich CSV data with Maschinen_Park_var and hours_of_saving columns"
    )
    parser.add_argument("input_file", help="Path to the input CSV file")
    args = parser.parse_args()

    # Get input file path from command line argument
    input_file = args.input_file

    # Generate output file path with "enriched_" prefix
    input_basename = os.path.basename(input_file)
    output_basename = f"enriched_{input_basename}"
    output_file = os.path.join(os.path.dirname(input_file), output_basename)

    # Read the CSV file
    try:
        print(f"Reading data from {input_file}")
        df = pd.read_csv(input_file, encoding="utf-8", skipinitialspace=True)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Validate required columns exist
    required_columns = ["Maschinen_Park_Size"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {', '.join(missing_columns)}")
        return

    # Create the Maschinen_Park_var column as integer
    print("Creating Maschinen_Park_var column")
    df["Maschinen_Park_var"] = df["Maschinen_Park_Size"].apply(extract_first_number)

    # Convert Maschinen_Park_var column to integer dtype (with NaN values preserved)
    df["Maschinen_Park_var"] = pd.to_numeric(
        df["Maschinen_Park_var"], errors="coerce"
    ).astype("Int64")

    # Create the hours_of_saving column as integer, handling NaN values properly
    print(f"Creating hours_of_saving column (Maschinen_Park_var × {HOURS_MULTIPLIER})")
    # Use numpy to multiply, which handles Int64 NA values correctly
    df["hours_of_saving"] = df["Maschinen_Park_var"].mul(HOURS_MULTIPLIER)

    # Save the enriched data
    try:
        print(f"Saving enriched data to {output_file}")
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print("Data enrichment completed successfully!")
    except Exception as e:
        print(f"Error saving output file: {e}")

    # After saving, print a summary
    non_null_count = df["Maschinen_Park_var"].count()
    total_records = len(df)
    print(f"Summary: Processed {total_records} records")
    print(
        f"         {non_null_count} records with valid Maschinen_Park_var values ({non_null_count / total_records:.1%})"
    )


if __name__ == "__main__":
    main()
