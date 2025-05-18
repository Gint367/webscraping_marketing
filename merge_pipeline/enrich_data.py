import argparse
import logging
import os
import os.path
import re
import sys

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Create a module-specific logger
logger = logging.getLogger("integrate_pipeline.enrich_data")
logger.setLevel(logging.INFO)

# Define the constant for hours calculation
HOURS_MULTIPLIER = 375


def extract_first_number(range_value):
    """
    Extract the first number from a range string like '15-20'

    Args:
        range_value: String or value from which to extract a number

    Returns:
        int: The first number found in the range, or None if no valid number is found
        or input is 'No Match'
    """
    if isinstance(range_value, str):
        if range_value == "No Match":
            return None
        # Extract the first number from patterns like '15-20'
        match = re.match(r"(\d+)[-]?\d*", range_value)
        if match:
            return int(match.group(1))
    return None


def enrich_data(input_file):
    """
    Enrich a CSV file with calculated Maschinen_Park_var and hours_of_saving columns.

    Args:
        input_file: Path to the input CSV file

    Returns:
        str: Path to the enriched output file

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If required columns are missing or CSV is malformed
    """
    # Generate output file path with "enriched_" prefix
    input_basename = os.path.basename(input_file)
    output_basename = f"enriched_{input_basename}"
    output_file = os.path.join(os.path.dirname(input_file), output_basename)

    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Input file '{input_file}' not found."
        )  # Read the CSV file
    try:
        logger.info(f"Reading data from {input_file}")
        df = pd.read_csv(input_file, encoding="utf-8", skipinitialspace=True)

        # Check if the input file is empty (has only header row or no rows at all)
        if len(df) == 0:
            logger.info(
                f"Input file '{input_file}' is empty, creating empty output file"
            )
            # Create an empty output file
            open(output_file, "w").close()
            return output_file
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file: {e}")
        raise ValueError(f"Error parsing CSV file: {e}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise ValueError(f"Error reading CSV file: {e}")

    # Validate required columns exist
    required_columns = ["Maschinen_Park_Size"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    total_records = len(df)
    logger.info(
        f"PROGRESS:integration:enrich_data:0/{total_records}:Starting enrichment for {input_basename}"
    )

    # Create the Maschinen_Park_var column as integer
    df["Maschinen_Park_var"] = df["Maschinen_Park_Size"].apply(extract_first_number)

    # Convert Maschinen_Park_var column to integer dtype (with NaN values preserved)
    df["Maschinen_Park_var"] = pd.to_numeric(df["Maschinen_Park_var"], errors="coerce")
    df["Maschinen_Park_var"] = df["Maschinen_Park_var"].astype("Int64")

    # Create the hours_of_saving column as integer, handling NaN values properly
    # Use numpy to multiply, which handles Int64 NA values correctly
    df["hours_of_saving"] = df["Maschinen_Park_var"].mul(HOURS_MULTIPLIER)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")

    # Save the enriched data
    try:
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        # Log completion progress
        logger.info(
            f"PROGRESS:integration:enrich_data:{total_records}/{total_records}:Enrichment completed for {input_basename}"
        )
        logger.info(
            f"Data enrichment completed successfully! Output saved to {output_file}"
        )
    except Exception as e:
        logger.error(f"Error saving output file: {e}")
        raise ValueError(f"Error saving output file: {e}")

    # After saving, print a summary
    total_records = len(df)
    logger.info(f"Processed {total_records} records")

    return output_file


def main():
    """
    Main function to handle command-line arguments and execute the data enrichment process.

    Returns:
        str: Path to the enriched output file

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If required columns are missing or CSV is malformed
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Enrich CSV data with Maschinen_Park_var and hours_of_saving columns"
    )
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Set the logging level based on the command-line argument
    logger.setLevel(getattr(logging, args.log_level))

    # Process the input file
    output_path = enrich_data(args.input_file)

    # Return the output path for use in pipelines
    return output_path


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Value error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
