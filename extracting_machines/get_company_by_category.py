#!/usr/bin/env python3
import argparse
import logging
import os
import re
from typing import Optional

import pandas as pd


def clean_company_name(name: str) -> str:
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


def extract_companies_by_category(input_file: str, category: str, output_file: Optional[str] = None) -> str:
    """
    Extract companies from an Excel file based on a specific category.
    Args:
        input_file (str): Path to the Excel file
        category (str): Category to filter by
        output_file (str, optional): Name of the output CSV file. If not provided,
                                    defaults to 'company_{category}_BA.csv'
    Returns:
        str: Path to the output file
    Raises:
        FileNotFoundError: If the input file does not exist
        ValueError: If required columns are missing
    """
    logger = logging.getLogger(__name__)
    try:
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            raise FileNotFoundError(f"Input file not found: {input_file}")
        df = pd.read_excel(input_file, sheet_name=0)
        required_columns = ['Firma1', 'Ort', 'Kategorie', 'URL']
        output_columns = {'Firma1': 'company name', 'Ort': 'location', 'URL': 'url'}
        if not all(col in df.columns for col in required_columns):
            # If the file already contains only the required output columns, skip filtering
            if set(['company name', 'location', 'url']).issubset(df.columns) and 'Kategorie' not in df.columns:
                logger.info("Input file already contains required columns. Skipping category filtering.")
                result_df = df[['company name', 'location', 'url']].copy()
            else:
                logger.error(f"Missing required columns. Found columns: {df.columns.tolist()}")
                raise ValueError(f"Input file must contain columns: {required_columns}")
        else:
            filtered_df = df[df['Kategorie'] == category]
            result_df = filtered_df[['Firma1', 'Ort', 'URL']].copy()
            result_df = result_df.rename(columns=output_columns)
            result_df['company name'] = result_df['company name'].apply(clean_company_name)
        if output_file is None:
            base_dir = os.path.dirname(os.path.abspath(input_file))
            output_file = os.path.join(base_dir, f'company_{category}_BA.csv')
        else:
            output_file = os.path.abspath(output_file)
        result_df.to_csv(output_file, index=False)
        logger.info(f"Extracted {len(result_df)} companies in category '{category}'")
        logger.info(f"Results saved to {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error extracting companies: {str(e)}")
        raise


def main(input_file: str, category: str, output_file: Optional[str] = None) -> str:
    """
    Main entry point for extracting companies by category.
    Args:
        input_file (str): Path to the input Excel file
        category (str): Category to filter by
        output_file (str, optional): Output CSV file name
    Returns:
        str: Path to the output file
    """
    return extract_companies_by_category(input_file, category, output_file)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Extract companies by category from Excel file')
    parser.add_argument('input_file', help='Path to the input Excel file')
    parser.add_argument('category', help='Category to filter by')
    parser.add_argument('--output', '-o', help='Output CSV file name (default: company_<category>_BA.csv)')
    args = parser.parse_args()
    try:
        output_path = main(args.input_file, args.category, args.output)
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)
