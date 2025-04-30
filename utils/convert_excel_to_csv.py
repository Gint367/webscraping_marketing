import os
import sys
from pathlib import Path

import pandas as pd


def list_sheets(excel_file):
    """List all sheets in an Excel file"""
    try:
        xl = pd.ExcelFile(excel_file)
        print(f"Available sheets in {os.path.basename(excel_file)}:")
        for idx, sheet in enumerate(xl.sheet_names, 1):
            print(f"{idx}. {sheet}")
        return xl.sheet_names
    except Exception as e:
        print(f"Error reading sheets: {str(e)}")
        sys.exit(1)


def convert_sheet(excel_file, sheet_name, output_file=None, output_dir=None):
    """Convert a single sheet to CSV"""
    try:
        # Read the Excel file with specified sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Determine output file name
        if output_file is None:
            base_name = Path(excel_file).stem
            output_name = f"{base_name}_{sheet_name}.csv"

            if output_dir:
                # Create output directory if it doesn't exist
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, output_name)
            else:
                output_path = output_name
        else:
            output_path = output_file

        # Convert to CSV
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Converted sheet '{sheet_name}' to {output_path}")
        return output_path

    except Exception as e:
        print(f"Error converting sheet '{sheet_name}': {str(e)}")
        return None


def convert_all_sheets(excel_file, output_dir=None):
    """Convert all sheets in an Excel file to separate CSV files"""
    try:
        xl = pd.ExcelFile(excel_file)
        converted_files = []

        for sheet in xl.sheet_names:
            output_file = convert_sheet(excel_file, sheet, output_dir=output_dir)
            if output_file:
                converted_files.append(output_file)

        print(f"Converted {len(converted_files)} sheets from {excel_file}")
        return converted_files

    except Exception as e:
        print(f"Error converting all sheets: {str(e)}")
        sys.exit(1)


def interactive_mode(excel_file, output_dir=None):
    """Interactive mode for selecting sheets to convert"""
    sheets = list_sheets(excel_file)

    print("\nOptions:")
    print("a. Convert all sheets")
    print("q. Quit")
    print("Or enter a sheet number to convert a single sheet")

    while True:
        choice = input("\nYour choice: ").strip().lower()

        if choice == 'q':
            return
        elif choice == 'a':
            convert_all_sheets(excel_file, output_dir)
            return
        else:
            try:
                sheet_idx = int(choice) - 1
                if 0 <= sheet_idx < len(sheets):
                    convert_sheet(excel_file, sheets[sheet_idx], output_dir=output_dir)
                    return
                else:
                    print(f"Invalid selection. Please enter a number between 1 and {len(sheets)}")
            except ValueError:
                print("Please enter a valid option")

if __name__ == "__main__":
    if len(sys.argv) == 1 or "--help" in sys.argv or "-h" in sys.argv:
        print("Usage:")
        print("  python convert_excel_to_csv.py <excel_file> [options]")
        print("\nOptions:")
        print("  --list-sheets                List all sheets in the Excel file")
        print("  --sheet <sheet_name>         Convert specified sheet to CSV")
        print("  --all-sheets                 Convert all sheets to separate CSV files")
        print("  --output-dir <directory>     Specify output directory for CSV files")
        print("  --output <file>              Specify output file name (only for single sheet)")
        print("  --interactive                Interactive mode for selecting sheets")
        print("\nExamples:")
        print("  python convert_excel_to_csv.py data.xlsx --list-sheets")
        print("  python convert_excel_to_csv.py data.xlsx --sheet Sheet1")
        print("  python convert_excel_to_csv.py data.xlsx --all-sheets --output-dir csv_output")
        print("  python convert_excel_to_csv.py data.xlsx --interactive")
        sys.exit(0)

    excel_file = sys.argv[1]

    # Check if file exists
    if not os.path.isfile(excel_file):
        print(f"Error: File {excel_file} not found")
        sys.exit(1)

    # Parse command line arguments
    output_file = None
    output_dir = None
    sheet_name = None
    convert_all = False
    interactive = False

    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--list-sheets":
            list_sheets(excel_file)
            sys.exit(0)
        elif sys.argv[i] == "--sheet" and i + 1 < len(sys.argv):
            sheet_name = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--output" and i + 1 < len(sys.argv):
            output_file = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--output-dir" and i + 1 < len(sys.argv):
            output_dir = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--all-sheets":
            convert_all = True
            i += 1
        elif sys.argv[i] == "--interactive":
            interactive = True
            i += 1
        else:
            print(f"Unknown option: {sys.argv[i]}")
            sys.exit(1)

    # Execute the appropriate conversion based on arguments
    if interactive:
        interactive_mode(excel_file, output_dir)
    elif convert_all:
        convert_all_sheets(excel_file, output_dir)
    elif sheet_name:
        convert_sheet(excel_file, sheet_name, output_file, output_dir)
    else:
        # If no specific options provided, default to interactive mode
        interactive_mode(excel_file, output_dir)
