import pandas as pd
import sys
import os

def list_sheets(excel_file):
    try:
        xl = pd.ExcelFile(excel_file)
        print("Available sheets:")
        for sheet in xl.sheet_names:
            print(f"- {sheet}")
    except Exception as e:
        print(f"Error reading sheets: {str(e)}")
        sys.exit(1)

def select_sheet(excel_file):
    try:
        xl = pd.ExcelFile(excel_file)
        sheets = xl.sheet_names
        
        print("\nAvailable sheets:")
        for idx, sheet in enumerate(sheets, 1):
            print(f"{idx}. {sheet}")
            
        while True:
            try:
                choice = int(input("\nSelect sheet number: ")) - 1
                if 0 <= choice < len(sheets):
                    return sheets[choice]
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
    except Exception as e:
        print(f"Error reading sheets: {str(e)}")
        sys.exit(1)

def excel_to_csv(excel_file, output_file=None, sheet_name=None):
    try:
        if sheet_name is None:
            sheet_name = select_sheet(excel_file)
            
        # Read the Excel file with specified sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        # If no output file specified, use the same name with .csv extension
        if output_file is None:
            base_name = os.path.splitext(excel_file)[0]
            output_file = f"{base_name}_{sheet_name}.csv"
        
        # Convert to CSV
        df.to_csv(output_file, index=False)
        print(f"Successfully converted sheet '{sheet_name}' from {excel_file} to {output_file}")
        
    except Exception as e:
        print(f"Error converting file: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_excel_to_csv.py <excel_file> [output_file] [sheet_name]")
        print("       python export_excel_to_csv.py --list-sheets <excel_file>")
        sys.exit(1)

    if sys.argv[1] == "--list-sheets":
        if len(sys.argv) < 3:
            print("Please provide an Excel file path")
            sys.exit(1)
        list_sheets(sys.argv[2])
    else:
        excel_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        sheet_name = sys.argv[3] if len(sys.argv) > 3 else None
        
        excel_to_csv(excel_file, output_file, sheet_name)
