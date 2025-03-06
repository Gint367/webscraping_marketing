import os
import csv
import unicodedata
import shutil
import sys

def sanitize_filename(name: str) -> str:
    """Sanitizes strings (company names, report names) by replacing spaces, ampersands, German Umlauts, etc."""
    umlaut_map = {
        "ä": "ae", "ö": "oe", "ü": "ue", 
        "Ä": "Ae", "Ö": "Oe", "Ü": "Ue", 
        "ß": "ss",
    }
    for umlaut, replacement in umlaut_map.items():
        name = name.replace(umlaut, replacement)
    name = unicodedata.normalize("NFKD", name)
    name = name.replace(" ", "_").replace("&", "and")
    name = name.replace("/", "_")
    return name

def get_companies_from_csv(csv_path):
    companies = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            # Skip header row
            next(csv_reader, None)
            for row in csv_reader:
                if row and len(row) > 0:
                    companies.append(row[0])
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    return companies

def main():
    # Path to the CSV file
    csv_path = 'enriched_companies.csv'
    
    # Path to the directory to search
    target_dir = 'bundesanzeiger_local_maschinenbau'
    
    if not os.path.exists(target_dir):
        print(f"Directory not found: {target_dir}")
        sys.exit(1)
    
    # Get company names from CSV
    companies = get_companies_from_csv(csv_path)
    
    # Sanitize company names
    sanitized_companies = [sanitize_filename(company) for company in companies]
    
    # Get list of existing folders
    existing_folders = [f for f in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, f))]
    
    # Find matching folders and non-matching companies
    matching_folders = []
    non_matching_companies = []
    
    for idx, sanitized_name in enumerate(sanitized_companies):
        folder_path = os.path.join(target_dir, sanitized_name)
        
        if sanitized_name in existing_folders:
            matching_folders.append((folder_path, companies[idx]))
        else:
            non_matching_companies.append((companies[idx], sanitized_name))
    
    # Delete matching folders without confirmation
    print(f"\nDeleting {len(matching_folders)} matching folders:")
    for folder_path, original_name in matching_folders:
        try:
            print(f"Deleting: {folder_path} (Original name: {original_name})")
            shutil.rmtree(folder_path)
        except Exception as e:
            print(f"Error deleting folder {folder_path}: {e}")
    
    # Display companies from CSV that don't have matching folders
    print(f"\nFound {len(non_matching_companies)} companies without matching folders:")
    for original_name, sanitized_name in non_matching_companies:
        print(f"No match for: {original_name} (Sanitized: {sanitized_name})")

if __name__ == "__main__":
    main()
