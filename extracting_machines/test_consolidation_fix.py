import json
from consolidate_keywords import consolidate_company_data

# Test with the sample data
input_file = "crawled_data_20250226.json"
output_file = "test_consolidated_data_fixed.json"

# Run the consolidation with our updated code
consolidated_data = consolidate_company_data(input_file, output_file)

# Print the results
print("\nResults summary:")
print("=" * 50)

# Check if we have direct data or nested data
if isinstance(consolidated_data, list):
    results = consolidated_data
else:
    results = consolidated_data.get("data", [])

for idx, entry in enumerate(results, 1):
    print(f"Company {idx}: {entry['company_name']}")
    
    # Check if keywords are dictionaries (machine info) or strings
    if entry['keywords'] and isinstance(entry['keywords'][0], dict):
        print("Machines:")
        for machine in entry['keywords']:
            name = machine.get('name', machine.get('machine_name', 'Unknown machine'))
            print(f"  - {name}")
    else:
        print(f"Keywords: {', '.join(entry['keywords'])}")
    
    print(f"Original entries: {entry['original_entries']}")
    print("-" * 50)
