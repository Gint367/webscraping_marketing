import pandas as pd

# Load the CSV file
df = pd.read_csv('/home/novoai/Documents/scraper/company_list_with_product_Sheet1.csv')

# Filter rows where at least one of 'Produkt 1', 'Produkt 2', or 'Produkt 3' is not empty
filtered_df = df.dropna(subset=['Produkt 1', 'Produkt 2', 'Produkt 3'], how='all')

# Save the cleaned data to a new CSV file
filtered_df.to_csv('/home/novoai/Documents/scraper/company_list_with_product_Sheet1_cleaned.csv', index=False)
