import os
import asyncio
import json
import pandas as pd
import re
from pydantic import BaseModel, Field
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from urllib.parse import urlparse
from datetime import datetime
from collections import Counter

#prompt = "Extrahieren Sie aus dem Text die fünf prägnantesten und spezifischsten Schlüsselwörter im Zusammenhang mit Maschinentypen (auf Deutsch). Vermeiden Sie dabei Wiederholungen sowie generische oder unspezifische Begriffe wie \"Automatisierung\", \"Technologie\" oder \"Produktion\" und ähnliche allgemeine Begriffe." 
#prompt = "Extrahieren Sie die fünf wichtigsten erwähnten Fertigungsmaschinen."
prompt = (
    "From the crawled content, extract the following details:\n"
    "1. Company Name\n"
    "2. Products that the company offers(Products can either be a machines or parts produced by machines.)\n"
    "3. Machines that the company uses in their manufacturing process\n"
    "4. Type of Process that their machine uses in their manufacturing process(e.g Milling, Driling, Turning, Grinding, etc)\n"
    "5. Whether the company offers contract manufacturing services (yes or no)\n"
    "Notes: Only write if its specifically mentioned.\n"
    )

test_urls = [
    "https://www.forst-online.de/index.php/de/",
    "https://www.schroedergroup.eu/de/",
    "https://huettenbrauck.com/",
       
]

"""
    "https://www.deharde.de/de/",
    "https://maier-heidenheim.com/",
    "https://www.pacoma.com/",
    "https://www.terrot.de/de/",
    "https://www.scheuchl.de/",
    "https://www.schuster-maschinenbau.de/",
    "https://www.fmb-machinery.de/",
    "https://www.dilco.de/",
    "https://www.guelde.de/de/",
    
    "https://www.jaecklin-industrial.de/",
    "https://www.peter-wolters.de/"

"""

class Company(BaseModel):
    company_name: str = Field(..., description="Name des Unternehmens.")
    products: List[str] = Field(..., description="List of Products offered by the company.")
    machines: List[str] = Field(..., description="List of Machines used in manufacturing process.")
    process_type: List[str] = Field(..., description="List of Process Types used in manufacturing process.")
    lohnfertigung: bool = Field(..., description="Whether the company offers contract manufacturing services.")


def consolidate_company_data(data):
    """
    Consolidate multiple entries for the same company into a single entry.
    Handles the structure from crawled_data where each item in data is an array 
    containing a company object.
    """
    consolidated_data = []
    
    if not data:  # Skip if empty data
        return consolidated_data
    
    # Process each entry in the data
    for entry_list in data:
        # Each entry should be a list containing one or more company objects
        if isinstance(entry_list, list) and entry_list:
            for company in entry_list:
                if isinstance(company, dict) and 'company_name' in company:
                    # Create consolidated entry
                    consolidated_company = {
                        'company_name': company.get('company_name', ''),
                        'keywords': company.get('keywords', []),
                        'machines': company.get('machines', []),
                        'error': company.get('error', False)
                    }
                    
                    # Add URL if available
                    if 'url' in company:
                        consolidated_company['url'] = company['url']
                    
                    consolidated_data.append(consolidated_company)
        
        # Handle cases where the entry might not be a list but a direct company object
        elif isinstance(entry_list, dict) and 'company_name' in entry_list:
            consolidated_data.append(entry_list)
    
    return consolidated_data

def sanitize_filename(url):
    """Convert URL to a valid filename by removing scheme and replacing invalid characters"""
    parsed = urlparse(url)
    domain = parsed.netloc
    # Replace invalid filename characters with underscores
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', domain)
    return sanitized

def ensure_output_directory(directory="crawled_markdown"):
    """Ensure the output directory for markdown files exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


async def main():
    # Read URLs from CSV file
    csv_path = "technische_anlagen_2021_0.csv"
    #df = pd.read_csv(csv_path)
    #urls = df['Account Website'].tolist()
    urls = test_urls
    # Filter out any invalid URLs (empty or NaN values)
    urls = [url for url in urls if isinstance(url, str) and url.strip()]
    
    # Filter URLs to only the top-level domain

    def get_top_level_domain(url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    urls = list(set(get_top_level_domain(url) for url in urls))
    """
    print("First X URLs:")
    for url in urls[:5]:
        print(url)
    
    """
    # Create output directory for markdown files
    markdown_dir = ensure_output_directory()
    
    # 1. Define the LLM extraction strategy
    temperature = 0.1
    max_tokens = 800

    llm_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o-mini",            # e.g. "ollama/llama2"
        #provider="ollama/deepseek-r1:14b",
        #provider="ollama/llama3.1:latest",
        api_token='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',
        # Or use model_json_schema()
        extraction_type="schema",
        schema=Company.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=1000,
        overlap_rate=0.0,
        apply_chunking=True,
        input_format="markdown",   # or "html", "fit_markdown"
        extra_args={"temperature": temperature, "max_tokens": max_tokens}
        
    )

    # 2. Build the crawler config
    crawl_config = CrawlerRunConfig(
        extraction_strategy=llm_strategy,
        cache_mode=CacheMode.BYPASS,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=10
        
    )

    # 3. Create a browser config if needed
    browser_cfg = BrowserConfig(
        verbose=True,
        headless=True,
        text_mode=True,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        crawler.crawler_strategy.set_custom_headers(
            {"Accept-Language": "de-DE,de;q=0.9"}
        )
        # 4. Crawl multiple pages
        results = await crawler.arun_many(
            urls,  
            config=crawl_config,
            verbose=True
        )

        # Process all results
        extracted_data = []
        for result in results:
            if result.success:
                print(f"Successfully crawled: {result.url}")
                print(f"Title: {result.metadata.get('title', 'N/A')}")
                print(f"Word count: {len(result.markdown.split())}")
                print(
                    f"Number of links: {len(result.links.get('internal', [])) + len(result.links.get('external', []))}"
                )
                
                # Save the markdown content to a file
                if result.markdown:
                    filename = sanitize_filename(result.url)
                    current_date = datetime.now().strftime("%Y%m%d")
                    markdown_path = os.path.join(markdown_dir, f"{filename}_{current_date}.md")
                    
                    with open(markdown_path, "w", encoding="utf-8") as f:
                        f.write(f"# {result.metadata.get('title', 'Untitled')}\n\n")
                        f.write(f"URL: {result.url}\n")
                        f.write(f"Crawled on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                        f.write(result.markdown)
                    
                    print(f"Markdown content saved to {markdown_path}")
                
                # Add the extracted content to our data collection
                if result.extracted_content:
                    extracted_data.append(result.extracted_content)
                    print(f"Extracted content added from {result.url}")
                else:
                    print(f"No extracted content from {result.url}")
                
            else:
                print(f"Failed to crawl: {result.url if hasattr(result, 'url') else 'unknown URL'}")
                if hasattr(result, 'error'):
                    print(f"Error: {result.error}")
            
        if extracted_data:
             # 6. Show usage stats
            llm_strategy.show_usage()
            
            # Save all extracted data to a JSON file
            # Get the current date
            current_date = datetime.now().strftime("%Y%m%d")

            # Process the extracted data to ensure it's in the correct format
            processed_data = []
            for entry in extracted_data:
                # If the entry is a JSON string, parse it first
                if isinstance(entry, str):
                    try:
                        parsed_entry = json.loads(entry)
                        processed_data.append(parsed_entry)
                    except json.JSONDecodeError:
                        print(f"Warning: Could not parse entry as JSON: {entry[:100]}...")
                        continue
                else:
                    # If it's already a list or dict, add it directly
                    processed_data.append(entry)

            # Append the date to the output file name
            output_file_with_date = f"crawled_data_{current_date}.json"

            # Include the prompt and parameters at the top of the JSON data
            output_data = {
                "instruction": prompt,
                "parameters": {
                    "temperature": temperature,
                    "max_tokens": max_tokens
                },
                "data": processed_data
            }

            with open(output_file_with_date, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            print(f"Raw data saved to {output_file_with_date}")
            """ 
            # Consolidate the extracted data using the processed data
            consolidated_data = consolidate_company_data(processed_data)
            
            # Save consolidated data to a separate file
            consolidated_file = f"consolidated_data_{current_date}.json"
            with open(consolidated_file, "w", encoding="utf-8") as f:
                json.dump(consolidated_data, f, indent=4, ensure_ascii=False)
            print(f"Consolidated data saved to {consolidated_file}")
             """
           
        else:
            print("No data was successfully extracted")

if __name__ == "__main__":
    asyncio.run(main())