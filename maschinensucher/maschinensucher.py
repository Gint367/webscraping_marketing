import argparse
import asyncio
import csv
import json
import logging
import os
import re
from typing import Any, Dict, List
from urllib.parse import quote

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlResult, CrawlerRunConfig, JsonCssExtractionStrategy, LLMConfig

async def grab_dealer_list(num_pages: int = 1) -> List[Dict[str, Any]]:
    """
    Grab dealer list from maschinensucher.de with pagination support.
    
    Args:
        num_pages (int): Number of pages to crawl, defaults to 1
        
    Returns:
        List[Dict[str, Any]]: List of dealer dictionaries containing extracted data
    """
    logging.info(f"Searching for dealers across {num_pages} pages...")
    browser_config = BrowserConfig(headless=True, verbose=True)
    
    all_dealers: List[Dict[str, Any]] = []
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                keep_attrs=["id", "class"],
                keep_data_attributes=True,
                delay_before_return_html=2.0,
                wait_for="css:body",  # Wait for page to load
            )

            for page in range(1, num_pages + 1):
                url = f"https://www.maschinensucher.de/Haendler/tci-218?page={page}"
                logging.info(f"Crawling page {page}: {url}")
                
                result = await crawler.arun(
                    url,
                    config=crawl_config,
                )  # type: ignore

                if result.success:  # type: ignore
                    schemas: Dict[str, Any] = await build_schema(result.html)  # type: ignore
                    
                    for schema in schemas.values():
                        extracted_data = JsonCssExtractionStrategy(
                            schema=schema,
                        ).run(url=url, sections=[result.html])  # type: ignore
                        
                        # Add page number and enrich with address data for each dealer entry
                        if isinstance(extracted_data, list):
                            for dealer in extracted_data:
                                if isinstance(dealer, dict):
                                    dealer['page_number'] = page
                                    dealer['source_url'] = url
                                    
                                    # Enrich with parsed address data
                                    dealer = enrich_dealer_with_address_data(dealer)
                                    
                            all_dealers.extend(extracted_data)
                        
                    logging.info(f"Extracted {len(extracted_data) if isinstance(extracted_data, list) else 0} dealers from page {page}")
                else:
                    logging.error(f"Failed to crawl page {page}: {result.error_message}")  # type: ignore

            logging.info(f"Total dealers extracted: {len(all_dealers)}")
            
        except Exception as e:
            logging.error(f"Error during crawling: {str(e)}")
            raise
    
    return all_dealers

def clean_html_address(raw_html: str) -> str:
    """
    Clean raw HTML address string by removing HTML tags and extra whitespace.
    
    Args:
        raw_html (str): Raw HTML string from address extraction
        
    Returns:
        str: Cleaned HTML string with normalized whitespace and removed HTML tags
    """
    if not raw_html:
        logging.warning("Empty raw HTML address provided")
        return ""
    
    # Remove HTML tags (like <span> and </span>)
    cleaned = re.sub(r'</?span[^>]*>', '', raw_html)
    
    # Remove leading/trailing whitespace and normalize internal whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned.strip())
    
    # Normalize <br> tags (handle various formats like <br>, <br/>, <br />)
    cleaned = re.sub(r'<br\s*/?>', '<br>', cleaned)
    
    logging.debug(f"Cleaned HTML address: {cleaned}")
    return cleaned

def parse_german_address_components(html_address: str) -> Dict[str, str]:
    """
    Parse German address components from HTML string with <br> tags.
    
    Expected format:
    "Street Address<br>Postal Code City<br>Country<br>State"
    
    Args:
        html_address (str): HTML address string with <br> tags
        
    Returns:
        Dict[str, str]: Parsed address components
    """
    logging.debug(f"Parsing German address: {html_address}")
    
    if not html_address:
        logging.warning("Empty address provided for parsing")
        return {
            'street': '',
            'postal_code': '',
            'city': '',
            'country': '',
            'state': '',
            'full_address': ''
        }
    
    # Clean the HTML first
    cleaned_html = clean_html_address(html_address)
    
    # Split by <br> tags and clean each part
    parts = [part.strip() for part in cleaned_html.split('<br>') if part.strip()]
    
    result = {
        'street': '',
        'postal_code': '',
        'city': '',
        'country': '',
        'state': '',
        'full_address': ', '.join(parts)
    }
    
    # Parse street address (first part)
    if len(parts) >= 1:
        result['street'] = parts[0]
        logging.debug(f"Extracted street: {result['street']}")
    
    # Parse postal code and city (second part)
    if len(parts) >= 2:
        postal_city_match = re.match(r'^(\d{4,5})\s+(.+)$', parts[1])
        if postal_city_match:
            result['postal_code'] = postal_city_match.group(1)
            result['city'] = postal_city_match.group(2)
            logging.debug(f"Extracted postal code: {result['postal_code']}, city: {result['city']}")
        else:
            result['city'] = parts[1]
            logging.debug(f"Extracted city (no postal code detected): {result['city']}")
    
    # Parse country (third part)
    if len(parts) >= 3:
        result['country'] = parts[2]
        logging.debug(f"Extracted country: {result['country']}")
    
    # Parse state (fourth part)
    if len(parts) >= 4:
        result['state'] = parts[3]
        logging.debug(f"Extracted state: {result['state']}")
    
    return result


def enrich_dealer_with_address_data(dealer: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich dealer data with parsed address components.
    
    Args:
        dealer (Dict[str, Any]): Raw dealer data dictionary
        
    Returns:
        Dict[str, Any]: Enhanced dealer data with parsed address components
    """
    logging.debug(f"Enriching dealer data for: {dealer.get('company_name', 'Unknown')}")
    
    # Check for raw HTML address field
    raw_html_address = dealer.get('address_raw_html', '')
    
    if not raw_html_address:
        logging.warning(f"No address_raw_html field found for dealer: {dealer.get('company_name', 'Unknown')}")
        return dealer
    
    # Parse address components
    address_components = parse_german_address_components(raw_html_address)
    
    # Add parsed components to dealer data
    dealer['address_raw'] = raw_html_address
    dealer['address_cleaned'] = clean_html_address(raw_html_address)
    dealer.update(address_components)
    
    logging.debug(f"Successfully enriched address data for: {dealer.get('company_name', 'Unknown')}")
    return dealer

async def build_schema(cleaned_html, force: bool = False) -> Dict[str, Any]:
    schemas = {}
    if os.path.exists("maschinensucher/schemas/dealer_schema.json") and not force:
        with open(
            "maschinensucher/schemas/dealer_schema.json", "r", encoding="utf-8"
        ) as f:
            schemas["dealer"] = json.load(f)
    else:
        print("Building dealer schema...")
        # extract schema from html
        dealer_schema = JsonCssExtractionStrategy.generate_schema(
            html=cleaned_html,
            llm_config=LLMConfig(
                provider="gemini/gemini-2.5-pro-preview-05-06",
                api_token=os.getenv("GEMINI_API_KEY", ""),
            ),
            target_json_example="""{
                "company name": "...",
                "address": "...",
                "distance": "...",
                "link": "",
                "attributes": "...",
            }""",
            query="""The given html is the crawled html from Maschinensucher website search result. Please find the schema for dealer information in the given html. I am interested in the dealer company name, the address, the distance from the detected location, and the maschinensucher page link for the company.
            """,
        )
        with open(
            "maschinensucher/schemas/dealer_schema.json", "w", encoding="utf-8"
        ) as f:
            json.dump(dealer_schema, f, indent=4, ensure_ascii=False)
        schemas["dealer"] = dealer_schema

    
    return schemas

def prepare_csv(dealers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare and clean dealer data for CSV export by filtering only clean data fields.
    
    Args:
        dealers (List[Dict[str, Any]]): List of dealer dictionaries from JSON data
        
    Returns:
        List[Dict[str, Any]]: List of cleaned dealer dictionaries with only specified fields
    """
    if not dealers:
        logging.warning("No dealer data to prepare for CSV")
        return []
    
    # Define the clean data fields to include in CSV
    clean_fieldnames = [
        'vertrauensiegel',
        'city', 
        'company_name',
        'country',
        'distance',
        'link',
        'page_number',
        'postal_code',
        'source_url',
        'state',
        'street'
    ]
    
    prepared_dealers = []
    
    try:
        # Clean and prepare each dealer entry
        for i, dealer in enumerate(dealers):
            clean_dealer = {}
            
            # Handle vertrauensiegel field - check if attributes contains "Vertrauenssiegel"
            attributes = dealer.get('attributes', '')
            clean_dealer['vertrauensiegel'] = 'vertrauenssiegel' in str(attributes).lower() if attributes else False
            has_vertrauensiegel = 'vertrauenssiegel' in str(attributes).lower() if attributes else False

            # Print the first 5 entries that have vertrauenssiegel
            if has_vertrauensiegel and sum(1 for d in prepared_dealers if d.get('vertrauensiegel')) < 5:
                logging.info(f"Dealer with Vertrauenssiegel: {dealer.get('company_name', 'Unknown')} | attributes='{attributes}'")

            # Handle other fields
            for field in clean_fieldnames:
                if field != 'vertrauensiegel':
                    clean_dealer[field] = dealer.get(field, '')
            
            prepared_dealers.append(clean_dealer)
        
        logging.info(f"Prepared {len(prepared_dealers)} dealer records for CSV export")
        logging.info(f"CSV fields: {', '.join(clean_fieldnames)}")
        
    except Exception as e:
        logging.error(f"Error preparing dealers for CSV: {str(e)}")
        raise
    
    return prepared_dealers


def save_to_csv(prepared_data: List[Dict[str, Any]], filename: str) -> bool:
    """
    Save prepared dealer data to a CSV file.
    
    Args:
        prepared_data (List[Dict[str, Any]]): List of cleaned dealer dictionaries
        filename (str): Output CSV filename
        
    Returns:
        bool: True if file was saved successfully, False otherwise
    """
    if not prepared_data:
        logging.warning("No prepared data to save to CSV")
        return False
    
    # Get fieldnames from the first record
    fieldnames = list(prepared_data[0].keys()) if prepared_data else []
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write the prepared data
            for record in prepared_data:
                writer.writerow(record)
        
        logging.info(f"Dealer data successfully saved to CSV: {filename}")
        logging.info(f"Saved {len(prepared_data)} records with fields: {', '.join(fieldnames)}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving data to CSV file '{filename}': {str(e)}")
        return False


async def main():
    """
    Main function for testing the dealer scraping functionality.
    """
    import argparse
    from datetime import datetime
        
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape dealers from maschinensucher.de')
    parser.add_argument('--pages', type=int, default=1, 
                       help='Number of pages to scrape (default: 1)')
    parser.add_argument('--force-schema', action='store_true', default=False,
                       help='Force regeneration of dealer schema')
    
    args = parser.parse_args()
    
    try:
        # Run the actual dealer scraping
        logging.info(f"Starting dealer scraping for {args.pages} page(s)...")
        dealers = await grab_dealer_list(num_pages=args.pages)
        
        # Save results to files if dealers were found
        if dealers:
            # Create output directory if it doesn't exist
            output_dir = "maschinensucher/output"
            os.makedirs(output_dir, exist_ok=True)
            
            # Save JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_json_file = os.path.join(output_dir, f"dealer_results_{timestamp}.json")
            with open(output_json_file, "w", encoding="utf-8") as file:
                json.dump(dealers, file, indent=4, ensure_ascii=False)
            logging.info(f"Results saved to JSON: {output_json_file}")
            
            # Save CSV
            csv_filename = os.path.join(output_dir, f"dealer_results_{timestamp}.csv")
            
            # Use the new refactored functions
            prepared_data = prepare_csv(dealers)
            success = save_to_csv(prepared_data, csv_filename)
            
            if not success:
                logging.error(f"Failed to save CSV file: {csv_filename}")
            else:
                logging.info(f"Successfully saved CSV file: {csv_filename}")
        
        print(f"\n=== SCRAPING RESULTS ===")
        print(f"Total dealers found: {len(dealers)}")
        if dealers:
            print(f"Results saved to:")
            print(f"  - JSON: {output_json_file}")
            print(f"  - CSV:  {csv_filename}")
        else:
            print("No dealers found to save.")
        
        logging.info("Dealer scraping completed successfully!")
            
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())