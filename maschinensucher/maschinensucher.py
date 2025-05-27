import argparse
import asyncio
import csv
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    CrawlResult,
    JsonCssExtractionStrategy,
    LLMConfig,
)


def load_schemas_from_json(
    schema_file_path: str = "maschinensucher/schemas/dealer_schema.json",
) -> List[Dict[str, Any]]:
    """
    Load extraction schemas from JSON file.
    This function is now primarily used internally by build_schema_dealer.

    Args:
        schema_file_path (str): Path to the schema JSON file

    Returns:
        List[Dict[str, Any]]: List of schema dictionaries

    Raises:
        FileNotFoundError: If the schema file doesn't exist
        json.JSONDecodeError: If the schema file contains invalid JSON
    """
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema file not found: {schema_file_path}")

    try:
        with open(schema_file_path, "r", encoding="utf-8") as f:
            schemas = json.load(f)

        if not isinstance(schemas, list):
            raise ValueError("Schema file must contain a list of schema objects")

        logging.debug(f"Loaded {len(schemas)} schemas from {schema_file_path}")
        return schemas

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in schema file {schema_file_path}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error loading schemas from {schema_file_path}: {str(e)}")
        raise


async def grab_dealer_list(
    num_pages: int = 1, category_code: str = "2"
) -> List[Dict[str, Any]]:
    """
    Grab dealer list from maschinensucher.de with pagination support.

    Args:
        num_pages (int): Number of pages to crawl, defaults to 1
        category_code (str): Category code for the dealer search, defaults to "2"

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
                delay_before_return_html=1.0,
                wait_for="css:body",  # Wait for page to load
            )

            for page in range(1, num_pages + 1):
                url = f"https://www.maschinensucher.de/Haendler/tci-{category_code}?page={page}&sort=kilometer"
                logging.info(f"Crawling page {page}: {url}")

                result = await crawler.arun(
                    url,
                    config=crawl_config,
                )  # type: ignore

                if result.success:  # type: ignore
                    # Get the latest schemas using build_schema_dealer
                    schemas = await build_schema_dealer(
                        cleaned_html=result.html, force=False
                    )  # type: ignore

                    # Track category text and extracted dealer data for this page
                    category_text = ""
                    page_dealers_data = []
                    page_dealers_count = 0

                    # First pass: Extract all data from schemas
                    extracted_data_by_schema = {}
                    for schema in schemas:
                        schema_name = schema.get("name", "Unknown Schema")
                        logging.debug(f"Processing schema: {schema_name}")

                        extracted_data = JsonCssExtractionStrategy(
                            schema=schema,
                        ).run(url=url, sections=[result.html])  # type: ignore

                        extracted_data_by_schema[schema_name] = extracted_data
                        logging.debug(f"Extracted data from {schema_name} schema")

                    # Second pass: Process extracted data in the correct order
                    # First, extract category text
                    if "Category Text" in extracted_data_by_schema:
                        category_data = extracted_data_by_schema["Category Text"]
                        if isinstance(category_data, list) and len(category_data) > 0:
                            if (
                                isinstance(category_data[0], dict)
                                and "text" in category_data[0]
                            ):
                                category_text = category_data[0]["text"]
                        logging.debug(f"Extracted category text: {category_text}")

                    # Then, process dealer data with the correct category text
                    if "Maschinensucher Dealer Card" in extracted_data_by_schema:
                        dealer_data = extracted_data_by_schema[
                            "Maschinensucher Dealer Card"
                        ]
                        if isinstance(dealer_data, list):
                            for dealer in dealer_data:
                                if isinstance(dealer, dict):
                                    dealer["page_number"] = page
                                    dealer["source_url"] = url
                                    dealer["category"] = category_text
                                    dealer["category_id"] = category_code

                                    # Enrich with parsed address data
                                    dealer = enrich_dealer_with_address_data(dealer)

                            all_dealers.extend(dealer_data)
                            page_dealers_count += len(dealer_data)
                            logging.debug(
                                f"Extracted {len(dealer_data)} dealers with category: '{category_text}'"
                            )

                    # Handle other schema types
                    for schema_name, extracted_data in extracted_data_by_schema.items():
                        if schema_name not in [
                            "Category Text",
                            "Maschinensucher Dealer Card",
                        ]:
                            logging.debug(
                                f"Processing {schema_name} schema - no specific handler implemented yet"
                            )
                            if isinstance(extracted_data, list):
                                logging.debug(
                                    f"Extracted {len(extracted_data)} items from {schema_name} schema"
                                )

                    logging.info(
                        f"Extracted {page_dealers_count} dealers from page {page}"
                    )
                else:
                    logging.error(
                        f"Failed to crawl page {page}: {result.error_message}"
                    )  # type: ignore

            logging.info(f"Total dealers extracted: {len(all_dealers)}")

        except Exception as e:
            logging.error(f"Error during crawling: {str(e)}")
            raise

    return all_dealers


async def grab_dealer_machines(
    dealer_id: str, category_code: str, num_pages: int = 1
) -> List[Dict[str, Any]]:
    """
        Grab machine list for a specific dealer from maschinensucher.de with pagination support.
        example output:
        {
            "category_name": "Holzbearbeitungsmaschinen",
            "count": "5",
            "sub_categories": [
                {
                "category_name": "Zimmereimaschinen",
                "count": "5"
                }
            ],
            "source_url": "https://www.maschinensucher.de/main/search/index?customer-number=46184&main-category-ids[]=3&page=1",
            "dealer_id": "46184",
            "category_id_filter": "3"
        }
        Args:
            dealer_id (str): The dealer's customer number.
            category_code (str): Category code for the machine search.
            num_pages (int): Number of pages to crawl, defaults to 1.

        Returns:
            List[Dict[str, Any]]: List of machine dictionaries containing extracted data.
    """
    logging.info(
        f"Searching for machines for dealer {dealer_id} in category {category_code} across {num_pages} pages..."
    )
    browser_config = BrowserConfig(headless=True, verbose=True)

    all_machines: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                keep_attrs=["id", "class"],
                keep_data_attributes=True,
                delay_before_return_html=1.0,
                wait_for="css:body",  # Wait for page to load
            )

            for page in range(1, num_pages + 1):
                url = f"https://www.maschinensucher.de/main/search/index?customer-number={dealer_id}&main-category-ids[]={category_code}&page={page}"

                result: CrawlResult = await crawler.arun(
                    url,
                    config=crawl_config,
                )  # type: ignore

                if result.success:  # type: ignore
                    schemas = await build_schema_machines(cleaned_html=result.html)  # type: ignore

                    page_machines_count = 0

                    for schema in schemas:
                        schema_name = schema.get("name", "Unknown Schema")
                        logging.debug(f"Processing schema: {schema_name}")

                        # Ensure result.html is not None before passing to extraction
                        current_html_content = (
                            result.html if result.html is not None else ""
                        )
                        if not current_html_content:
                            logging.warning(
                                f"HTML content is empty for URL: {url}. Skipping schema extraction for {schema_name}."
                            )
                            continue

                        extracted_data = JsonCssExtractionStrategy(
                            schema=schema,
                        ).run(url=url, sections=[current_html_content])  # type: ignore

                        logging.debug(
                            f"Extracted data from {schema_name} schema: {json.dumps(extracted_data, indent=2, ensure_ascii=False)}"
                        )

                        if isinstance(extracted_data, list):
                            for item in extracted_data:
                                if isinstance(item, dict):
                                    item["source_url"] = url
                                    item["dealer_id"] = dealer_id
                                    item["category_id_filter"] = category_code
                            all_machines.extend(
                                [d for d in extracted_data if isinstance(d, dict)]
                            )
                            page_machines_count += len(extracted_data)
                        elif isinstance(extracted_data, dict):
                            extracted_data["source_url"] = url
                            extracted_data["dealer_id"] = dealer_id
                            extracted_data["category_id_filter"] = category_code
                            all_machines.append(extracted_data)
                            page_machines_count += 1

                else:
                    logging.error(
                        f"Failed to crawl page {page} for dealer {dealer_id}: {result.error_message}"
                    )  # type: ignore
        except Exception as e:
            logging.error(
                f"Error during machine crawling for dealer {dealer_id}: {str(e)}"
            )
            raise

    return all_machines


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
    cleaned = re.sub(r"</?span[^>]*>", "", raw_html)

    # Remove leading/trailing whitespace and normalize internal whitespace
    cleaned = re.sub(r"\s+", " ", cleaned.strip())

    # Normalize <br> tags (handle various formats like <br>, <br/>, <br />)
    cleaned = re.sub(r"<br\s*/?>", "<br>", cleaned)

    # logging.debug(f"Cleaned HTML address: {cleaned}")
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
            "street": "",
            "postal_code": "",
            "city": "",
            "country": "",
            "state": "",
            "full_address": "",
        }

    # Clean the HTML first
    cleaned_html = clean_html_address(html_address)

    # Split by <br> tags and clean each part
    parts = [part.strip() for part in cleaned_html.split("<br>") if part.strip()]

    result = {
        "street": "",
        "postal_code": "",
        "city": "",
        "country": "",
        "state": "",
        "full_address": ", ".join(parts),
    }

    # Parse street address (first part)
    if len(parts) >= 1:
        result["street"] = parts[0]
        logging.debug(f"Extracted street: {result['street']}")

    # Parse postal code and city (second part)
    if len(parts) >= 2:
        postal_city_match = re.match(r"^(\d{4,5})\s+(.+)$", parts[1])
        if postal_city_match:
            result["postal_code"] = postal_city_match.group(1)
            result["city"] = postal_city_match.group(2)
            logging.debug(
                f"Extracted postal code: {result['postal_code']}, city: {result['city']}"
            )
        else:
            result["city"] = parts[1]
            logging.debug(f"Extracted city (no postal code detected): {result['city']}")

    # Parse country (third part)
    if len(parts) >= 3:
        result["country"] = parts[2]
        logging.debug(f"Extracted country: {result['country']}")

    # Parse state (fourth part)
    if len(parts) >= 4:
        result["state"] = parts[3]
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
    raw_html_address = dealer.get("address_raw_html", "")

    if not raw_html_address:
        logging.warning(
            f"No address_raw_html field found for dealer: {dealer.get('company_name', 'Unknown')}"
        )
        return dealer

    # Parse address components
    address_components = parse_german_address_components(raw_html_address)

    # Add parsed components to dealer data
    dealer["address_raw"] = raw_html_address
    dealer["address_cleaned"] = clean_html_address(raw_html_address)
    dealer.update(address_components)

    logging.debug(
        f"Successfully enriched address data for: {dealer.get('company_name', 'Unknown')}"
    )
    return dealer


async def build_schema_dealer(
    cleaned_html: str, force: bool = False
) -> List[Dict[str, Any]]:
    """
    Build or load extraction schemas for dealer data.
    Always returns the latest schemas - loads existing or generates new ones.
    Supports multiple schema generators and robustly saves all generated schemas.

    Args:
        cleaned_html (str): HTML content for schema generation (only used if force=True or no schema exists)
        force (bool): Force regeneration of dealer schema

    Returns:
        List[Dict[str, Any]]: List of schema dictionaries
    """
    schema_file_path = "maschinensucher/schemas/dealer_schema.json"

    # Try to load existing schemas first (unless force=True)
    if os.path.exists(schema_file_path) and not force:
        try:
            existing_schemas = load_schemas_from_json(schema_file_path)
            logging.info(
                f"Successfully loaded {len(existing_schemas)} existing schemas"
            )
            return existing_schemas
        except Exception as e:
            logging.warning(
                f"Failed to load existing schemas: {str(e)}. Will regenerate..."
            )
            force = True  # Force regeneration if loading fails

    # Generate new schemas if no existing schemas or force=True
    if not cleaned_html and force:
        logging.warning(
            "No HTML provided for schema generation, but force=True. Cannot generate new schemas."
        )
        if os.path.exists(schema_file_path):
            logging.info("Falling back to existing schema file...")
            return load_schemas_from_json(schema_file_path)
        else:
            raise ValueError(
                "Cannot generate schemas without HTML content and no existing schema file found"
            )

    if not cleaned_html:
        logging.warning(
            "No HTML provided and no existing schema file found. Cannot proceed."
        )
        raise ValueError(
            "HTML content required for schema generation when no existing schema file exists"
        )

    logging.info("Building dealer schemas...")
    generated_schemas = []

    try:
        # Schema Generator 1: Dealer Card Schema
        logging.info("Generating Maschinensucher Dealer Card schema...")
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
                "link": "...",
                "attributes": "...",
            }""",
            query="""The given html is the crawled html from Maschinensucher website search result. Please find the schema for dealer information in the given html. Name the schema as "Maschinensucher Dealer Card". I am interested in the dealer company name, the address(save this as html because i need to preserve the <br> format), the distance from the detected location, and the maschinensucher page link for the company.
            """,
        )

        # Handle single schema or list of schemas from generator
        if isinstance(dealer_schema, dict):
            generated_schemas.append(dealer_schema)
        elif isinstance(dealer_schema, list):
            generated_schemas.extend(dealer_schema)

        # Schema Generator 2: Category Text Schema
        logging.info("Generating Category Text schema...")
        category_schema = JsonCssExtractionStrategy.generate_schema(
            html=cleaned_html,
            llm_config=LLMConfig(
                provider="gemini/gemini-2.5-pro-preview-05-06",
                api_token=os.getenv("GEMINI_API_KEY", ""),
            ),
            target_json_example="""{
                "text": "..."
            }""",
            query="""The given html is the crawled html from Maschinensucher website search result. Please find the schema for extracting the category/page title text. Name the schema as "Category Text". I am interested in extracting the main category title/heading text from the page.
            """,
        )

        # Handle category schema
        if isinstance(category_schema, dict):
            generated_schemas.append(category_schema)
        elif isinstance(category_schema, list):
            generated_schemas.extend(category_schema)

        # Future schema generators can be added here
        # Schema Generator 3: Additional schemas...

        if not generated_schemas:
            raise ValueError("No schemas were generated successfully")

        # Robustly save all generated schemas to JSON file
        os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
        with open(schema_file_path, "w", encoding="utf-8") as f:
            json.dump(generated_schemas, f, indent=4, ensure_ascii=False)

        logging.info(
            f"Successfully generated and saved {len(generated_schemas)} schemas to {schema_file_path}"
        )
        return generated_schemas

    except Exception as e:
        logging.error(f"Error generating schemas: {str(e)}")
        # Try to fall back to existing schemas if generation fails
        if os.path.exists(schema_file_path) and not force:
            logging.info(
                "Schema generation failed, falling back to existing schemas..."
            )
            return load_schemas_from_json(schema_file_path)
        raise


async def build_schema_machines(
    cleaned_html: str, force: bool = False
) -> List[Dict[str, Any]]:
    """
    Build or load extraction schemas for dealer's machines data.
    Always returns the latest schemas - loads existing or generates new ones.
    Supports multiple schema generators and robustly saves all generated schemas.

    Args:
        cleaned_html (str): HTML content for schema generation (only used if force=True or no schema exists)
        force (bool): Force regeneration of machine schema

    Returns:
        List[Dict[str, Any]]: List of machine schema dictionaries
    """
    schema_file_path = "maschinensucher/schemas/machines_schema.json"

    # Try to load existing schemas first (unless force=True)
    if os.path.exists(schema_file_path) and not force:
        try:
            existing_schemas = load_schemas_from_json(schema_file_path)
            logging.info(
                f"Successfully loaded {len(existing_schemas)} existing machine schemas"
            )
            return existing_schemas
        except Exception as e:
            logging.warning(
                f"Failed to load existing machine schemas: {str(e)}. Will regenerate..."
            )
            force = True  # Force regeneration if loading fails

    # Generate new schemas if no existing schemas or force=True
    if not cleaned_html and force:
        logging.warning(
            "No HTML provided for schema generation, but force=True. Cannot generate new machine schemas."
        )
        if os.path.exists(schema_file_path):
            logging.info("Falling back to existing machine schema file...")
            return load_schemas_from_json(schema_file_path)
        else:
            raise ValueError(
                "Cannot generate machine schemas without HTML content and no existing schema file found"
            )

    if not cleaned_html:
        logging.warning(
            "No HTML provided and no existing machine schema file found. Cannot proceed."
        )
        raise ValueError(
            "HTML content required for machine schema generation when no existing schema file exists"
        )

    logging.info("Building machine schemas...")
    generated_schemas = []

    try:
        # Schema Generator 1: Machine Category Schema
        logging.info("Generating Machine Category Filter schema...")
        machine_category_schema = JsonCssExtractionStrategy.generate_schema(
            html=cleaned_html,
            llm_config=LLMConfig(
                provider="gemini/gemini-2.5-pro-preview-05-06",
                api_token=os.getenv("GEMINI_API_KEY", ""),
            ),
            target_json_example="""[
                {
                    "category_name": "...maschinen",
                    "count": 5,
                    "sub_categories": [
                        {
                            "category_name": "...maschinen",
                            "count": 5
                        }
                    ]
                }
            ]""",
            query="""The provided HTML snippet is from a maschinensucher website and displays category filters. Please generate a schema named 'MachineCategoryFilter' to extract the category and subcategory information. For each main category, I need its name, the count of items (the number in parentheses). Each main category might have a list of subcategories. For each subcategory, I also need its name, and count of items.
            """,
        )

        # Handle single schema or list of schemas from generator
        if isinstance(machine_category_schema, dict):
            generated_schemas.append(machine_category_schema)
        elif isinstance(machine_category_schema, list):
            generated_schemas.extend(machine_category_schema)

        # Future schema generators can be added here
        # Schema Generator 2: Machine Details Schema...
        # Schema Generator 3: Additional schemas...

        if not generated_schemas:
            raise ValueError("No machine schemas were generated successfully")

        # Robustly save all generated schemas to JSON file
        os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
        with open(schema_file_path, "w", encoding="utf-8") as f:
            json.dump(generated_schemas, f, indent=4, ensure_ascii=False)

        logging.info(
            f"Successfully generated and saved {len(generated_schemas)} machine schemas to {schema_file_path}"
        )
        return generated_schemas

    except Exception as e:
        logging.error(f"Error generating machine schemas: {str(e)}")
        # Try to fall back to existing schemas if generation fails
        if os.path.exists(schema_file_path) and not force:
            logging.info(
                "Machine schema generation failed, falling back to existing schemas..."
            )
            return load_schemas_from_json(schema_file_path)
        raise


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
        "vertrauensiegel",
        "company_name",
        "street",
        "postal_code",
        "city",
        "state",
        "country",
        "distance",
        "link",
        "category",
        "category_id",
        "page_number",
        "source_url",
    ]

    prepared_dealers = []

    try:
        # Clean and prepare each dealer entry
        for i, dealer in enumerate(dealers):
            clean_dealer = {}

            # Handle vertrauensiegel field - check if attributes contains "Vertrauenssiegel"
            attributes = dealer.get("attributes", "")
            clean_dealer["vertrauensiegel"] = (
                "vertrauensiegel" in str(attributes).lower() if attributes else False
            )
            has_vertrauensiegel = (
                "vertrauensiegel" in str(attributes).lower() if attributes else False
            )

            # Print the first 5 entries that have vertrauenssiegel
            if (
                has_vertrauensiegel
                and sum(1 for d in prepared_dealers if d.get("vertrauensiegel")) < 5
            ):
                logging.info(
                    f"Dealer with Vertrauenssiegel: {dealer.get('company_name', 'Unknown')} | attributes='{attributes}'"
                )

            # Handle other fields
            for field in clean_fieldnames:
                if field != "vertrauensiegel":
                    clean_dealer[field] = dealer.get(field, "")

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
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write the prepared data
            for record in prepared_data:
                writer.writerow(record)

        logging.info(f"Dealer data successfully saved to CSV: {filename}")
        logging.info(
            f"Saved {len(prepared_data)} records with fields: {', '.join(fieldnames)}"
        )
        return True

    except Exception as e:
        logging.error(f"Error saving data to CSV file '{filename}': {str(e)}")
        return False


async def main():
    """
    Main function for testing the dealer scraping functionality.
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Scrape dealers from maschinensucher.de"
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to scrape (default: 1)"
    )
    parser.add_argument(
        "--force-schema",
        action="store_true",
        default=False,
        help="Force regeneration of dealer schema",
    )

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
            output_json_file = os.path.join(
                output_dir, f"dealer_results_{timestamp}.json"
            )
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

        print("\n=== SCRAPING RESULTS ===")
        print(f"Total dealers found: {len(dealers)}")
        if dealers:
            print("Results saved to:")
            print(f"  - JSON: {output_json_file}")
            print(f"  - CSV:  {csv_filename}")
        else:
            print("No dealers found to save.")

        logging.info("Dealer scraping completed successfully!")

    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
