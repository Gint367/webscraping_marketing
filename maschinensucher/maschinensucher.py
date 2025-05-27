import argparse
from ast import parse
import asyncio
import csv
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerMonitor,
    CrawlerRunConfig,
    CrawlResult,
    DisplayMode,
    JsonCssExtractionStrategy,
    LLMConfig,
)

# Try to import MemoryAdaptiveDispatcher (may not be available in all versions)
try:
    from crawl4ai import MemoryAdaptiveDispatcher
    HAS_MEMORY_DISPATCHER = True
except ImportError:
    HAS_MEMORY_DISPATCHER = False


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


async def grab_dealer_machines_parallel(
    dealer_id: str, category_code: str, num_pages: int = 1
) -> List[Dict[str, Any]]:
    """
    Parallel version of grab_dealer_machines using crawl4ai's arun_many() method.
    
    Grab machine list for a specific dealer from maschinensucher.de with parallel pagination support.
    This function crawls all pages for a dealer simultaneously instead of sequentially.
    
    Args:
        dealer_id (str): The dealer's customer number.
        category_code (str): Category code for the machine search.
        num_pages (int): Number of pages to crawl, defaults to 1.

    Returns:
        List[Dict[str, Any]]: List of machine dictionaries containing extracted data.
    """
    logging.info(
        f"Parallel searching for machines for dealer {dealer_id} in category {category_code} across {num_pages} pages..."
    )
    
    # Early return for single page to avoid overhead
    if num_pages == 1:
        return await grab_dealer_machines(dealer_id, category_code, num_pages)
    
    browser_config = BrowserConfig(headless=True, verbose=True)
    all_machines: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            # Generate all URLs for parallel crawling
            urls = [
                f"https://www.maschinensucher.de/main/search/index?customer-number={dealer_id}&main-category-ids[]={category_code}&page={page}"
                for page in range(1, num_pages + 1)
            ]
            
            logging.debug(f"Generated {len(urls)} URLs for parallel crawling")
            
            # Configure crawler with conservative rate limiting
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                keep_attrs=["id", "class"],
                keep_data_attributes=True,
                delay_before_return_html=1.0,
                wait_for="css:body",  # Wait for page to load
            )
            
            # Use MemoryAdaptiveDispatcher for intelligent concurrency control
            try:
                from crawl4ai import MemoryAdaptiveDispatcher
                dispatcher = MemoryAdaptiveDispatcher(
                    memory_threshold_percent=75.0,  # Conservative memory usage
                    check_interval=1,
                    monitor=CrawlerMonitor(
                        display_mode=DisplayMode.AGGREGATED,
                        max_visible_rows=10,  # Limit visible rows for performance
                    )
                )
                logging.debug("Using MemoryAdaptiveDispatcher for concurrency control")
            except ImportError:
                logging.warning("MemoryAdaptiveDispatcher not available, using default dispatcher")
                dispatcher = None
            
            # Crawl all pages in parallel
            start_time = time.time()
            if dispatcher:
                results = await crawler.arun_many(
                    urls=urls,
                    config=crawl_config,
                    dispatcher=dispatcher
                )
            else:
                results = await crawler.arun_many(
                    urls=urls,
                    config=crawl_config
                )
            
            crawl_duration = time.time() - start_time
            logging.info(f"Parallel crawling completed in {crawl_duration:.2f} seconds")
            
            # Pre-generate schemas once for efficiency
            schemas = None
            successful_results = [r for r in results if r.success]  # type: ignore
            
            if successful_results:
                # Use first successful result to generate schemas
                first_html = successful_results[0].html  # type: ignore
                if first_html:
                    schemas = await build_schema_machines(cleaned_html=first_html)
                    logging.debug(f"Generated {len(schemas) if schemas else 0} schemas for extraction")
            
            # Process all results
            total_pages_processed = 0
            failed_pages = 0
            
            for i, result in enumerate(results):
                page_number = i + 1
                url = urls[i]
                
                if result.success:  # type: ignore
                    total_pages_processed += 1
                    page_machines_count = 0
                    
                    # Use pre-generated schemas or generate if needed
                    current_schemas = schemas
                    if not current_schemas and result.html:  # type: ignore
                        current_schemas = await build_schema_machines(cleaned_html=result.html)  # type: ignore
                    
                    if not current_schemas:
                        logging.warning(f"No schemas available for page {page_number}, skipping extraction")
                        continue
                    
                    for schema in current_schemas:
                        schema_name = schema.get("name", "Unknown Schema")
                        logging.debug(f"Processing schema: {schema_name} for page {page_number}")

                        # Ensure result.html is not None before passing to extraction
                        current_html_content = (
                            result.html if result.html is not None else ""  # type: ignore
                        )
                        if not current_html_content:
                            logging.warning(
                                f"HTML content is empty for URL: {url}. Skipping schema extraction for {schema_name}."
                            )
                            continue

                        try:
                            extracted_data = JsonCssExtractionStrategy(
                                schema=schema,
                            ).run(url=url, sections=[current_html_content])  # type: ignore

                            logging.debug(
                                f"Extracted data from {schema_name} schema (page {page_number}): {json.dumps(extracted_data, indent=2, ensure_ascii=False) if extracted_data else 'No data'}"
                            )

                            if isinstance(extracted_data, list):
                                for item in extracted_data:
                                    if isinstance(item, dict):
                                        item["source_url"] = url
                                        item["dealer_id"] = dealer_id
                                        item["category_id_filter"] = category_code
                                        item["page_number"] = page_number  # Add page tracking
                                all_machines.extend(
                                    [d for d in extracted_data if isinstance(d, dict)]
                                )
                                page_machines_count += len(extracted_data)
                            elif isinstance(extracted_data, dict):
                                extracted_data["source_url"] = url
                                extracted_data["dealer_id"] = dealer_id
                                extracted_data["category_id_filter"] = category_code
                                extracted_data["page_number"] = page_number  # Add page tracking
                                all_machines.append(extracted_data)
                                page_machines_count += 1
                        
                        except Exception as e:
                            logging.error(f"Error extracting data from schema {schema_name} on page {page_number}: {str(e)}")
                            continue
                    
                    logging.info(f"Extracted {page_machines_count} machines from page {page_number}")
                
                else:
                    failed_pages += 1
                    logging.error(
                        f"Failed to crawl page {page_number} for dealer {dealer_id}: {result.error_message}"  # type: ignore
                    )
            
            # Log summary statistics
            logging.info(
                f"Parallel crawling summary for dealer {dealer_id}: "
                f"{total_pages_processed}/{num_pages} pages successful, "
                f"{failed_pages} failed, "
                f"{len(all_machines)} total machines extracted"
            )
            
        except Exception as e:
            logging.error(
                f"Error during parallel machine crawling for dealer {dealer_id}: {str(e)}"
            )
            # Fallback to sequential crawling on error
            logging.warning(f"Falling back to sequential crawling for dealer {dealer_id}")
            return await grab_dealer_machines(dealer_id, category_code, num_pages)

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


def clean_machine_data(machine_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean machine data by removing unnecessary fields.
    
    Args:
        machine_data (Dict[str, Any]): Raw machine data
        
    Returns:
        Dict[str, Any]: Cleaned machine data with only category info
    """
    cleaned = {}
    
    # Keep only the category information
    if "category_name" in machine_data:
        cleaned["category_name"] = machine_data["category_name"]
    
    if "count" in machine_data:
        cleaned["count"] = machine_data["count"]
        
    if "sub_categories" in machine_data:
        # Clean sub-categories as well
        cleaned_sub_categories = []
        for sub_cat in machine_data["sub_categories"]:
            if isinstance(sub_cat, dict):
                cleaned_sub_cat = {}
                if "category_name" in sub_cat:
                    cleaned_sub_cat["category_name"] = sub_cat["category_name"]
                if "count" in sub_cat:
                    cleaned_sub_cat["count"] = sub_cat["count"]
                if cleaned_sub_cat:  # Only add if not empty
                    cleaned_sub_categories.append(cleaned_sub_cat)
        
        if cleaned_sub_categories:
            cleaned["sub_categories"] = cleaned_sub_categories
    
    return cleaned


def prepare_csv(dealers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare and clean dealer data for CSV export by filtering only clean data fields.
    Includes machine data with main category and subcategories as separate columns.

    Args:
        dealers (List[Dict[str, Any]]): List of dealer dictionaries from JSON data

    Returns:
        List[Dict[str, Any]]: List of cleaned dealer dictionaries with only specified fields
    """
    if not dealers:
        logging.warning("No dealer data to prepare for CSV")
        return []

    # First pass: determine the maximum number of subcategories across all dealers
    max_subcategories = 0
    for dealer in dealers:
        machines_data = dealer.get("machines_data", [])
        if machines_data and len(machines_data) > 0:
            for machine_category in machines_data:
                sub_categories = machine_category.get("sub_categories", [])
                max_subcategories = max(max_subcategories, len(sub_categories))

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
        # Machine data fields
        "main_category",
        "main_category_count",
    ]

    # Add dynamic subcategory columns
    for i in range(1, max_subcategories + 1):
        clean_fieldnames.extend([
            f"sub_category_{i}",
            f"sub_category_{i}_count"
        ])

    prepared_dealers = []

    try:
        # Clean and prepare each dealer entry
        for i, dealer in enumerate(dealers):
            clean_dealer = {}

            # Handle vertrauensiegel field - check if attributes contains "Vertrauenssiegel"
            attributes = dealer.get("attributes", "")
            clean_dealer["vertrauensiegel"] = (
                "vertrauenssiegel" in str(attributes).lower() if attributes else False
            )
            has_vertrauensiegel = (
                "vertrauenssiegel" in str(attributes).lower() if attributes else False
            )

            # Print the first 5 entries that have vertrauenssiegel
            if (
                has_vertrauensiegel
                and sum(1 for d in prepared_dealers if d.get("vertrauenssiegel")) < 5
            ):
                logging.info(
                    f"Dealer with Vertrauenssiegel: {dealer.get('company_name', 'Unknown')} | attributes='{attributes}'"
                )

            # Handle other basic fields
            for field in clean_fieldnames:
                if field not in ["vertrauenssiegel", "main_category", "main_category_count"] and not field.startswith("sub_category_"):
                    clean_dealer[field] = dealer.get(field, "")

            # Handle machine data fields
            machines_data = dealer.get("machines_data", [])
            clean_dealer["main_category"] = ""
            clean_dealer["main_category_count"] = ""
            
            # Initialize all subcategory fields
            for j in range(1, max_subcategories + 1):
                clean_dealer[f"sub_category_{j}"] = ""
                clean_dealer[f"sub_category_{j}_count"] = ""

            # Process machine data if available
            if machines_data and len(machines_data) > 0:
                # Take the first main category (assuming one main category per dealer)
                main_machine_data = machines_data[0]
                clean_dealer["main_category"] = main_machine_data.get("category_name", "")
                clean_dealer["main_category_count"] = str(main_machine_data.get("count", ""))

                # Process subcategories
                sub_categories = main_machine_data.get("sub_categories", [])
                for idx, sub_cat in enumerate(sub_categories):
                    if idx < max_subcategories:  # Ensure we don't exceed our column count
                        col_idx = idx + 1
                        clean_dealer[f"sub_category_{col_idx}"] = sub_cat.get("category_name", "")
                        clean_dealer[f"sub_category_{col_idx}_count"] = str(sub_cat.get("count", ""))

            prepared_dealers.append(clean_dealer)

        logging.info(f"Prepared {len(prepared_dealers)} dealer records for CSV export")
        logging.info(f"CSV fields: {', '.join(clean_fieldnames)}")
        logging.info(f"Maximum subcategories found: {max_subcategories}")

    except Exception as e:
        logging.error(f"Error preparing dealers for CSV: {str(e)}")
        raise

    return prepared_dealers


def save_to_csv(prepared_data: List[Dict[str, Any]], filename: str) -> bool:
    """
    Save prepared dealer data to a CSV file with BOM for German language compatibility.

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
        with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write the prepared data
            for record in prepared_data:
                writer.writerow(record)

        logging.info(f"Dealer data successfully saved to CSV with BOM: {filename}")
        logging.info(
            f"Saved {len(prepared_data)} records with fields: {', '.join(fieldnames)}"
        )
        return True

    except Exception as e:
        logging.error(f"Error saving data to CSV file '{filename}': {str(e)}")
        return False


def extract_dealer_id_from_link(link: str) -> str:
    """
    Extract dealer ID from dealer link.
    
    Args:
        link (str): Dealer link in format /Haendler/47038/imexx-systemtechnik-ronnenberg
        
    Returns:
        str: Dealer ID (e.g., "47038")
    """
    try:
        # Split the link by "/" and get the dealer ID part
        parts = link.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "Haendler":
            return parts[1]
        else:
            logging.warning(f"Could not extract dealer ID from link: {link}")
            return ""
    except Exception as e:
        logging.error(f"Error extracting dealer ID from link '{link}': {str(e)}")
        return ""


async def main():
    """
    Main function for scraping dealers and their machine data.
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Scrape dealers and their machines from maschinensucher.de"
    )
    parser.add_argument(
        "--category",
        type=str,
        default="",
        help="Category code to scrape dealers from (default: all categories)",
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
    parser.add_argument(
        "--parallel",
        action="store_true",
        default=False,
        help="Use parallel execution for machine data crawling (faster for multiple pages)",
    )
    parser.add_argument(
        "--machine-pages",
        type=int,
        default=1,
        help="Number of machine pages to crawl per dealer (default: 1)",
    )

    args = parser.parse_args()

    try:
        # Run the actual dealer scraping
        logging.info(f"Starting dealer scraping for {args.pages} page(s)...")
        dealers = await grab_dealer_list(num_pages=args.pages, category_code=args.category)

        if not dealers:
            logging.warning("No dealers found. Exiting.")
            print("\n=== SCRAPING RESULTS ===")
            print("No dealers found.")
            return

        logging.info(f"Found {len(dealers)} dealers. Now scraping machine data for each dealer...")

        # Process each dealer to get their machine data
        enhanced_dealers = []
        for i, dealer in enumerate(dealers):
            dealer_name = dealer.get("company_name", "Unknown")
            dealer_link = dealer.get("link", "")
            dealer_category_id = dealer.get("category_id", "")
            
            logging.info(f"Processing dealer {i+1}/{len(dealers)}: {dealer_name}")
            
            # Extract dealer ID from link
            dealer_id = extract_dealer_id_from_link(dealer_link)
            
            if not dealer_id:
                logging.warning(f"Could not extract dealer ID for {dealer_name}. Skipping machine data collection.")
                # Add dealer without machine data
                enhanced_dealer = dealer.copy()
                enhanced_dealer["machines_data"] = []
                enhanced_dealers.append(enhanced_dealer)
                continue
            
            if not dealer_category_id:
                logging.warning(f"No category ID found for {dealer_name}. Skipping machine data collection.")
                # Add dealer without machine data
                enhanced_dealer = dealer.copy()
                enhanced_dealer["machines_data"] = []
                enhanced_dealers.append(enhanced_dealer)
                continue
            
            try:
                # Get machine data for this dealer
                logging.debug(f"Grabbing machines for dealer {dealer_id} in category {dealer_category_id}")
                
                # Choose between parallel and sequential crawling
                if args.parallel and args.machine_pages > 1:
                    logging.debug(f"Using parallel crawling for {args.machine_pages} pages")
                    machines = await grab_dealer_machines_parallel(
                        dealer_id=dealer_id,
                        category_code=dealer_category_id,
                        num_pages=args.machine_pages
                    )
                else:
                    logging.debug(f"Using sequential crawling for {args.machine_pages} page(s)")
                    machines = await grab_dealer_machines(
                        dealer_id=dealer_id,
                        category_code=dealer_category_id,
                        num_pages=args.machine_pages
                    )
                
                # Clean machine data to keep only category info
                cleaned_machines = []
                for machine in machines:
                    if isinstance(machine, dict):
                        cleaned_machine = clean_machine_data(machine)
                        if cleaned_machine:  # Only add if not empty
                            cleaned_machines.append(cleaned_machine)
                
                # Add machine data to dealer
                enhanced_dealer = dealer.copy()
                enhanced_dealer["machines_data"] = cleaned_machines
                enhanced_dealers.append(enhanced_dealer)
                
                logging.info(f"Found {len(cleaned_machines)} machine categories for {dealer_name}")
                
            except Exception as e:
                logging.error(f"Error getting machine data for dealer {dealer_name} (ID: {dealer_id}): {str(e)}")
                # Add dealer without machine data in case of error
                enhanced_dealer = dealer.copy()
                enhanced_dealer["machines_data"] = []
                enhanced_dealers.append(enhanced_dealer)

        # Save results to files
        if enhanced_dealers:
            # Create output directory if it doesn't exist
            output_dir = "maschinensucher/output"
            os.makedirs(output_dir, exist_ok=True)

            # Save JSON with machine data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_json_file = os.path.join(
                output_dir, f"dealers_with_machines_{timestamp}.json"
            )
            with open(output_json_file, "w", encoding="utf-8") as file:
                json.dump(enhanced_dealers, file, indent=4, ensure_ascii=False)
            logging.info(f"Enhanced results saved to JSON: {output_json_file}")

            # Save CSV (original dealer data only for CSV compatibility)
            csv_filename = os.path.join(output_dir, f"dealer_results_{timestamp}.csv")
            prepared_data = prepare_csv(enhanced_dealers)  # Use enhanced dealers for CSV
            success = save_to_csv(prepared_data, csv_filename)

            if not success:
                logging.error(f"Failed to save CSV file: {csv_filename}")
            else:
                logging.info(f"Successfully saved CSV file: {csv_filename}")

        print("\n=== SCRAPING RESULTS ===")
        print(f"Total dealers processed: {len(enhanced_dealers)}")
        
        # Summary of machine data
        total_machine_categories = sum(len(dealer.get("machines_data", [])) for dealer in enhanced_dealers)
        dealers_with_machines = sum(1 for dealer in enhanced_dealers if dealer.get("machines_data", []))
        
        print(f"Dealers with machine data: {dealers_with_machines}/{len(enhanced_dealers)}")
        print(f"Total machine categories found: {total_machine_categories}")
        
        if enhanced_dealers:
            print("Results saved to:")
            print(f"  - JSON with machine data: {output_json_file}")
            print(f"  - CSV (dealers only):     {csv_filename}")
        else:
            print("No dealers found to save.")

        logging.info("Dealer and machine scraping completed successfully!")

    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Test prepare_csv function with actual data
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test_csv":
        print("Testing prepare_csv function with actual data...")
        # Load the test data
        test_file = "maschinensucher/output/dealers_with_machines_20250527_150155.json"
        try:
            with open(test_file, "r", encoding="utf-8") as f:
                test_dealers = json.load(f)
            
            print(f"Loaded {len(test_dealers)} dealers from {test_file}")
            
            # Test prepare_csv function
            prepared_data = prepare_csv(test_dealers)
            print(f"Prepared {len(prepared_data)} records for CSV")
            
            # Show first record fields
            if prepared_data:
                print(f"CSV fields ({len(prepared_data[0].keys())} total):")
                print(list(prepared_data[0].keys()))
                
                # Show sample values for machine data fields
                first_record = prepared_data[0]
                print("\nSample machine data fields:")
                for key in first_record:
                    if key.startswith(("main_category", "sub_category")):
                        print(f"  {key}: {first_record[key]}")
            
            # Save to CSV file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_test_file = f"maschinensucher/output/test_csv_output_{timestamp}.csv"
            success = save_to_csv(prepared_data, csv_test_file)
            
            if success:
                print(f"\nCSV successfully saved to: {csv_test_file}")
                
                # Show CSV file stats
                import os
                file_size = os.path.getsize(csv_test_file)
                print(f"File size: {file_size:,} bytes")
                
                # Read first few lines to verify
                with open(csv_test_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()[:3]
                    print(f"First 3 lines of CSV:")
                    for i, line in enumerate(lines):
                        print(f"  Line {i+1}: {line.strip()[:100]}{'...' if len(line.strip()) > 100 else ''}")
            else:
                print("Failed to save CSV file")
                
        except Exception as e:
            print(f"Error testing prepare_csv: {str(e)}")
            import traceback
            traceback.print_exc()
    else:
        # Normal execution
        asyncio.run(main())
