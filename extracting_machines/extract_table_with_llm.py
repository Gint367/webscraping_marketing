import os
import json
import asyncio
from datetime import datetime
from typing import List
from pydantic import BaseModel, Field
import argparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy


class Company(BaseModel):
    umsatz: float = Field(..., description="Umsatz in Euro")
    table_name: str = Field(..., description="Name der Tabelle")
    
prompt = """
    Das folgende HTML enthält Finanzdaten aus dem Jahresabschluss eines deutschen Unternehmens. 
    Bitte identifizieren Sie die wahrscheinlichste Position für den Gesamtumsatz des Unternehmens im aktuellen Geschäftsjahr. 
    Achten Sie dabei auf den Kontext der übrigen Finanzinformationen. 
    Extrahieren Sie den zugehörigen numerischen Wert als Fließkommazahl in EUR. 
    Sollte kein Umsatz identifiziert werden können, geben Sie '0' zurück.
    Strikte Einhaltung der Datenwahrheit: Keine Halluzinationen oder Ergänzungen durch eigene Annahmen.
    """


def ensure_output_directory(base_dir="financial_data"):
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    return base_dir


dispatcher = MemoryAdaptiveDispatcher(
    memory_threshold_percent=70.0,
    check_interval=2.0,
    max_session_permit=5,
)

rate_limiter = RateLimiter(
    base_delay=(30, 60),
    max_delay=60,
    max_retries=3,
    rate_limit_codes=[429, 503]
)


async def process_html_files(file_paths, llm_strategy, output_dir):
    """
    Process multiple HTML files using LLM extraction and save all results in a single folder.
    
    Args:
        file_paths (list of str): List of HTML file paths to be processed
        llm_strategy (LLMExtractionStrategy): The LLM strategy to use for extraction
        output_dir (str): Directory where results will be saved
    """
    # Convert file paths to URLs with file:// protocol
    file_urls = [f"file://{os.path.abspath(path)}" for path in file_paths]
    
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=llm_strategy,
    )
    
    async with AsyncWebCrawler() as crawler:
        results = await crawler.arun_many(
            urls=file_urls,  
            config=config,
            dispatcher=dispatcher,
            rate_limiter=rate_limiter,
        )
        
        # Create a timestamp for the batch process
        batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Dictionary to collect all successful extractions
        all_company_data = {}
        
        for idx, result in enumerate(results):
            file_path = file_paths[idx]
            basename = os.path.basename(file_path)
            # Remove _cleaned suffix and extension to get company name
            company_name = basename.replace('_cleaned', '').split('.')[0]
            
            if result.success and result.extracted_content:
                # Parse the extracted content
                content = result.extracted_content
                if isinstance(content, str):
                    try:
                        # If it's a JSON string, parse it
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        # If not valid JSON, create a simple dict with the content as a message
                        content = {"message": content}
                
                # Add company name to the data
                if isinstance(content, dict):
                    content["company_name"] = company_name
                
                # Store in the combined dictionary
                all_company_data[company_name] = content
                
                print(f"Successfully extracted financial data for {company_name}")
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error')
                print(f"Failed to extract data from {company_name}: {error_msg}")
                
                # Store error information in the combined dictionary
                all_company_data[company_name] = {
                    "company_name": company_name,
                    "error": True,
                    "error_message": error_msg
                }
        
        # Save all data to a single JSON file
        if all_company_data:
            output_file = os.path.join(output_dir, f"all_company_financial_data_{batch_timestamp}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_company_data, f, indent=2, ensure_ascii=False)
            print(f"Combined financial data for all companies saved to {output_file}")
        else:
            print("No data was successfully extracted")


async def main():
    parser = argparse.ArgumentParser(description='Extract financial data from HTML files using LLM')
    parser.add_argument('input', help='Input directory containing HTML files')
    parser.add_argument('--output', '-o', help='Output directory for extracted data', 
                        default='financial_data')
    parser.add_argument('--limit', '-l', type=int, help='Limit number of files to process',
                        default=None)
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    output_dir = ensure_output_directory(args.output)
    
    # Configure LLM strategy
    llm_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o-mini",
        api_token='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',
        extraction_type="schema",
        schema=Company.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=4096,
        overlap_rate=0.1,
        input_format="html",
        apply_chunking=False,
        extra_args={"temperature": 0.5, "max_tokens": 800},
        verbose=True,
    )
    
    # Validate input directory
    if not os.path.isdir(args.input):
        print(f"Error: {args.input} is not a valid directory")
        return
    
    # Find all HTML files
    files_to_process = []
    for file in os.listdir(args.input):
        if file.endswith('.html') and '_cleaned' in file:
            files_to_process.append(os.path.join(args.input, file))
    
    if not files_to_process:
        print(f"No HTML files found in {args.input}")
        return
    
    # Apply limit if specified
    if args.limit is not None and args.limit > 0:
        files_to_process = files_to_process[:args.limit]
    
    print(f"Processing {len(files_to_process)} files...")
    
    # Process files
    await process_html_files(files_to_process, llm_strategy, output_dir)


if __name__ == "__main__":
    asyncio.run(main())

