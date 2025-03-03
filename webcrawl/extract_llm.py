import os
import json
import asyncio
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerMonitor, DisplayMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
import argparse

class Company(BaseModel):
    company_name: str = Field(..., description="Name des Unternehmens.")
    products: List[str] = Field(..., description="Produkte, die das Unternehmen vertreibt.")
    machines: List[str] = Field(..., description="(Optional)Maschinen, die das Unternehmen in der eigenen Fertigung nutzt.")
    process_type: List[str] = Field(..., description="(Optional)Produktionsprozesse, die das Unternehmen in der eigenen Fertigung nutzt.")
    lohnfertigung: bool = Field(..., description="Ob das Unternehmen Lohnfertigung anbietet")
    #isGroup: bool = Field(..., description="Whether the company is a group of companies.")

""" prompt = (
    "From the crawled content, extract the following details:\n"
    "1. Company Name\n"
    "2. Products that the company offers (Products can either be machines or parts produced by machines.)\n"
    "3. Machines that the company uses in their manufacturing process\n"
    "4. Type of Process that their machine uses in their manufacturing process (e.g., Milling, Drilling, Turning, Grinding, etc.)\n"
    "5. Whether the company offers contract manufacturing services (yes or no)\n"
    "6. Whether the company is a company that consists of multiple subsidiary companies or divisions.)\n"
    "Notes: Only write if it's specifically mentioned.\n"
) """
prompt = """
Funktionen:
- Durchsucht angegebene Webseiten und alle Unterseiten nach relevanten Informationen.
- Namen des Unternehmens(legale Bezeichnung, schön Formattiert)
- Identifiziert die drei bedeutendsten Produkte oder Dienstleistungen, die das Unternehmen anbietet(Schreiben im Pluralform).
- Berücksichtigt, ob ein Unternehmen eigene Produkte vertreibt, als Zulieferer tätig ist oder in der Lohnfertigung arbeitet.
- Erkennt den Maschinenpark des Unternehmens, d. h. welche Maschinen für die Herstellung der eigenen Produkte genutzt werden (keine Maschinen, die als eigene Produkte verkauft werden)(Schreiben im Pluralform).
- Gibt Maschinen nur als allgemeine Maschinenkategorie aus, ohne genaue Modell- oder Markennamen. Beispiel: "HIGH SPEED EAGLE V9" wird als "Fräsmaschinen" ausgegeben, "GANTRY EAGLE 1200" als "Erodiermaschinen".
- Analysiert die Produktionsprozesse, die das Unternehmen für die eigene Fertigung nutzt (keine Prozesse, die mit eigenen verkauften Maschinen durchgeführt werden können, sondern nur die tatsächlich genutzten Verfahren).
- Nutzt eine vordefinierte Liste typischer Produktionsprozesse aus verschiedenen Branchen (z. B. Metallbearbeitung, Holzbearbeitung, Kunststoffverarbeitung) zur besseren Identifikation und Zuordnung.
- Produktionsprozesse, die nicht mit der Verarbeitung oder Produktion von Materialien zu tun haben (z. B. "Transport", "Logistik"), werden nicht als relevante Keywords aufgenommen.
- Bietet Ihr Unternehmen Lohnfertigung oder Auftragsfertigung für externe Kunden an?

- Falls weniger als drei Einträge in einer Kategorie gefunden werden, bleiben die entsprechenden Felder leer.
- Strikte Einhaltung der Datenwahrheit: Keine Halluzinationen oder Ergänzungen durch eigene Annahmen.
- strukturierte Ergebnisse mit potenziellen Ansatzpunkten für E-Mail-Texte.

Einschränkungen:
- Ergbnisse nur auf Deutsch.
"""

def ensure_output_directory(directory="llm_extracted_data"):
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=2.0,
            max_session_permit=10,
        )
rate_limiter = RateLimiter(
    base_delay=(30, 60),
    max_delay=60,
    max_retries=3,
    rate_limit_codes=[429, 503]
)
async def process_file(file_path, llm_strategy, output_dir):
    """Process a single markdown file with LLM extraction"""
    # Convert file path to URL with file:// protocol
    file_url = f"file://{os.path.abspath(file_path)}"
    config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        extraction_strategy=llm_strategy,
        
    )
    
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url=file_url, 
            config=config,
            dispatcher=dispatcher,
            rate_limiter=rate_limiter,
        )
        
        if result.success and result.extracted_content:
            # Create output filename based on input filename
            basename = os.path.basename(file_path)
            name_without_ext = os.path.splitext(basename)[0]
            output_file = os.path.join(output_dir, f"{name_without_ext}_extracted.json")
            
            # Save extracted content
            with open(output_file, "w", encoding="utf-8") as f:
                if isinstance(result.extracted_content, str):
                    f.write(result.extracted_content)
                else:
                    json.dump(result.extracted_content, f, indent=2, ensure_ascii=False)
            
            print(f"Extracted data saved to {output_file}")
            llm_strategy.show_usage()
            return result.extracted_content
        else:
            error_msg = getattr(result, 'error_message', 'Unknown error')
            print(f"No content extracted from {file_path}: {error_msg}")
            return None
        
async def process_directory(dir_path, llm_strategy, output_dir, file_extension=".md"):
    """Process all markdown files in a directory with LLM extraction"""
    # Get all markdown files in the directory
    all_files = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            if file.endswith(file_extension):
                all_files.append(os.path.join(root, file))
    
    if not all_files:
        print(f"No {file_extension} files found in {dir_path}")
        return
    
    # Process all files and collect results
    print(f"Processing {len(all_files)} files...")
    
    results = []
    for file_path in all_files[:10]:
        print(f"Processing {file_path}...")
        result = await process_file(file_path, llm_strategy, output_dir)
        if result:
            results.append(result)
    
    # Save combined results
    if results:
        # Show usage stats
        llm_strategy.show_usage()
        
        # Get the current date
        current_date = datetime.now().strftime("%Y%m%d")
        
        # Save all extracted data to a combined JSON file
        combined_file = os.path.join(output_dir, f"combined_extracted_data_{current_date}.json")
        
        # Process the results to ensure they're in the correct format
        processed_results = []
        for result in results:
            # If the result is a JSON string, parse it first
            if isinstance(result, str):
                try:
                    parsed_result = json.loads(result)
                    processed_results.append(parsed_result)
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON string: {result}")
            else:
                processed_results.append(result)
        
        # Include the prompt and parameters at the top of the JSON data
        output_data = {
            "instruction": prompt,
            "parameters": {
                "model": llm_strategy.provider,
                "temperature": llm_strategy.extra_args.get("temperature", 0),
                "max_tokens": llm_strategy.extra_args.get("max_tokens", 0)
            },
            "data": processed_results
        }
        
        with open(combined_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        print(f"Combined data saved to {combined_file}")
    else:
        print("No data was successfully extracted")

async def main():
    parser = argparse.ArgumentParser(description='Extract data from markdown files using LLM')
    parser.add_argument('input', help='Input file or directory path')
    parser.add_argument('--output', '-o', help='Output directory for extracted data', 
                        default='llm_extracted_data')
    parser.add_argument('--ext', '-e', help='File extension to process (default: .md)',
                        default='.md')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    output_dir = ensure_output_directory(args.output)
    
    # Define LLM strategy once (shared between file and directory processing)
    temperature = 0.5
    max_tokens = 10000
    llm_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o-mini",
        api_token='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',
        extraction_type="schema",
        schema=Company.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=4096,
        overlap_rate=0.0,
        input_format="markdown",
        apply_chunking=False,
        extra_args={"temperature": temperature, "max_tokens": max_tokens},
        verbose=True,
    )
    
    # Check if input is a file or directory
    if os.path.isfile(args.input):
        await process_file(args.input, llm_strategy, output_dir)
    elif os.path.isdir(args.input):
        await process_directory(args.input, llm_strategy, output_dir, args.ext)
    else:
        print(f"Error: {args.input} is not a valid file or directory")

if __name__ == "__main__":
    asyncio.run(main())
