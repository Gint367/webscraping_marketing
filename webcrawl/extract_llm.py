import os
import json
import asyncio
from pydantic import BaseModel, Field
from typing import List
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig
import argparse

class Company(BaseModel):
    company_name: str = Field(..., description="Name des Unternehmens.")
    company_url: str = Field(..., description="URL des Unternehmens.")
    products: List[str] = Field(..., description="Produkte, die das Unternehmen vertreibt.(in Pluralform)",
                                min_items=1, max_items=5)
    machines: List[str] = Field(..., description="(Optional)Maschinen, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
                                max_items=5)
    process_type: List[str] = Field(..., description="(Optional)Produktionsprozesse, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
                                max_items=5)
    lohnfertigung: bool = Field(..., description="Ob das Unternehmen Lohnfertigung anbietet")

prompt = """
## Task
Sie sind ein hilfsbereiter Data Analyst mit jahrelangem Wissen bei der Identifizierung von Fertigungsmaschinen, die von vielen Unternehmen eingesetzt werden. Durchsucht angegebene Webseiten und alle Unterseiten nach relevanten Informationen.

## Informationen, die gesammelt werden müssen
- **company_name:** Namen des Unternehmens (in zeile **"Company Name: "**, legale Bezeichnung, schön formatiert)  
- **company_url:** URL des Unternehmens (in zeile **"Main URL: "**)  
- **products:** Identifiziert die fünf bedeutendsten Produkte oder Dienstleistungen, die das Unternehmen anbietet (Schreiben in der Pluralform).  
- Berücksichtigt, ob ein Unternehmen eigene Produkte vertreibt, als Zulieferer tätig ist oder in der Lohnfertigung arbeitet.  
- **machines:** Erkennt den Maschinenpark des Unternehmens, d. h. welche Maschinen für die Herstellung der eigenen Produkte genutzt werden (Schreiben in der Pluralform). 
- Erfassen Sie bei machines nur Geräte für die interne Produktion, keine zum Verkauf angebotenen Maschinen.
- Gibt Maschinen nur als allgemeine Maschinenkategorie aus, ohne genaue Modell- oder Markennamen.  
  - Beispiel:  
    - "HIGH SPEED EAGLE V9" wird als **"Fräsmaschinen"** ausgegeben.  
    - "GANTRY EAGLE 1200" wird als **"Erodiermaschinen"** ausgegeben.  
- **process_type:** Analysiert die Produktionsprozesse, die das Unternehmen für die eigene Fertigung nutzt.  
  - **Keine Prozesse, die mit eigenen verkauften Maschinen durchgeführt werden können**, sondern nur die tatsächlich genutzten Verfahren.  
  - Nutzt eine **vordefinierte Liste typischer Produktionsprozesse** aus verschiedenen Branchen zur besseren Identifikation und Zuordnung.  
- Produktionsprozesse, die nicht mit der Verarbeitung oder Produktion von Materialien zu tun haben (z. B. **"Transport", "Logistik"**), werden nicht als relevante Keywords aufgenommen (Schreiben in der Pluralform).  
- **lohnfertigung:** Bietet das Unternehmen **Lohnfertigung oder Auftragsfertigung** für externe Kunden an? 
- Jeder Eintrag soll kurz und prägnant sein (für Keyword-Variablen im E-Mail-Marketing, zussamenfassen in 1 wort).
- schreibe nur max 5 Einträge aus jeder Kategorie.
- Jeder Eintrag muss ein einzelnes Wort sein, keine Komposita mit Trennzeichen oder Konjunktionen.
- Falls weniger als fünf Einträge in einer Kategorie gefunden werden, bleiben die entsprechenden Felder leer.  
- **Strikte Einhaltung der Datenwahrheit**: Keine Halluzinationen oder Ergänzungen durch eigene Annahmen.  

### **Typische Produktionsprozesse**  
- Drehen  
- Fräsen  
- Bohren  
- Schleifen  
- Erodieren  
- Laserschneiden  
- Wasserstrahlschneiden  
- Biegen
- Abkanten  
- Schweißen  
- Gießen  
- Oberflächenbehandlung  
- Montagen
- Zersägen  
- Hobeln  
- Profilieren  
- Dübeln  
- Verleimen
- Laminieren  
- Drechseln 
- Polieren  
- Lackieren
- Beizen
- Ölen  
- Spritzgießen  
- Extrudieren  
- Tiefziehen  
- Blasformen  
- Pressen  
- Schweißen  
- Bedrucken
- Kaschieren  
- Mechanische Bearbeitung  
- Mahlen  
- Mischen
- Kneten 
- Pasteurisieren
- Trocknen
- Abfüllen
- Verpacken  
- Räuchern
- Gären
- Fermentieren  
- Leiterplattenfertigung  
- Löten  
- Spritzguss
- Spinnen  
- Weben  
- Stricken  
- Färben  
- Bedrucken  
- Beschichten  
- Veredeln  
- Schneiden  
- Nähen  
- Sticken  
- Waschen  
- Trocknen  
- Bügeln
"""

def ensure_output_directory(directory="llm_extracted_data"):
    """Ensure the output directory for extracted data exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=2.0,
            max_session_permit=3,
        )
rate_limiter = RateLimiter(
    base_delay=(30, 60),
    max_delay=60,
    max_retries=3,
    rate_limit_codes=[429, 503]
)

async def process_files(file_paths, llm_strategy, output_dir):
    """
    Process one or more files using a specified LLM extraction strategy and save the results.
    Args:
        file_paths (list of str): List of file paths to be processed.
        llm_strategy (LLMStrategy): The language model strategy to use for extraction.
        output_dir (str): Directory where the extracted data and combined results will be saved.
    Returns:
        list: A list of extracted content from each file.
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
        
        extracted_data = []
        for idx, result in enumerate(results):
            file_path = file_paths[idx]
            if result.success and result.extracted_content:
                # Extract source URL info for validation and naming
                original_url = result.url
                parsed_url = urlparse(original_url)
                
                # Always use the URL for naming, regardless of whether it's a file or web URL
                netloc = parsed_url.netloc
                # For file URLs, netloc will be empty, so handle that case
                if not netloc and parsed_url.path:
                    # For file URLs, extract the filename from the path
                    basename = os.path.basename(parsed_url.path)
                    name_without_ext = os.path.splitext(basename)[0]
                else:
                    # For web URLs, remove 'www.' prefix if present
                    if netloc.startswith('www.'):
                        netloc = netloc[4:]
                    name_without_ext = netloc
                
                output_file = os.path.join(output_dir, f"{name_without_ext}_extracted.json")
                

                # Save extracted content
                with open(output_file, "w", encoding="utf-8") as f:
                    if isinstance(result.extracted_content, str):
                        f.write(result.extracted_content)
                    else:
                        json.dump(result.extracted_content, f, indent=2, ensure_ascii=False)
                
                print(f"Extracted data saved to {output_file}")
                extracted_data.append(result.extracted_content)
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error')
                print(f"No content extracted from {file_path}: {error_msg}")
        
        # Show usage stats
        llm_strategy.show_usage()

async def main():
    parser = argparse.ArgumentParser(description='Extract data from markdown files using LLM')
    parser.add_argument('input', help='Input file or directory path')
    parser.add_argument('--output', '-o', help='Output directory for extracted data', 
                        default='llm_extracted_data')
    parser.add_argument('--ext', '-e', help='File extension to process (default: .md)',
                        default='.md')
    parser.add_argument('--limit', '-l', type=int, help='Limit number of files to process (default: all)',
                        default=None)
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    output_dir = ensure_output_directory(args.output)
    
    
    # Define LLM strategy once
    temperature = 0.7
    max_tokens = 1000
    llm_strategy = LLMExtractionStrategy(
        llm_config = LLMConfig(
            #provider="openai/gpt-4o-mini", 
            provider="bedrock/amazon.nova-pro-v1:0", 
        ),
        extraction_type="schema",
        schema=Company.model_json_schema(),
        instruction=prompt,
        chunk_token_threshold=4096,
        overlap_rate=0.1,
        input_format="markdown",
        apply_chunking=False,
        extra_args={"temperature": temperature, "max_tokens": max_tokens},
        #verbose=True,
    )
    
    # Prepare list of files to process
    files_to_process = []
    
    # Check if input is a file or directory
    if os.path.isfile(args.input):
        files_to_process = [args.input]
    elif os.path.isdir(args.input):
        # Get all files with the specified extension in the directory
        for root, _, files in os.walk(args.input):
            for file in files:
                if file.endswith(args.ext):
                    files_to_process.append(os.path.join(root, file))
                    
        if not files_to_process:
            print(f"No {args.ext} files found in {args.input}")
            return
            
        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            files_to_process = files_to_process[:args.limit]
            
        print(f"Processing {len(files_to_process)} files...")
    else:
        print(f"Error: {args.input} is not a valid file or directory")
        return
    
    # Process all files with a single function call
    await process_files(files_to_process, llm_strategy, output_dir)

if __name__ == "__main__":
    asyncio.run(main())
