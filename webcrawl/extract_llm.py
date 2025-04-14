import os
import json
import asyncio
import logging 
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional # Added Any, Optional
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher, RateLimiter
from crawl4ai.async_configs import CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl4ai.async_configs import LLMConfig
import argparse

# Configure logging
def setup_logging():
    """Sets up the basic logging configuration."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # Set log level for LiteLLM and Botocore
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

# Setup logging at the module level
setup_logging()
logger = logging.getLogger(__name__)

class Company(BaseModel):
    company_name: str = Field(..., description="Name des Unternehmens.")
    company_url: str = Field(..., description="URL des Unternehmens.")
    products: List[str] = Field(
        ...,
        description="Produkte, die das Unternehmen vertreibt.(in Pluralform)",
        min_items=1,
        max_items=5,
    )
    machines: List[str] = Field(
        ...,
        description="(Optional)Maschinen, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
        max_items=5,
    )
    process_type: List[str] = Field(
        ...,
        description="(Optional)Produktionsprozesse, die das Unternehmen in der eigenen Fertigung nutzt.(in Pluralform)",
        max_items=5,
    )
    lohnfertigung: bool = Field(
        ..., description="Ob das Unternehmen Lohnfertigung anbietet"
    )


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


def ensure_output_directory(directory="llm_extracted_data") -> str:
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
    base_delay=(30, 60), max_delay=60, max_retries=3, rate_limit_codes=[429, 503]
)


def _get_output_filename(file_path: str, output_dir: str) -> str:
    """Generates the output JSON filename based on the input file path."""
    basename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(basename)[0]
    return os.path.join(output_dir, f"{name_without_ext}_extracted.json")


def _filter_files_to_process(file_paths: List[str], output_dir: str, overwrite: bool) -> List[str]:
    """Filters the list of file paths based on existing output files and overwrite flag."""
    if overwrite:
        return file_paths

    filtered_file_paths = []
    for path in file_paths:
        output_file = _get_output_filename(path, output_dir)
        if os.path.exists(output_file):
            logger.debug(f"Skipping {path} as output already exists at {output_file}")
            continue
        filtered_file_paths.append(path)

    if not filtered_file_paths and file_paths: # Check if initial list was not empty
        logger.info("All files already have output files. Use --overwrite to reprocess.")
    
    skipped_count = len(file_paths) - len(filtered_file_paths)
    logger.info(f"Skipped {skipped_count} files as output already exists. Use --overwrite to reprocess.")
    return filtered_file_paths


def _save_result(result_content: Any, output_dir: str, source_url: str):
    """Saves the extracted content to a JSON file."""
    parsed_url = urlparse(source_url)
    netloc = parsed_url.netloc

    if not netloc and parsed_url.path: # Handle file URLs
        basename = os.path.basename(parsed_url.path)
        name_without_ext = os.path.splitext(basename)[0]
    elif netloc: # Handle web URLs
        if netloc.startswith("www."):
            netloc = netloc[4:]
        name_without_ext = netloc
    else: # Fallback if URL parsing fails unexpectedly
        logger.warning(f"Could not determine filename from URL: {source_url}. Using fallback.")
        # Create a fallback name, e.g., based on hash or timestamp if needed
        # For now, let's just use a generic name, but this might cause collisions
        name_without_ext = f"unknown_source_{hash(source_url)}" 

    output_file = os.path.join(output_dir, f"{name_without_ext}_extracted.json")

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            if isinstance(result_content, str):
                # Attempt to parse string as JSON, otherwise write as string
                try:
                    parsed_json = json.loads(result_content)
                    json.dump(parsed_json, f, indent=2, ensure_ascii=False)
                except json.JSONDecodeError:
                    f.write(result_content) # Write as plain string if not valid JSON
            else:
                # Assume it's already a dict/list suitable for JSON
                json.dump(result_content, f, indent=2, ensure_ascii=False)
        logger.info(f"Extracted data saved to {output_file}")
    except IOError as e:
        logger.error(f"Failed to write output file {output_file}: {e}")
    except TypeError as e:
         logger.error(f"Failed to serialize result to JSON for {output_file}: {e}")


async def process_files(file_paths: List[str], llm_strategy: LLMExtractionStrategy, output_dir: str, overwrite: bool = False) -> List[Dict]:
    """
    Process one or more files using a specified LLM extraction strategy and save the results.
    
    Args:
        file_paths (list of str): List of file paths to be processed.
        llm_strategy (LLMExtractionStrategy): The language model strategy to use for extraction.
        output_dir (str): Directory where the extracted data and combined results will be saved.
        overwrite (bool, optional): Whether to overwrite existing output files. Defaults to False.
        
    Returns:
        List[Dict]: A list of extracted content (as dictionaries) from each file.
    """
    
    # Filter files first
    actual_files_to_process = _filter_files_to_process(file_paths, output_dir, overwrite)
    
    if not actual_files_to_process:
        return [] # Return early if no files need processing

    # Convert file paths to URLs with file:// protocol
    file_urls = [f"file://{os.path.abspath(path)}" for path in actual_files_to_process]
    
    logger.info(f"Processing {len(actual_files_to_process)} files...")

    config = CrawlerRunConfig(
        cache_mode=CacheMode.WRITE_ONLY,
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
        # Use actual_files_to_process for indexing results correctly
        for idx, result in enumerate(results):
            # Get the original file path corresponding to this result
            original_file_path = actual_files_to_process[idx] 
            if result.success and result.extracted_content:
                _save_result(result.extracted_content, output_dir, result.url)
                # Ensure extracted_data stores structured content if possible
                if isinstance(result.extracted_content, str):
                    try:
                       extracted_data.append(json.loads(result.extracted_content))
                    except json.JSONDecodeError:
                       extracted_data.append({"raw_content": result.extracted_content}) # Store as dict if not JSON
                else:
                     extracted_data.append(result.extracted_content)

            else:
                error_msg = getattr(result, "error_message", "Unknown error")
                logger.error(f"No content extracted from {original_file_path}: {error_msg}")
                # Optionally save error information to a file or return it
                error_info = {"error": True, "source_file": original_file_path, "message": error_msg}
                # Use the original file path to generate the error filename
                error_output_file = _get_output_filename(original_file_path, output_dir)
                try:
                    with open(error_output_file, "w", encoding="utf-8") as f:
                        json.dump(error_info, f, indent=2, ensure_ascii=False)
                    logger.info(f"Error details saved to {error_output_file}")
                except IOError as e:
                    logger.error(f"Failed to write error file {error_output_file}: {e}")


        # Show usage stats
        llm_strategy.show_usage()
        
        return extracted_data


def _find_original_file(error_json_file: str, input_dir: str, ext: str) -> Optional[str]:
    """Finds the original source file corresponding to an error JSON file.

    Args:
        error_json_file (str): The path to the JSON file indicating an error.
        input_dir (str): The directory containing the original source files.
        ext (str): The file extension of the original files.

    Returns:
        Optional[str]: The path to the original file, or None if not found.
    """
    original_name = os.path.basename(error_json_file).replace('_extracted.json', ext)
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file == original_name:
                return os.path.join(root, file)
    logger.warning(f"Could not find original file {original_name} for error file {error_json_file}")
    return None

def _find_error_files(output_dir: str) -> List[str]:
    """Scans the output directory for JSON files indicating processing errors.

    Args:
        output_dir (str): The directory containing the output JSON files.

    Returns:
        List[str]: A list of paths to JSON files that indicate errors.
    """
    error_files = []
    if not os.path.isdir(output_dir):
        logger.error(f"Output directory {output_dir} not found.")
        return error_files
        
    for json_file in os.listdir(output_dir):
        if not json_file.endswith("_extracted.json"):
            continue
        
        json_path = os.path.join(output_dir, json_file)
        if not os.path.isfile(json_path):
            continue
            
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            has_error = False
            if isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], dict) and data[0].get('error') is True:
                    has_error = True
            elif isinstance(data, dict) and data.get('error') is True:
                has_error = True
                
            if has_error:
                error_files.append(json_path)
        except json.JSONDecodeError:
            logger.warning(f"Could not decode JSON from {json_path}. It might indicate an incomplete process or error.")
            # Optionally treat decode errors as files needing reprocessing
            # error_files.append(json_path)
        except Exception as e:
            logger.error(f"Error reading or processing {json_path}: {e}")
            
    return error_files


async def check_and_reprocess_error_files(output_dir: str, input_dir: str, ext: str, llm_strategy: LLMExtractionStrategy) -> int:
    """
    Check for files with errors in the output directory and reprocess them.
    
    Args:
        output_dir (str): Directory containing the extracted JSON files
        input_dir (str): Directory containing the original source files
        ext (str): File extension of the original files (e.g., ".md")
        llm_strategy (LLMExtractionStrategy): The language model strategy to use for extraction
    
    Returns:
        int: Number of files reprocessed
    """
    
    logger.info(f"Checking for files with errors in {output_dir}...")
    
    error_json_files = _find_error_files(output_dir)
    
    files_to_reprocess = []
    for error_file_path in error_json_files:
        original_file = _find_original_file(error_file_path, input_dir, ext)
        if original_file:
            files_to_reprocess.append(original_file)
            logger.info(f"Found error marker in {os.path.basename(error_file_path)}, will reprocess {original_file}")
        # else: The warning is logged inside _find_original_file

    # Reprocess the files with errors
    if files_to_reprocess:
        logger.info(f"Reprocessing {len(files_to_reprocess)} files with errors...")
        # Always overwrite error files
        await process_files(files_to_reprocess, llm_strategy, output_dir, overwrite=True)
        return len(files_to_reprocess)
    else:
        logger.info("No files with errors found needing reprocessing.")
        return 0


async def main():
    parser = argparse.ArgumentParser(
        description="Extract data from markdown files using LLM"
    )
    parser.add_argument("input", help="Input file or directory path")
    parser.add_argument(
        "--output",
        "-o",
        help="Output directory for extracted data",
        default="llm_extracted_data",
    )
    parser.add_argument(
        "--ext", "-e", help="File extension to process (default: .md)", default=".md"
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Limit number of files to process (default: all)",
        default=None,
    )
    parser.add_argument(
        "--only-recheck",
        action="store_true",
        help="Only recheck files with errors in the output directory",
    )
    parser.add_argument(
        "--overwrite",
        "-w",
        action="store_true",
        help="Overwrite existing output files instead of skipping them",
    )

    args = parser.parse_args()

    # Ensure output directory exists
    output_dir = ensure_output_directory(args.output)

    # Define LLM strategy once
    temperature = 0.7
    max_tokens = 1000
    llm_strategy = LLMExtractionStrategy(
        llm_config=LLMConfig(
            # provider="openai/gpt-4o-mini",
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
        # verbose=True,
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
            logger.warning(f"No {args.ext} files found in {args.input}")
            return

        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            files_to_process = files_to_process[: args.limit]

        logger.info(f"Found {len(files_to_process)} files to potentially process...")
    else:
        logger.error(f"Error: {args.input} is not a valid file or directory")
        return

    # Check for and reprocess files with errors
    if os.path.isdir(args.input):
        input_dir = args.input
    else:
        input_dir = os.path.dirname(args.input)
        
    
    # If --only-recheck is specified, skip the initial processing
    if args.only_recheck:
        logger.info("Only rechecking files with errors, skipping initial processing.")
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy) 
    else:
        # Process all files and do error checking
        await process_files(files_to_process, llm_strategy, output_dir, args.overwrite)
        await check_and_reprocess_error_files(output_dir, input_dir, args.ext, llm_strategy)


if __name__ == "__main__":
    asyncio.run(main())
