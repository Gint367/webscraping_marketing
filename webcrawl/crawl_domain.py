import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin
import re
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlResult, CrawlerMonitor, CrawlerRunConfig, CacheMode, DefaultMarkdownGenerator, MemoryAdaptiveDispatcher, PruningContentFilter
import logging
# Import the new function
from excel_reader import read_urls_from_excel
from get_company_by_category import read_urls_and_companies
from get_company_by_top1machine import read_urls_and_companies_by_top1machine
# Import to handle colorama recursion issues
import sys
# Import argparse for command line arguments
import argparse

# Add a function to disable colorama to prevent recursion errors
def disable_colorama():
    """
    Disable colorama's text processing to prevent recursion errors with large outputs.
    This is necessary when processing very large strings that can exceed Python's recursion limit.
    """
    try:
        import colorama
        # Disable colorama's text processing
        colorama.deinit()
        print("Colorama disabled to prevent recursion errors with large outputs.")
    except ImportError:
        pass

# Add a function to parse command line arguments
def parse_args():
    """
    Parse command line arguments for the crawler.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Crawl domains from an Excel file and save content to markdown files")
    
    parser.add_argument(
        "--excel", 
        "-e", 
        type=str, 
        required=True,
        help="Path to Excel file containing URLs and company names"
    )
    
    parser.add_argument(
        "--output", 
        "-o", 
        type=str, 
        required=True,
        help="Output directory for aggregated content"
    )
    
    parser.add_argument(
        "--max-links",
        type=int,
        default=60,
        help="Maximum number of internal links to crawl per domain (default: 60)"
    )
    
    return parser.parse_args()

def sanitize_filename(url):
    """Convert URL to a valid filename by removing scheme and replacing invalid characters"""
    parsed = urlparse(url)
    domain = parsed.netloc
    # Remove 'www.' prefix if it exists
    if domain.startswith('www.'):
        domain = domain[4:]
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', domain)
    return sanitized

def ensure_output_directory(directory):
    """Ensure the output directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def get_path_depth(url):
    """
    Calculate the path depth of a URL.
    For German sites, we include the 'de' language path in the depth calculation.
    For example: https://www.schroedergroup.eu/de/produkte/maschinen/ has a depth of 3
    """
    parsed = urlparse(url)
    path = parsed.path.strip('/')  # Remove leading and trailing slashes
    
    if not path:
        return 0
    
    # Split the path into segments
    segments = path.split('/')
    
    # For German sites, we want to include the language code 'de' in the depth calculation
    # No need to exclude language codes anymore, simply count all segments
    
    # Return the number of segments
    return len(segments)

def filter_urls_by_depth_reverse(urls):
    """
    Filter URLs to include only those with one level less than the maximum depth.
    Also removes duplicates.
    """
    if not urls:
        return []
    
    # Calculate depths for all URLs
    url_depths = [(url, get_path_depth(url)) for url in urls]
    
    # Find the maximum depth
    max_depth = max(depth for _, depth in url_depths)
    
    # If max_depth is 0 or 1, keep all URLs to avoid empty results
    target_depth = max_depth - 1 if max_depth > 1 else max_depth
    
    print(f"Maximum URL depth: {max_depth}, filtering for depth: {target_depth}")
    
    # Filter URLs by target depth
    filtered_urls = [url for url, depth in url_depths if depth == target_depth]
    
    # Remove duplicates while preserving order
    unique_urls = []
    seen = set()
    
    for url in filtered_urls:
        normalized_url = url.rstrip('/')  # Remove trailing slash for comparison
        if normalized_url not in seen:
            seen.add(normalized_url)
            unique_urls.append(url)
    
    print(f"Filtered from {len(urls)} to {len(unique_urls)} URLs based on depth")
    return unique_urls

def filter_urls_by_depth(urls, target_depth=2):
    """
    Filter URLs to include only those with depths less than or equal to the specified depth.
    Also removes duplicates.
    # Example URLs with different depths
    example_urls = [
        "https://www.example.com",  # Depth 0
        "https://www.example.com/about",  # Depth 1
        "https://www.example.com/about/team",  # Depth 2
        "https://www.example.com/about/team/members",  # Depth 3
    ]

    Args:
        urls: List of URLs to filter
        target_depth: The maximum depth level to keep (default: 2)
    
    Returns:
        List of filtered URLs with no duplicates
    """
    if not urls:
        return []
    
    # Calculate depths for all URLs
    url_depths = [(url, get_path_depth(url)) for url in urls]
    
    # Filter URLs by target depth - keep only URLs with depth <= target_depth
    filtered_urls = [url for url, depth in url_depths if depth <= target_depth]
    
    # Remove duplicates while preserving order
    unique_urls = []
    seen = set()
    
    for url in filtered_urls:
        normalized_url = url.rstrip('/')  # Remove trailing slash for comparison
        if normalized_url not in seen:
            seen.add(normalized_url)
            unique_urls.append(url)
    
    print(f"Filtered from {len(urls)} to {len(unique_urls)} URLs with depth <= {target_depth}")
    return unique_urls

async def collect_internal_links(crawler, main_url, max_links=50):
    """
    Collect all internal links from the main URL up to a specified maximum number of links.
    Intelligently handles both German sites with /de/ language codes and those without.
    """
    print(f"Collecting internal links from {main_url}...")
    
    # Configure crawler for link collection
    crawl_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=10  # We want all pages, even small ones
    )
    
    # Crawl the main URL
    result = await crawler.arun(main_url, config=crawl_config)
    
    if not result.success:
        print(f"Failed to collect links from {main_url}: {result.error if hasattr(result, 'error') else 'Unknown error'}")
        return []
    
    # Get internal links
    internal_links = result.links.get('internal', [])
    
    # Limit number of links to avoid overwhelming the system
    if len(internal_links) > max_links:
        print(f"Found {len(internal_links)} internal links. Limiting to {max_links}.")
        internal_links = internal_links[:max_links]
    else:
        print(f"Found {len(internal_links)} internal links.")
    
    # Ensure all URLs are absolute
    domain = urlparse(main_url).netloc
    scheme = urlparse(main_url).scheme
    base_url = f"{scheme}://{domain}"
    
    # Check if this is a site that uses language codes in paths
    uses_language_codes = any(pattern in main_url.lower() for pattern in ['/de/', '/de-de/', '/de_de/'])
    print(f"Site appears to {'' if uses_language_codes else 'not '}use language codes in URLs")
    
    # Common non-content pages to filter out (German and English)
    non_content_keywords = [
        'login', 'signup', 'register', 'download', 'datenschutz', 
        'kontakt', 'contact', 'privacy', 'agb', 'cart', 
        'warenkorb', 'checkout', 'search', 'suche', 'sitemap',
        'anmelden', 'registrieren', 'einkaufswagen', 'newsletter',
        'terms', 'conditions', 'faq', 'hilfe', 'help', 'support',
        'nutzungsbedingungen', 'cookie', 'imprint', 'impressum',
        'disclaimer', 'rechtliches', 'legal', 'terms-of-service',
    ]
    
    # Non-German language codes to filter out
    non_german_language_patterns = [
        '/en/', '/en-us/', '/en_us/', '/en-gb/', '/en_gb/', 
        '/fr/', '/es/', '/it/', '/nl/', '/pl/', '/ru/', '/zh/', 
        '/ja/', '/ko/', '/ar/', '/pt/', '/tr/', '/sv/'
    ]
    
    absolute_links = []
    filtered_count = 0
    anchor_filtered_count = 0
    language_filtered_count = 0
    
    for link_obj in internal_links:
        # Handle the link dictionary structure with 'href' key
        if isinstance(link_obj, dict):
            if 'href' in link_obj:
                link = link_obj['href']
            elif 'url' in link_obj:
                link = link_obj['url']
            else:
                print(f"Skipping link object without href or url: {link_obj}")
                continue
        elif isinstance(link_obj, str):
            link = link_obj
        else:
            print(f"Skipping unexpected link format: {link_obj}")
            continue
        
        # Make sure the URL is absolute
        if not link.startswith(('http://', 'https://')):
            absolute_link = urljoin(base_url, link)
        else:
            # Only include links from the same domain
            if urlparse(link).netloc != domain:
                continue
            absolute_link = link
            
        # Filter out URLs containing '#' as they're just anchor links to the same page
        if '#' in absolute_link:
            print(f"Skipping anchor link: {absolute_link}")
            anchor_filtered_count += 1
            continue
            
        # Skip URLs with file extensions, but be careful not to filter out domains
        # Check the path part of the URL for file extensions, not the domain part
        parsed_url = urlparse(absolute_link)
        path = parsed_url.path.lower()  # Convert to lowercase for case-insensitive matching
        
        # Skip files like PDFs, images, etc. but keep HTML and dynamic pages
        if path and '.' in path.split('/')[-1]:
            file_extension = path.split('.')[-1].lower()
            
            # Skip if it's a known file extension (not a webpage)
            if file_extension not in ['html', 'htm', 'php', 'asp', 'aspx', 'jsp', '']:
                print(f"Skipping link with file extension: {absolute_link}")
                filtered_count += 1
                continue
        
        # Is this the base domain?
        is_base_domain = path == "" or path == "/"
        
        # Skip non-content pages
        if not is_base_domain:  # Always keep the base domain
            # Check if any non-content keyword is in the URL path
            if any(keyword in path for keyword in non_content_keywords):
                print(f"Skipping non-content page: {absolute_link}")
                filtered_count += 1
                continue
        
        # Language filtering logic - more comprehensive check:
        # 1. If a URL contains a non-German language code, skip it
        # 2. If site uses language codes and URL doesn't have /de/, it might be another language
        keep_url = True
        
        # Always check for and filter out non-German language patterns
        if any(pattern in absolute_link.lower() for pattern in non_german_language_patterns):
            print(f"Skipping non-German language URL: {absolute_link}")
            language_filtered_count += 1
            keep_url = False
        elif uses_language_codes:
            # For sites with language codes, prefer German pages
            has_german = any(pattern in absolute_link.lower() for pattern in ['/de/', '/de-de/', '/de_de/']) or is_base_domain
            if not has_german:
                # Check if it's any language code pattern (like /xx/)
                if re.search(r'/[a-z]{2}(-[a-z]{2})?/', absolute_link.lower()):
                    print(f"Skipping possible non-German language URL: {absolute_link}")
                    language_filtered_count += 1
                    keep_url = False
        
        if keep_url:
            absolute_links.append(absolute_link)
    
    print(f"Filtered out {filtered_count} non-content pages")
    print(f"Filtered out {anchor_filtered_count} anchor links (URLs with '#')")
    print(f"Filtered out {language_filtered_count} non-German language URLs")
    
    # Filter URLs based on path depth - keep URLs at specified target depth
    absolute_links = filter_urls_by_depth(absolute_links, target_depth=2)
    
    # Remove duplicates while considering URLs with different query parameters as the same
    #print("Removing duplicate URLs (ignoring query parameters)...")
    unique_links = []
    seen_normalized_urls = set()
    
    for url in absolute_links:
        # Normalize the URL by removing query parameters
        parsed = urlparse(url)
        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # Remove trailing slash for comparison
        normalized_url = normalized_url.rstrip('/')
        
        if normalized_url not in seen_normalized_urls:
            seen_normalized_urls.add(normalized_url)
            unique_links.append(url)
    
    print(f"Reduced from {len(absolute_links)} to {len(unique_links)} URLs after removing duplicates (normalized by path)")
    
    return unique_links

def remove_links_from_markdown(markdown_text):
    """
    Remove markdown links from text while preserving the link text.
    
    Handles these link formats:
    - [link text](url)
    - [link text](url "title")
    - <url>
    - [![alt text](image_url)](link_url)
    - ![alt text](image_url)
    - [](url)
    
    Args:
        markdown_text (str): Markdown text containing links
        
    Returns:
        str: Markdown text with links removed but link text preserved
    """
    # Replace standard markdown links [text](url) and [text](url "title") with just text
    link_pattern = r'\[([^\]]*)\]\((?:https?://[^)]+)(?:\s+"[^"]*")?\)'
    text_without_links = re.sub(link_pattern, r'\1', markdown_text)
    
    # Remove image links with format [![alt text](image_url)](link_url)
    image_link_pattern = r'\[!\[[^\]]*\]\([^)]+\)\]\([^)]+\)'
    text_without_links = re.sub(image_link_pattern, '', text_without_links)
    
    # Remove image tags ![alt text](image_url)
    image_pattern = r'!\[[^\]]*\]\([^)]+\)'
    text_without_links = re.sub(image_pattern, '', text_without_links)
    
    # Remove empty links [](url)
    empty_link_pattern = r'\[\]\([^)]+\)'
    text_without_links = re.sub(empty_link_pattern, '', text_without_links)
    
    # Remove bare URLs with angle brackets: <https://example.com>
    url_pattern = r'<(https?://[^>]+)>'
    text_without_links = re.sub(url_pattern, '', text_without_links)
    
    # Remove any leftover simple URLs
    simple_url_pattern = r'https?://\S+'
    text_without_links = re.sub(simple_url_pattern, '', text_without_links)
    
    # Clean up multiple consecutive whitespaces and newlines
    text_without_links = re.sub(r'\n\s*\n\s*\n', '\n\n', text_without_links)
    text_without_links = re.sub(r' {2,}', ' ', text_without_links)
    
    return text_without_links

async def crawl_domain(main_url, output_dir_aggregated="domain_content_aggregated", max_links=50, company_name= None):
    """
    Crawl a main URL and all its internal links, then aggregate the content.
    Uses a two-phase approach:
    1. First crawl the main URL completely
    2. Then crawl the rest of the internal links (body content only)
    
    Args:
        main_url (str): URL to crawl
        output_dir_aggregated (str): Directory to save aggregated content
        max_links (int): Maximum internal links to crawl
        company_name (str, optional): Company name associated with the URL
    
    Returns:
        str: Path to the aggregated markdown file
        int: Number of pages crawled
    """
    prune_filter = PruningContentFilter(
        threshold=0.45,
        min_word_threshold=30,
        threshold_type="dynamic",
    )
    
    # Create browser configuration
    browser_cfg = BrowserConfig(
        #verbose=True,
        headless=True,
        text_mode=True,
    )
    
    # Create crawler configuration for main URL - complete crawl
    main_crawl_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        delay_before_return_html=2.0,
        word_count_threshold=20,
        #magic=True
    )
    
    # Create crawler configuration for internal links - body only
    body_only_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=50,
        delay_before_return_html=1.0,
        #magic=True,
        #remove_forms=True,
        # Only extract the main content body
        excluded_tags=['header', 'footer', 'nav', 'img'],
        excluded_selector="img",
        markdown_generator= DefaultMarkdownGenerator(
            content_filter=prune_filter,
        )
    )
    
    try:
        from crawl4ai import DisplayMode # display mode can crash the program on background run
        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=2.0,
            max_session_permit=10,
        )
        """
            monitor=CrawlerMonitor(
                display_mode=DisplayMode.DETAILED
            )
        """
    except ImportError:
        dispatcher = None
    
    # Ensure both output directories exist
    ensure_output_directory(output_dir_aggregated)
    
    # Get domain name for the output file
    domain_name = sanitize_filename(main_url)
    
    # Create output filenames in their respective directories
    #output_markdown_file = os.path.join(output_dir_aggregated, f"{domain_name}_{current_date}.md")
    output_markdown_file = os.path.join(output_dir_aggregated, f"{domain_name}.md")
    
    # Initialize aggregate content
    aggregate_content = f"# Aggregated Content for {domain_name}\n\n"
    
    # Add company name if provided
    if company_name:
        aggregate_content += f"Company Name: {company_name}\n"
    
    aggregate_content += f"Main URL: {main_url}\n"
    aggregate_content += f"Crawled on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    # Temporarily increase recursion limit for large data processing
    original_limit = sys.getrecursionlimit()
    try:
        sys.setrecursionlimit(10000)  # Increase the recursion limit
        
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            # Set German language header
            crawler.crawler_strategy.set_custom_headers(
                {"Accept-Language": "de-DE,de;q=0.9"}
            )
            
            # Phase 1: Crawl the main URL
            print(f"\n=== Phase 1: Crawling main URL: {main_url} ===\n")
            main_result : CrawlResult = await crawler.arun(main_url, config=main_crawl_config)
            
            if not main_result.success:
                print(f"Failed to crawl main URL: {main_url}")
                print(f"Error: {main_result.error if hasattr(main_result, 'error') else 'Unknown error'}")
                return output_markdown_file, 0
            
            # Process main URL result
            aggregate_content += f"## Main Page: {main_url}\n\n"
            aggregate_content += f"### Title: {main_result.metadata.get('title', 'Untitled')}\n\n"
            
            # Add the raw content
            aggregate_content += "### Content:\n\n"
            aggregate_content += main_result.markdown + "\n\n" 
            aggregate_content += "-" * 80 + "\n\n"
            
            print(f"Successfully crawled main URL: {main_url}")
            
            # Now collect internal links from the main result
            internal_links = await collect_internal_links(crawler, main_url, max_links)
            
            # Remove the main URL from the list if present, as we've already crawled it
            internal_links = [link for link in internal_links if link != main_url]
            
            # Phase 2: Crawl the internal links (body content only)
            if internal_links:
                print(f"\n=== Phase 2: Crawling {len(internal_links)} internal links (body only) ===\n")
                # Now crawl all internal links at once using arun_many with body-only config
                if dispatcher:
                    results  = await crawler.arun_many(
                        internal_links,
                        config=body_only_config,
                        #verbose=True,
                        dispatcher=dispatcher
                    )
                else:
                    results  = await crawler.arun_many(
                        internal_links,
                        config=body_only_config,
                        #verbose=True
                    )
                
                # Process results
                for i, result in enumerate(results):
                    url = result.url if hasattr(result, 'url') else internal_links[i]
                    
                    if result.success:
                        # Add the page content to our aggregate
                        aggregate_content += f"## Page {i+1}: {url}\n\n"
                        aggregate_content += f"### Title: {result.metadata.get('title', 'Untitled')}\n\n"
                        
                        # Add the content with links removed
                        aggregate_content += "### Content (body only):\n\n"
                        # Remove links from the content for the internal pages
                        # Check if the result has a 'fit_markdown' attribute, else use 'markdown'
                        if hasattr(result, 'markdown') and isinstance(result.markdown, (str, object)):
                            content_to_clean = result.markdown.fit_markdown if hasattr(result.markdown, 'fit_markdown') else result.markdown
                            cleaned_content = remove_links_from_markdown(content_to_clean)
                            aggregate_content += cleaned_content + "\n\n"
                        else:
                            print(f"Unexpected markdown format for URL: {url}")
                            aggregate_content += "Content could not be processed.\n\n"
                        
                        print(f"Successfully crawled: {url}")
                    else:
                        # Report failure
                        error_msg = result.error if hasattr(result, 'error') else "Unknown error"
                        aggregate_content += f"## Page {i+1}: {url}\n\n"
                        aggregate_content += f"Failed to crawl: {error_msg}\n\n"
                        aggregate_content += "-" * 80 + "\n\n"
                        
                        print(f"Failed to crawl: {url}, Error: {error_msg}")
            else:
                print("No additional internal links found to crawl")
            
            # If the content gets very large, disable colorama to prevent recursion errors
            if len(aggregate_content) > 100000:  # 100KB threshold
                disable_colorama()
                
    finally:
        # Restore original recursion limit
        sys.setrecursionlimit(original_limit)
    
    # Write the aggregate content to file
    with open(output_markdown_file, "w", encoding="utf-8") as f:
        f.write(aggregate_content)
    print(f"Aggregate content saved to {output_markdown_file}")
    
    # Count total pages crawled (main URL + internal links that were successfully crawled)
    total_crawled = 1 + len([r for r in results if getattr(r, 'success', False)]) if 'results' in locals() else 1
    
    return output_markdown_file, total_crawled

async def main():
    """
    Main function to initiate the web crawling process.
    This function parses command line arguments, reads URLs and company names from an Excel file or uses hardcoded domains,
    creates the output directory, and then crawls each domain to collect data.
    Arguments:
        None
    Command Line Arguments:
        --excel: Path to the Excel file containing URLs and company names.
        --output: Directory where the output files will be saved.
        --max_links: Maximum number of links to crawl per domain.
    Returns:
        None
    """
    
    # Parse command line arguments
    args = parse_args()
    
    # Use arguments for Excel file and output directory
    excel_file = args.excel
    output_dir = args.output
    max_links = args.max_links
    
    # You can either use hardcoded domains or read from Excel
    use_excel = True  # Set to True to use Excel file
    
    if use_excel:
        # Use the Excel file path from arguments
        urls_and_companies = read_urls_and_companies_by_top1machine(excel_file) # For merged excel files from merge_excel.py
        # print first 10 entries
        #print(f"First 10 entries from Excel: {urls_and_companies[:20]}")
        if not urls_and_companies:
            return print("No valid URLs found in Excel file. Using default domains instead.")
            
    else:
        # List of domains to crawl (hardcoded)
        domains = [
            [
                ("https://www.pfaff-industrial.de/", "PFAFF Industriesysteme und Maschinen"),
                
            ]
        ]
        # Convert to the same format as Excel reader output for consistent handling
        urls_and_companies = [(url, company) for sublist in domains for url, company in sublist]
    
    # Create output directory from arguments
    output_dir = args.output
    
    # Crawl each domain
    results = []
    for url, company_name in urls_and_companies:
        company_info = f" ({company_name})" if company_name else ""
        print(f"\n{'='*40}\nStarting crawl of domain: {url}{company_info}\n{'='*40}\n")
        
        # Disable colorama before processing large amounts of data
        disable_colorama()
        
        markdown_file, page_count = await crawl_domain(
            url, 
            output_dir_aggregated=output_dir, 
            max_links=max_links,
            company_name=company_name
        )
        results.append({
            "domain": url,
            "company_name": company_name,
            "markdown_file": markdown_file,
            "pages_crawled": page_count
        })
    
    # Summary of results
    print("\n\n" + "="*40)
    print("CRAWL SUMMARY")
    print("="*40)
    for result in results:
        print(f"Domain: {result['domain']}")
        if result['company_name']:
            print(f"Company: {result['company_name']}")
        print(f"Pages crawled: {result['pages_crawled']}")
        print(f"Markdown file: {os.path.basename(result['markdown_file'])}")
        print("-"*40)

if __name__ == "__main__":

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Increase the recursion limit for the entire program
    sys.setrecursionlimit(5000)
    
    # Disable colorama for potentially large outputs
    disable_colorama()
    
    asyncio.run(main())
