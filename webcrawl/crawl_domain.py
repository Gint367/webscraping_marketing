import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin
import re
from typing import List, Dict, Tuple, Set, Optional, Any, Union
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

# Configuration constants
# ----------------------

# File and URL constants
WEBPAGE_EXTENSIONS = ['html', 'htm', 'php', 'asp', 'aspx', 'jsp', '']
LARGE_CONTENT_THRESHOLD = 100000  # 100KB threshold for disabling colorama

# Language-related constants
GERMAN_LANGUAGE_PATTERNS = ['/de/', '/de-de/', '/de_de/']
NON_GERMAN_LANGUAGE_PATTERNS = [
    '/en/', '/en-us/', '/en_us/', '/en-gb/', '/en_gb/', 
    '/fr/', '/es/', '/it/', '/nl/', '/pl/', '/ru/', '/zh/', 
    '/ja/', '/ko/', '/ar/', '/pt/', '/tr/', '/sv/'
]

# URL filtering constants
NON_CONTENT_KEYWORDS = [
    'login', 'signup', 'register', 'download', 'datenschutz', 
    'kontakt', 'contact', 'privacy', 'agb', 'cart', 
    'warenkorb', 'checkout', 'search', 'suche', 'sitemap',
    'anmelden', 'registrieren', 'einkaufswagen', 'newsletter',
    'terms', 'conditions', 'faq', 'hilfe', 'help', 'support',
    'nutzungsbedingungen', 'cookie', 'imprint', 'impressum',
    'disclaimer', 'rechtliches', 'legal', 'terms-of-service',
]

# Crawler configuration constants
DEFAULT_PATH_DEPTH = 2
DEFAULT_MAX_LINKS = 50
DEFAULT_MEMORY_THRESHOLD = 70.0
PRUNE_FILTER_THRESHOLD = 0.45
PRUNE_FILTER_MIN_WORDS = 30
CRAWL_WORD_COUNT_THRESHOLD = {
    'main': 10,    # For main URL
    'body': 20,    # For body-only content
    'links': 5     # For link collection
}
EXCLUDED_TAGS = ['header', 'footer', 'nav', 'img']

# Browser headers
GERMAN_LANGUAGE_HEADERS = {"Accept-Language": "de-DE,de;q=0.9"}

# System constants
RECURSION_LIMIT = 10000  # Temporary recursion limit for large data processing

# Add a function to disable colorama to prevent recursion errors
def disable_colorama() -> None:
    """
    Disable colorama's text processing to prevent recursion errors with large outputs.
    
    This is necessary when processing very large strings that can exceed Python's recursion limit.
    Colorama can cause stack overflow when processing large texts due to its recursive pattern matching.
    
    Returns:
        None
    """
    try:
        import colorama
        # Disable colorama's text processing
        colorama.deinit()
        print("Colorama disabled to prevent recursion errors with large outputs.")
    except ImportError:
        pass

# Add a function to parse command line arguments
def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the crawler.
    
    Defines and processes the following arguments:
    - excel/e: Path to Excel file containing URLs and company names (required)
    - output/o: Output directory for aggregated content (required)
    - max-links: Maximum number of internal links to crawl per domain (default: 60)
    
    Returns:
        argparse.Namespace: Parsed command line arguments
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

def sanitize_filename(url: str) -> str:
    """
    Convert URL to a valid filename by removing scheme and replacing invalid characters.
    
    Args:
        url: The URL to convert to a filename
        
    Returns:
        str: A sanitized string that can be safely used as a filename
             (removes www. prefix and replaces invalid characters with underscores)
    """
    parsed = urlparse(url)
    domain = parsed.netloc
    # Remove 'www.' prefix if it exists
    if domain.startswith('www.'):
        domain = domain[4:]
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', domain)
    return sanitized

def ensure_output_directory(directory: str) -> str:
    """
    Ensure the output directory exists, creating it if necessary.
    
    Args:
        directory: Path to the directory that should exist
        
    Returns:
        str: The validated directory path
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def get_path_depth(url: str) -> int:
    """
    Calculate the path depth of a URL by counting path segments.
    
    For German sites, includes the 'de' language path in the depth calculation.
    
    Examples:
        - https://example.com/ -> 0
        - https://example.com/about -> 1
        - https://www.schroedergroup.eu/de/produkte/maschinen/ -> 3
    
    Args:
        url: The URL to analyze
        
    Returns:
        int: The depth of the URL path (number of segments)
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

def filter_urls_by_depth_reverse(urls: List[str]) -> List[str]:
    """
    Filter URLs to include only those with one level less than the maximum depth.
    
    This is useful for focusing on category pages rather than deep leaf pages.
    
    Algorithm:
    1. Calculate the depth for each URL
    2. Find the maximum depth across all URLs
    3. Filter URLs to keep only those at (max_depth - 1)
    4. Remove duplicates while preserving order
    
    Args:
        urls: List of URLs to filter
        
    Returns:
        List[str]: Filtered URLs with depth = max_depth - 1 (or max_depth if max_depth ≤ 1)
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

def filter_urls_by_depth(urls: List[str], target_depth: int = 2) -> List[str]:
    """
    Filter URLs to include only those with depths less than or equal to the specified depth.
    
    Useful for focusing on higher-level pages with more general content rather than
    deep, specific pages.
    
    Example for different depths:
        - "https://www.example.com"              -> Depth 0
        - "https://www.example.com/about"        -> Depth 1
        - "https://www.example.com/about/team"   -> Depth 2
        - "https://www.example.com/about/team/members" -> Depth 3

    Args:
        urls: List of URLs to filter
        target_depth: The maximum depth level to keep (default: 2)
    
    Returns:
        List[str]: Filtered unique URLs with depth ≤ target_depth
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

def is_non_content_url(url_path: str) -> bool:
    """
    Check if a URL path corresponds to a non-content page.
    
    Non-content pages are utility pages like login, contact, privacy policy, etc.
    that don't contain substantive information about the business or its products.
    
    Args:
        url_path: Path portion of the URL to check (without domain)
        
    Returns:
        bool: True if it's a non-content URL that should be filtered out, False otherwise
    """
    
    # Check if any non-content keyword is in the URL path
    return any(keyword in url_path for keyword in NON_CONTENT_KEYWORDS)

def is_file_url(url_path: str) -> bool:
    """
    Check if a URL path points to a file rather than a webpage.
    
    Identifies file URLs based on file extensions, filtering out non-HTML resources
    like PDFs, images, documents, etc. 
    
    Args:
        url_path: Path portion of the URL to check (without domain)
        
    Returns:
        bool: True if it's a file URL (not a webpage), False otherwise
    """
    # Skip if there's no path or no file extension
    if not url_path or '.' not in url_path.split('/')[-1]:
        return False
    
    # Get the file extension
    file_extension = url_path.split('.')[-1].lower()
    
    # Return False for common webpage extensions
    return file_extension not in WEBPAGE_EXTENSIONS

def should_filter_by_language(url: str, uses_language_codes: bool, is_base_domain: bool) -> bool:
    """
    Determine if a URL should be filtered based on language.
    
    For sites with multiple language versions, this function helps filter out
    non-German content, focusing only on German pages. It handles:
    1. URLs with explicit language codes (/en/, /fr/, etc.)
    2. Sites that use the /de/ pattern for German content
    
    Args:
        url: The URL to check
        uses_language_codes: Whether the site uses language codes in paths
        is_base_domain: Whether this is the base domain URL
        
    Returns:
        bool: True if URL should be filtered out (non-German), False if it should be kept
    """
    
    # Always check for and filter out non-German language patterns
    if any(pattern in url.lower() for pattern in NON_GERMAN_LANGUAGE_PATTERNS):
        return True
    
    # For sites with language codes, prefer German pages
    if uses_language_codes and not is_base_domain:
        has_german = any(pattern in url.lower() for pattern in GERMAN_LANGUAGE_PATTERNS)
        if not has_german:
            # Check if it's any language code pattern (like /xx/)
            if re.search(r'/[a-z]{2}(-[a-z]{2})?/', url.lower()):
                return True
    
    return False

def normalize_and_filter_links(internal_links: List[Any], base_url: str, max_links: int) -> List[str]:
    """
    Normalize links to absolute URLs and perform initial filtering.
    
    Handles different types of link objects, converts to absolute URLs,
    filters out anchor links, and limits the number of links to process.
    
    Args:
        internal_links: List of internal links (may be strings or dicts with 'href'/'url' keys)
        base_url: The base URL of the website (scheme + domain)
        max_links: Maximum number of links to return
    
    Returns:
        List[str]: Filtered and normalized absolute URLs
    """
    domain = urlparse(base_url).netloc
    
    absolute_links = []
    anchor_filtered_count = 0
    
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
        
        absolute_links.append(absolute_link)
    
    print(f"Filtered out {anchor_filtered_count} anchor links (URLs with '#')")
    
    # Limit number of links to avoid overwhelming the system
    if len(absolute_links) > max_links:
        print(f"Found {len(absolute_links)} internal links. Limiting to {max_links}.")
        absolute_links = absolute_links[:max_links]
    
    return absolute_links

def apply_content_filters(urls: List[str], base_url: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Apply content-based filters to URLs to focus on relevant content pages.
    
    Applies multiple filtering criteria:
    1. File type filtering (removes PDFs, images, etc.)
    2. Non-content page filtering (removes login, contact, etc.)
    3. Language filtering (focuses on German content)
    
    Args:
        urls: List of URLs to filter
        base_url: The base URL of the website
    
    Returns:
        Tuple[List[str], Dict[str, int]]: (filtered_urls, filtered_counts) where
        filtered_counts is a dict with keys 'content_filtered' and 'language_filtered'
    """
    filtered_urls = []
    filtered_count = 0
    language_filtered_count = 0
    
    # Check if this is a site that uses language codes in paths
    uses_language_codes = any(pattern in base_url.lower() for pattern in GERMAN_LANGUAGE_PATTERNS)
    print(f"Site appears to {'' if uses_language_codes else 'not '}use language codes in URLs")
    
    for url in urls:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()  # Convert to lowercase for case-insensitive matching
        
        # Is this the base domain?
        is_base_domain = path == "" or path == "/"
        
        # Skip file URLs
        if is_file_url(path):
            print(f"Skipping link with file extension: {url}")
            filtered_count += 1
            continue
        
        # Skip non-content pages
        if not is_base_domain and is_non_content_url(path):
            print(f"Skipping non-content page: {url}")
            filtered_count += 1
            continue
        
        # Apply language filtering
        if should_filter_by_language(url, uses_language_codes, is_base_domain):
            print(f"Skipping non-German language URL: {url}")
            language_filtered_count += 1
            continue
        
        filtered_urls.append(url)
    
    filter_counts = {
        'content_filtered': filtered_count,
        'language_filtered': language_filtered_count
    }
    
    return filtered_urls, filter_counts

def remove_duplicate_urls(urls: List[str]) -> List[str]:
    """
    Remove duplicate URLs while considering URLs with different query parameters as the same.
    
    Normalizes URLs by:
    1. Removing query parameters
    2. Removing trailing slashes
    3. Comparing only scheme, domain, and path
    
    Args:
        urls: List of URLs to deduplicate
        
    Returns:
        List[str]: Deduplicated list of URLs preserving original order
    """
    unique_links = []
    seen_normalized_urls = set()
    
    for url in urls:
        # Normalize the URL by removing query parameters
        parsed = urlparse(url)
        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # Remove trailing slash for comparison
        normalized_url = normalized_url.rstrip('/')
        
        if normalized_url not in seen_normalized_urls:
            seen_normalized_urls.add(normalized_url)
            unique_links.append(url)
    
    print(f"Reduced from {len(urls)} to {len(unique_links)} URLs after removing duplicates (normalized by path)")
    
    return unique_links

async def collect_internal_links(crawler: AsyncWebCrawler, main_url: str, max_links: int = DEFAULT_MAX_LINKS) -> List[str]:
    """
    Collect and filter internal links from a website's main URL.
    
    Performs a series of operations:
    1. Crawls the main URL to extract all internal links
    2. Normalizes links to absolute URLs
    3. Filters out non-content, file, and non-German pages
    4. Filters by path depth to focus on important pages
    5. Removes duplicates
    
    Args:
        crawler: Initialized AsyncWebCrawler instance
        main_url: Primary URL to crawl for internal links
        max_links: Maximum number of internal links to return
    
    Returns:
        List[str]: Filtered list of internal URLs to crawl
    """
    print(f"Collecting internal links from {main_url}...")
    
    # Configure crawler for link collection
    crawl_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=CRAWL_WORD_COUNT_THRESHOLD['links']  # We want all pages, even small ones
    )
    
    # Crawl the main URL
    result = await crawler.arun(main_url, config=crawl_config)
    
    if not result.success:
        print(f"Failed to collect links from {main_url}: {result.error if hasattr(result, 'error') else 'Unknown error'}")
        return []
    
    # Get internal links
    internal_links = result.links.get('internal', [])
    
    print(f"Found {len(internal_links)} internal links.")
    
    # Ensure all URLs are absolute and perform initial filtering
    domain = urlparse(main_url).netloc
    scheme = urlparse(main_url).scheme
    base_url = f"{scheme}://{domain}"
    
    # Normalize and filter links
    absolute_links = normalize_and_filter_links(internal_links, base_url, max_links)
    
    # Apply content and language filters
    filtered_links, filter_counts = apply_content_filters(absolute_links, main_url)
    print(f"Filtered out {filter_counts['content_filtered']} non-content pages")
    print(f"Filtered out {filter_counts['language_filtered']} non-German language URLs")
    
    # Filter URLs based on path depth - keep URLs at specified target depth
    filtered_links = filter_urls_by_depth(filtered_links, target_depth=DEFAULT_PATH_DEPTH)
    
    # Remove duplicates
    unique_links = remove_duplicate_urls(filtered_links)
    
    return unique_links

def remove_links_from_markdown(markdown_text: str) -> str:
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
        markdown_text: Markdown text containing links
        
    Returns:
        str: Cleaned markdown text with links removed but link text preserved
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

async def crawl_domain(
    main_url: str, 
    output_dir_aggregated: str = "domain_content_aggregated", 
    max_links: int = DEFAULT_MAX_LINKS, 
    company_name: Optional[str] = None
) -> Tuple[str, int]:
    """
    Crawl a main URL and all its internal links, then aggregate the content.
    
    Uses a two-phase approach:
    1. First crawl the main URL completely
    2. Then crawl the internal links (body content only) up to max_links
    
    The crawled content is processed, cleaned, and aggregated into a single markdown file.
    
    Args:
        main_url: Primary URL to crawl
        output_dir_aggregated: Directory to save aggregated markdown content
        max_links: Maximum number of internal links to crawl (default: 50)
        company_name: Optional company name associated with the URL
    
    Returns:
        Tuple[str, int]: (output_file_path, pages_crawled)
            - output_file_path: Path to the generated markdown file
            - pages_crawled: Number of successfully crawled pages
    """
    prune_filter = PruningContentFilter(
        threshold=PRUNE_FILTER_THRESHOLD,
        min_word_threshold=PRUNE_FILTER_MIN_WORDS,
        threshold_type="fixed",
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
        delay_before_return_html=1.0,
        word_count_threshold=CRAWL_WORD_COUNT_THRESHOLD['main'],
        #magic=True
    )
    
    # Create crawler configuration for internal links - body only
    body_only_config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=CRAWL_WORD_COUNT_THRESHOLD['body'],
        delay_before_return_html=1.0,
        #magic=True,
        #remove_forms=True,
        # Only extract the main content body
        excluded_tags=EXCLUDED_TAGS,
        excluded_selector="img",
        markdown_generator= DefaultMarkdownGenerator(
            content_filter=prune_filter,
        )
    )
    
    try:
        from crawl4ai import DisplayMode # display mode can crash the program on background run
        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=DEFAULT_MEMORY_THRESHOLD,
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
        sys.setrecursionlimit(RECURSION_LIMIT)  # Increase the recursion limit
        
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            # Set German language header
            crawler.crawler_strategy.set_custom_headers(GERMAN_LANGUAGE_HEADERS)
            
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
            if len(aggregate_content) > LARGE_CONTENT_THRESHOLD:  # 100KB threshold
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

async def main() -> None:
    """
    Main function to initiate the web crawling process.
    
    This function:
    1. Parses command line arguments
    2. Reads URLs and company names from an Excel file
    3. Creates the output directory
    4. Crawls each domain to collect and save content
    5. Outputs a summary of results
    
    Command Line Arguments:
        --excel (-e): Path to Excel file containing URLs and company names
        --output (-o): Directory where the output files will be saved
        --max-links: Maximum number of links to crawl per domain (default: 60)
    
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
                ("https://alles-in-metall.de/de/", "a.i.m. all in metal GmbH"),
                
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
