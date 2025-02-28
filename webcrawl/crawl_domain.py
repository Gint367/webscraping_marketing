import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse, urljoin
import re
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerMonitor, CrawlerRunConfig, CacheMode, MemoryAdaptiveDispatcher, PruningContentFilter

def sanitize_filename(url):
    """Convert URL to a valid filename by removing scheme and replacing invalid characters"""
    parsed = urlparse(url)
    domain = parsed.netloc
    sanitized = re.sub(r'[\\/*?:"<>|]', '_', domain)
    return sanitized

def ensure_output_directory(directory):
    """Ensure the output directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

async def collect_internal_links(crawler, main_url, max_links=50):
    """Collect all internal links from the main URL up to max_links"""
    print(f"Collecting internal links from {main_url}...")
    
    # Configure crawler for link collection
    crawl_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
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
    
    # Common non-content pages to filter out (German and English)
    non_content_keywords = [
        'login', 'signup', 'register', 'impressum', 'datenschutz', 
        'kontakt', 'contact', 'imprint', 'privacy', 'agb', 'cart', 
        'warenkorb', 'checkout', 'search', 'suche', 'sitemap',
        'anmelden', 'registrieren', 'einkaufswagen', 'newsletter',
        'terms', 'conditions', 'faq', 'hilfe', 'help', 'support',
        'nutzungsbedingungen', 'cookie'
    ]
    
    absolute_links = []
    filtered_count = 0
    
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
        
        # Check for language codes in the path
        lang_match = re.search(r'/([a-zA-Z]{2})(-[a-zA-Z]{2})?/', path)
        if lang_match and not is_base_domain:
            lang_code = lang_match.group(1).lower()
            # Keep German language codes, skip others
            if lang_code != 'de':
                print(f"Skipping non-German language URL: {absolute_link} (detected: {lang_code})")
                filtered_count += 1
                continue
        
        absolute_links.append(absolute_link)
    
    print(f"Filtered out {filtered_count} non-content or non-German pages")
    return absolute_links

async def crawl_domain(main_url, output_dir_aggregated="domain_content_aggregated", output_dir_pruned="domain_content_pruned", max_links=50):
    """
    Crawl a main URL and all its internal links, then aggregate the content.
    Uses a two-phase approach:
    1. First crawl the main URL completely
    2. Then crawl the rest of the internal links (body content only)
    """
    
    # Create browser configuration
    browser_cfg = BrowserConfig(
        verbose=True,
        headless=True,
        text_mode=True,
    )
    
    # Create crawler configuration for main URL - complete crawl
    main_crawl_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=10
    )
    
    # Create crawler configuration for internal links - body only
    body_only_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        only_text=True,
        exclude_external_links=True,
        exclude_social_media_links=True,
        word_count_threshold=10,
        excluded_tags=['form', 'header', 'footer', 'img'],  # Only extract the main content body
    )
    
    try:
        from crawl4ai import DisplayMode
        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=70.0,
            check_interval=1,
            max_session_permit=10,
            monitor=CrawlerMonitor(
                display_mode=DisplayMode.DETAILED
            )
        )
    except ImportError:
        dispatcher = None
    
    # Ensure both output directories exist
    ensure_output_directory(output_dir_aggregated)
    ensure_output_directory(output_dir_pruned)
    
    # Get domain name for the output file
    domain_name = sanitize_filename(main_url)
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Create output filenames in their respective directories
    output_markdown_file = os.path.join(output_dir_aggregated, f"{domain_name}_{current_date}.md")
    output_pruned_file = os.path.join(output_dir_pruned, f"{domain_name}_{current_date}.md")
    
    # Initialize aggregate content
    aggregate_content = f"# Aggregated Content for {domain_name}\n\n"
    aggregate_content += f"Main URL: {main_url}\n"
    aggregate_content += f"Crawled on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Set German language header
        crawler.crawler_strategy.set_custom_headers(
            {"Accept-Language": "de-DE,de;q=0.9"}
        )
        
        # Phase 1: Crawl the main URL and collect internal links
        print(f"\n=== Phase 1: Crawling main URL: {main_url} ===\n")
        main_result = await crawler.arun(main_url, config=main_crawl_config)
        
        if not main_result.success:
            print(f"Failed to crawl main URL: {main_url}")
            print(f"Error: {main_result.error if hasattr(main_result, 'error') else 'Unknown error'}")
            return output_markdown_file, output_pruned_file, 0
        
        # Process main URL result
        aggregate_content += f"## Main Page: {main_url}\n\n"
        aggregate_content += f"### Title: {main_result.metadata.get('title', 'Untitled')}\n\n"
        
        # Add the raw content
        aggregate_content += "### Content:\n\n"
        page_content = main_result.markdown[:5000] + "...\n\n" if len(main_result.markdown) > 5000 else main_result.markdown + "\n\n"
        aggregate_content += page_content
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
                results = await crawler.arun_many(
                    internal_links[:10],
                    config=body_only_config,
                    verbose=True,
                    dispatcher=dispatcher
                )
            else:
                results = await crawler.arun_many(
                    internal_links[:10],
                    config=body_only_config,
                    verbose=True
                )
            
            # Process results
            for i, result in enumerate(results):
                url = result.url if hasattr(result, 'url') else internal_links[i]
                
                if result.success:
                    # Add the page content to our aggregate
                    aggregate_content += f"## Page {i+1}: {url}\n\n"
                    aggregate_content += f"### Title: {result.metadata.get('title', 'Untitled')}\n\n"
                    
                    # Add the raw content
                    aggregate_content += "### Content (body only):\n\n"
                    page_content = result.markdown[:5000] + "...\n\n" if len(result.markdown) > 5000 else result.markdown + "\n\n"
                    aggregate_content += page_content
                    aggregate_content += "-" * 80 + "\n\n"
                    
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
    
    # Write the aggregate content to file
    with open(output_markdown_file, "w", encoding="utf-8") as f:
        f.write(aggregate_content)
    print(f"Aggregate content saved to {output_markdown_file}")
    
    # Apply pruning to the collected content
    print("Applying content pruning...")
    pruning_filter = PruningContentFilter(threshold=0.5, min_word_threshold=50, threshold_type="dynamic")
    
    # Pruning works directly on the aggregate content
    pruned_chunks = pruning_filter.filter_content(aggregate_content)
                
    # Combine the pruned chunks into a single document without metadata
    pruned_content = f"# Pruned Content for {domain_name}\n\n"
    
    if pruned_chunks:
        for chunk in pruned_chunks:
            pruned_content += chunk + "\n\n"
    else:
        pruned_content += "No significant content found after pruning.\n"
        
    print(f"Raw content length: {len(aggregate_content)}")
    print(f"Pruned content length: {len(pruned_content)}")
    
    # Write the pruned content to file
    with open(output_pruned_file, "w", encoding="utf-8") as f:
        f.write(pruned_content)
    print(f"Pruned content saved to {output_pruned_file}")
    
    # Count total pages crawled (main URL + internal links that were successfully crawled)
    total_crawled = 1 + len([r for r in results if getattr(r, 'success', False)]) if 'results' in locals() else 1
    
    return output_markdown_file, output_pruned_file, total_crawled

async def main():
    # List of domains to crawl
    domains = [
        "https://www.forst-online.de",
    ]
    """
        "https://www.schroedergroup.eu/de/",
        "https://huettenbrauck.com"
    """
    # Create output directories
    output_dir_aggregated = "domain_content_aggregated"
    output_dir_pruned = "domain_content_pruned"
    
    # Crawl each domain
    results = []
    for domain in domains:
        print(f"\n{'='*40}\nStarting crawl of domain: {domain}\n{'='*40}\n")
        markdown_file, pruned_file, page_count = await crawl_domain(
            domain, 
            output_dir_aggregated=output_dir_aggregated, 
            output_dir_pruned=output_dir_pruned
        )
        results.append({
            "domain": domain,
            "markdown_file": markdown_file,
            "pruned_file": pruned_file,
            "pages_crawled": page_count
        })
    
    # Summary of results
    print("\n\n" + "="*40)
    print("CRAWL SUMMARY")
    print("="*40)
    for result in results:
        print(f"Domain: {result['domain']}")
        print(f"Pages crawled: {result['pages_crawled']}")
        print(f"Markdown file: {os.path.basename(result['markdown_file'])}")
        print(f"Pruned file: {os.path.basename(result['pruned_file'])}")
        print("-"*40)

if __name__ == "__main__":
    asyncio.run(main())
