# Crawl4AI - AsyncWebCrawler Documentation

## 1. Overview

`AsyncWebCrawler` is the core class for asynchronous web crawling in Crawl4AI. It provides powerful features for web scraping with modern browsers.

## 2. Installation

```python
pip install crawl4ai
```

## 3. Basic Usage

### Constructor

```python
from crawl4ai import AsyncWebCrawler, BrowserConfig

browser_cfg = BrowserConfig(
    browser_type="chromium",  # Options: chromium, firefox
    headless=True,            # Run browser invisibly
    verbose=True              # Show detailed logs
)

crawler = AsyncWebCrawler(config=browser_cfg)
```

### Context Manager (Recommended)

```python
import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig

async def main():
    browser_cfg = BrowserConfig(browser_type="chromium", headless=True)
    
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun("https://example.com")
        print("Cleaned HTML length:", len(result.cleaned_html))

asyncio.run(main())
```

### Manual Start & Close

```python
crawler = AsyncWebCrawler(config=browser_cfg)
await crawler.start()

result1 = await crawler.arun("https://example.com")
result2 = await crawler.arun("https://another.com")

await crawler.close()
```

## 4. Configuration Objects

### BrowserConfig

Controls browser behavior:

```python
browser_cfg = BrowserConfig(
    browser_type="chromium",  # or "firefox" 
    headless=True,            # True for invisible browser
    verbose=True,             # Show detailed logs
    # Many other options available
)
```

### CrawlerRunConfig

Controls per-crawl behavior:

```python
from crawl4ai import CrawlerRunConfig, CacheMode

run_cfg = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,      # Skip cache for fresh content
    css_selector="main.article",      # Target specific content
    word_count_threshold=10,          # Filter short content
    screenshot=True,                  # Capture page screenshot
    remove_overlay_elements=True,     # Remove cookie banners, etc.
    wait_for="css:.my-content"        # Wait for element to appear
)
```

## 5. Core Methods

### arun() - Single URL Processing

```python
result = await crawler.arun(
    url="https://example.com",
    config=run_cfg  # CrawlerRunConfig object
)

if result.success:
    print("Content length:", len(result.cleaned_html))
    print("Final URL after redirects:", result.url)
else:
    print("Error:", result.error_message)
```

### arun_many() - Batch Processing

Process multiple URLs efficiently:

```python
urls = ["https://example.com/1", "https://example.com/2", "https://example.com/3"]
results = await crawler.arun_many(urls, config=run_cfg)

for result in results:
    if result.success:
        print(f"Crawled {result.url}: {len(result.cleaned_html)} chars")
    else:
        print(f"Failed {result.url}: {result.error_message}")
```

Features of `arun_many()`:
- Intelligent rate limiting
- Memory usage monitoring
- Domain-specific throttling
- Progress reporting
- Automatic retries with backoff

## 5.1 arun_many() Return Type and Iteration Notes

`arun_many()` can return two types depending on the configuration:

- **Batch (default, non-streaming) mode:**
  - Returns a `List[CrawlResult]`.
  - Usage:
    ```python
    results = await crawler.arun_many(urls, config=run_cfg)
    for result in results:  # type: ignore
        ...
    ```
  - If your linter or type checker complains about the type, you can safely ignore the warning with `# type: ignore` because this is the intended usage for non-streaming mode.

- **Streaming mode (`stream=True` in config):**
  - Returns an `AsyncGenerator[CrawlResult, None]`.
  - Usage:
    ```python
    async for result in await crawler.arun_many(urls, config=run_cfg):
        ...
    ```

**Note:**
If you see a type error when iterating over the results of `arun_many()` in batch mode, verify that `stream` is not set to `True` in your `CrawlerRunConfig`. If so, you can safely use a regular `for` loop and add `# type: ignore` to suppress the warning.

## 6. CrawlResult Object

Each `arun()` call returns a `CrawlResult` with:

- `url`: Final URL (after redirects)
- `html`: Original HTML
- `cleaned_html`: Sanitized HTML
- `markdown`: Converted markdown content
- `extracted_content`: JSON data (if using extraction strategies)
- `screenshot`: Base64 encoded screenshot (if requested)
- `pdf`: PDF version of page (if requested)
- `media`: Information about discovered images
- `links`: Information about discovered URLs
- `success`: Boolean indicating success
- `error_message`: Error details (if any)

## 7. Content Extraction Strategies

Extract structured data using strategies:

```python
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
import json

# Define extraction schema
schema = {
    "name": "Articles",
    "baseSelector": "article.post",
    "fields": [
        {
            "name": "title", 
            "selector": "h2", 
            "type": "text"
        },
        {
            "name": "url", 
            "selector": "a", 
            "type": "attribute", 
            "attribute": "href"
        }
    ]
}

run_cfg = CrawlerRunConfig(
    extraction_strategy=JsonCssExtractionStrategy(schema),
    word_count_threshold=15
)

async with AsyncWebCrawler(config=browser_cfg) as crawler:
    result = await crawler.arun("https://example.com/blog", config=run_cfg)
    
    if result.success and result.extracted_content:
        articles = json.loads(result.extracted_content)
        print("Extracted articles:", articles[:2])
```

## 8. Advanced Features

- **Rate limiting**: Automatic delay between requests
- **Resource monitoring**: Memory usage tracking
- **Session management**: Cookie handling
- **File downloading**: Save assets from pages
- **Proxy support**: Use proxy servers
- **Authentication**: Handle login flows
- **Page interaction**: Fill forms, click buttons
- **Wait conditions**: Wait for specific page events

## 9. Best Practices

1. Use `BrowserConfig` for global browser settings
2. Use `CrawlerRunConfig` for per-request settings
3. Prefer context manager (`async with`) for resource cleanup
4. Use `arun_many()` for batch processing
5. Set appropriate wait conditions for dynamic content
6. Monitor resource usage for large crawls
7. Implement rate limiting to avoid IP bans

## 10. Migration Notes

If you're using older versions of Crawl4AI:

- Move browser settings from constructor to `BrowserConfig`
- Move crawl parameters from `arun()` to `CrawlerRunConfig`
- Replace deprecated `always_bypass_cache` with `CacheMode`
- Use `markdown` instead of deprecated `markdown_v2`

## 11. Crawl4AI v0.5.0 Features and Advanced Usage

### 11.1 Deep Crawling

Crawl4AI v0.5.0 introduces powerful deep crawling capabilities, allowing you to explore websites beyond the initial URLs:

```python
import time
import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BFSDeepCrawlStrategy
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import (
    DomainFilter, ContentTypeFilter, FilterChain, 
    URLPatternFilter, KeywordRelevanceScorer, BestFirstCrawlingStrategy
)

# Create a filter chain to filter URLs
filter_chain = FilterChain(
    [
        DomainFilter(
            allowed_domains=["example.com"],
            blocked_domains=["ads.example.com"],
        ),
        URLPatternFilter(patterns=["*product*", "*blog*"],),
        # It is possible to create inverse URL filter(blacklist) with URLPatternFilter
        URLPatternFilter(reverse=True, patterns=["*login*","*register*"])
        ContentTypeFilter(allowed_types=["text/html"]),
    ]
)


# Create a keyword scorer for prioritization
keyword_scorer = KeywordRelevanceScorer(
    keywords=["product", "review", "feature"], weight=0.7
)

# Configure deep crawling
deep_crawl_config = CrawlerRunConfig(
    deep_crawl_strategy=BestFirstCrawlingStrategy(
        max_depth=2,  # How deep to crawl
        include_external=False,  # Don't follow external links
        filter_chain=filter_chain,
        url_scorer=keyword_scorer,
    ),
    scraping_strategy=LXMLWebScrapingStrategy(),
    stream=True,
    verbose=True,
)

async def main():
    async with AsyncWebCrawler() as crawler:
        start_time = time.perf_counter()
        results = []
        async for result in await crawler.arun(url="https://example.com", config=deep_crawl_config):
            print(f"Crawled: {result.url} (Depth: {result.metadata['depth']}), score: {result.metadata['score']:.2f}")
            results.append(result)
        duration = time.perf_counter() - start_time
        print(f"\nCrawled {len(results)} pages in {duration:.2f} seconds")

asyncio.run(main())
```

Available deep crawling strategies:
- `BFSDeepCrawlStrategy` (Breadth-First Search): Explores the website level by level
- `DFSDeepCrawlStrategy` (Depth-First Search): Explores each branch deeply before backtracking
- `BestFirstCrawlingStrategy`: Uses a scoring function to prioritize URLs

### 11.2 Memory-Adaptive Dispatcher

Crawl4AI v0.5.0 includes a new `MemoryAdaptiveDispatcher` that dynamically adjusts concurrency based on available system memory and includes built-in rate limiting:

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, MemoryAdaptiveDispatcher
import asyncio

# Configure the dispatcher with custom settings
dispatcher = MemoryAdaptiveDispatcher(
    memory_threshold_percent=80.0,  # Pause if memory usage exceeds 80%
    check_interval=0.5,  # Check memory every 0.5 seconds
)

async def batch_crawl():
    async with AsyncWebCrawler() as crawler:
        # Batch mode
        results = await crawler.arun_many(
            urls=["https://example.com", "https://example.org"],
            config=CrawlerRunConfig(stream=False),
            dispatcher=dispatcher,
        )
        for result in results:
            print(f"Crawled: {result.url} with status code: {result.status_code}")

async def stream_crawl():
    async with AsyncWebCrawler() as crawler:
        # Streaming mode
        async for result in await crawler.arun_many(
            urls=["https://example.com", "https://example.org"],
            config=CrawlerRunConfig(stream=True),
            dispatcher=dispatcher,
        ):
            print(f"Crawled: {result.url} with status code: {result.status_code}")

# Run one of these functions
asyncio.run(batch_crawl())
```

Note: `AsyncWebCrawler.arun_many()` now uses `MemoryAdaptiveDispatcher` by default.

### 11.3 Multiple Crawling Strategies

Crawl4AI v0.5.0 offers two crawling strategies:

1. **`AsyncPlaywrightCrawlerStrategy`** (Default): Uses Playwright for browser-based crawling with JavaScript rendering
2. **`AsyncHTTPCrawlerStrategy`**: A lightweight, fast HTTP-only crawler for simple scraping tasks

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, HTTPCrawlerConfig
from crawl4ai.async_crawler_strategy import AsyncHTTPCrawlerStrategy
import asyncio

# Configure the HTTP crawler
http_crawler_config = HTTPCrawlerConfig(
    method="GET",
    headers={"User-Agent": "MyCustomBot/1.0"},
    follow_redirects=True,
    verify_ssl=True
)

async def main():
    # Use the HTTP crawler strategy
    async with AsyncWebCrawler(crawler_strategy=AsyncHTTPCrawlerStrategy(browser_config=http_crawler_config)) as crawler:
        result = await crawler.arun("https://example.com")
        print(f"Status code: {result.status_code}")
        print(f"Content length: {len(result.html)}")

asyncio.run(main())
```

### 11.4 Docker Deployment

Crawl4AI v0.5.0 can be deployed as a Docker container with FastAPI endpoints:

```bash
# Build the image (from the project root)
docker build -t crawl4ai .

# Run the container
docker run -d -p 8000:8000 --name crawl4ai crawl4ai
```

API Endpoints:
- `/crawl` (POST): Non-streaming crawl
- `/crawl/stream` (POST): Streaming crawl (NDJSON)
- `/health` (GET): Health check
- `/schema` (GET): Returns configuration schemas
- `/md/{url}` (GET): Returns markdown content of the URL
- `/llm/{url}` (GET): Returns LLM extracted content
- `/token` (POST): Get JWT token

Requirements:
- `.llm.env` file for API keys
- Redis configuration
- Authentication setup (JWT tokens)

### 11.5 Command-Line Interface

Crawl4AI v0.5.0 includes a command-line interface (CLI) called `crwl`:

```bash
# Basic crawl
crwl https://example.com

# Get markdown output
crwl https://example.com -o markdown

# Use a configuration file
crwl https://example.com -B browser.yml -C crawler.yml

# Use LLM-based extraction
crwl https://example.com -e extract.yml -s schema.json

# Ask a question about the crawled content
crwl https://example.com -q "What is the main topic?"
```

### 11.6 LXML Scraping Mode

For faster HTML parsing, especially with large pages, use the `LXMLWebScrapingStrategy`:

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
import asyncio

async def main():
    config = CrawlerRunConfig(
        scraping_strategy=LXMLWebScrapingStrategy()
    )
    
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://example.com", config=config)
        print(f"Content processed in: {result.metadata.get('processing_time')}ms")

asyncio.run(main())
```

### 11.7 Proxy Rotation

Rotate through multiple proxies to avoid IP bans and improve crawling reliability:

```python
import re
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    RoundRobinProxyStrategy,
)
from crawl4ai.proxy_strategy import ProxyConfig
import asyncio

async def main():
    # Load proxies from environment variable
    # export PROXIES="ip1:port1:username1:password1,ip2:port2:username2:password2"
    proxies = ProxyConfig.from_env()
    if not proxies:
        print("No proxies found in environment. Set PROXIES env variable!")
        return

    proxy_strategy = RoundRobinProxyStrategy(proxies)
    
    browser_config = BrowserConfig(headless=True, verbose=False)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        proxy_rotation_strategy=proxy_strategy
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        urls = ["https://httpbin.org/ip"] * 3  # Test with multiple requests
        
        results = await crawler.arun_many(
            urls=urls,
            config=run_config
        )
        
        for result in results:
            if result.success:
                print(f"URL {result.url}")
                print(f"Using proxy: {result.metadata.get('proxy', 'None')}")
                print(f"Content: {result.html[:100]}...")

asyncio.run(main())
```

### 11.8 LLM-Powered Content Processing

Use LLMs for content filtering and extraction:

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator, LLMConfig
from crawl4ai.content_filter_strategy import LLMContentFilter
import asyncio

# Configure LLM integration
llm_config = LLMConfig(
    provider="gemini/gemini-1.5-pro", 
    api_token="env:GEMINI_API_KEY"  # Uses GEMINI_API_KEY environment variable
)

# Create content filter
markdown_generator = DefaultMarkdownGenerator(
    content_filter=LLMContentFilter(
        llm_config=llm_config, 
        instruction="Extract key concepts and summaries"
    )
)

config = CrawlerRunConfig(markdown_generator=markdown_generator)

async def main():
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://example.com", config=config)
        print(result.markdown.fit_markdown)  # AI-filtered markdown content

asyncio.run(main())
```

### 11.9 Automated Schema Generation

Generate extraction schemas automatically using LLMs:

```python
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai import LLMConfig, AsyncWebCrawler, CrawlerRunConfig
import asyncio

async def main():
    # Configure LLM
    llm_config = LLMConfig(
        provider="gemini/gemini-1.5-pro", 
        api_token="env:GEMINI_API_KEY"
    )
    
    # First, crawl a page to get HTML
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://example.com/products")
        
        if result.success:
            # Generate extraction schema from HTML
            schema = await JsonCssExtractionStrategy.generate_schema(
                html=result.html,
                llm_config=llm_config,
                query="Extract product names and prices"
            )
            
            print("Generated Schema:", schema)
            
            # Now use this schema for extraction
            extraction_strategy = JsonCssExtractionStrategy(schema)
            
            # Crawl again with the extraction strategy
            result = await crawler.arun(
                "https://example.com/products",
                config=CrawlerRunConfig(extraction_strategy=extraction_strategy)
            )
            
            if result.extracted_content:
                print("Extracted data:", result.extracted_content)

asyncio.run(main())
```

### 11.10 PDF Processing

Extract content from PDF files (both local and remote):

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.processors.pdf import PDFCrawlerStrategy, PDFContentScrapingStrategy
import asyncio

async def main():
    # Configure PDF crawler
    async with AsyncWebCrawler(crawler_strategy=PDFCrawlerStrategy()) as crawler:
        # Process a remote PDF
        remote_result = await crawler.arun(
            "https://example.com/document.pdf",
            config=CrawlerRunConfig(
                scraping_strategy=PDFContentScrapingStrategy()
            )
        )
        
        print("PDF Title:", remote_result.metadata.get("title"))
        print("PDF Author:", remote_result.metadata.get("author"))
        print("PDF Content Length:", len(remote_result.markdown))
        
        # Process a local PDF
        local_result = await crawler.arun(
            "file:///path/to/local/document.pdf",
            config=CrawlerRunConfig(
                scraping_strategy=PDFContentScrapingStrategy()
            )
        )
        
        print("Local PDF Content:", local_result.markdown[:500])

asyncio.run(main())
```

### 11.11 Robots.txt Compliance

Respect robots.txt rules for ethical crawling:

```python
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
import asyncio

async def main():
    # Enable robots.txt checking
    config = CrawlerRunConfig(check_robots_txt=True)
    
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://example.com", config=config)
        
        if result.success:
            print("Successfully crawled (allowed by robots.txt)")
        else:
            print(f"Failed to crawl: {result.error_message}")

asyncio.run(main())
```

## 12. Migration Guide for v0.5.0

If you're upgrading to Crawl4AI v0.5.0 from an earlier version, here are the key changes to be aware of:

1. **Deep Crawling**: Import paths for deep crawling strategies have changed, and `max_depth` is now part of `CrawlerRunConfig`
2. **Dispatcher**: `MemoryAdaptiveDispatcher` is now the default for `arun_many()`
3. **Browser Context**: The `BrowserContext` API has been updated
4. **Models**: Many fields in data models are now optional with default values
5. **Scraping Mode**: `ScrapingMode` enum replaced by strategy pattern (`WebScrapingStrategy`, `LXMLWebScrapingStrategy`)
6. **Content Filter**: Use extraction strategies or markdown generators with filters instead
7. **Renamed**: `final_url` to `redirected_url` for consistency
8. **Config**: `FastFilterChain` has been replaced with `FilterChain`

Update your import statements and adapt your code to use the new parameter locations and class names.
