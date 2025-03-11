import json
import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonXPathExtractionStrategy

async def extract_financial_data():
    # 1. Sample HTML with financial statement table
    # You can replace this with reading from an actual HTML file
    with open('Jahresabschluss_zum_Geschaeftsjahr_vom_01.01.2020_bis_zum_31.12.2020_raw_report.html', 'r') as file:
        html_content = file.read()

    # 2. Define the schema for XPath extraction
    schema = {
        "name": "Financial Statement Table",
        "baseSelector": "//table[@class='std_table']",
        "fields": [
            {
                "name": "sachanlagen",
                "selector": ".//tbody/tr[2]",
                "type": "nested",
                "fields": [
                    {
                        "name": "date_1",
                        "selector": "./td[2]",
                        "type": "text"
                    },
                    {
                        "name": "date_2",
                        "selector": "./td[3]",
                        "type": "text"
                    }
                ]
            },
            {
                "name": "technische_anlagen_und_maschinen",
                "selector": ".//tbody/tr[5]",
                "type": "nested",
                "fields": [
                    {
                        "name": "date_1",
                        "selector": "./td[2]",
                        "type": "text"
                    },
                    {
                        "name": "date_2",
                        "selector": "./td[3]",
                        "type": "text"
                    }
                ]
            }
        ]
    }

    # 3. Configure the crawler with the extraction strategy
    config = CrawlerRunConfig(
        extraction_strategy=JsonXPathExtractionStrategy(schema, verbose=True)
    )

    # 4. Use raw:// scheme to pass HTML content directly
    raw_url = f"raw://{html_content}"

    # 5. Create and run the crawler
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=raw_url,
            config=config
        )

        if not result.success:
            print("Crawl failed:", result.error_message)
            return

        # 6. Process and display the extracted data
        data = json.loads(result.extracted_content)
        
        # Print the whole extracted data structure
        print("Extracted Financial Data:")
        print(json.dumps(data, indent=2))
        

# Run the extraction function
if __name__ == "__main__":
    asyncio.run(extract_financial_data())
