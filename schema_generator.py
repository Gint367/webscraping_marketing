from crawl4ai import JsonXPathExtractionStrategy
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.async_configs import LLMConfig

llm_config = LLMConfig(provider="gemini/gemini-1.5-pro", api_token="env:GEMINI_API_KEY")

with open('sample_html.html', 'r') as file:
    html_content = file.read()

schema_1 = JsonCssExtractionStrategy.generate_schema(
    html=html_content,
    llm_config=llm_config,
    query="extract the numbers in the rows 'technische Anlagen und Maschinen' and sachanlagen from the given table of finance statement",
)

schema_2 = JsonXPathExtractionStrategy.generate_schema(
    html=html_content,
    llm_config=llm_config,
    query="extract the numbers in the rows 'technische Anlagen und Maschinen' and sachanlagen from the given table of finance statement",
)
print(schema_1)
print("===================================")
print(schema_2)