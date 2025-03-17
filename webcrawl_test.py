import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    DomainFilter,
    URLPatternFilter,
    ContentTypeFilter
)
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer

async def run_advanced_crawler():
    # Create a sophisticated filter chain
    filter_chain = FilterChain([

        # Content type filtering
        ContentTypeFilter(allowed_types=["text/html"])
    ])

    # Create a relevance scorer
    keyword_scorer = KeywordRelevanceScorer(
        keywords=[
            # ✅ Products (Produkte)
            "CNC-Maschine", "3D-Druck", "Fräsmaschine", "Laserschneiden",
            "Schweißtechnik", "Spritzgussform", "Gussteile", "Bearbeitungszentrum",
            "Drehteile", "Frästeile", "Blechprodukte", "Kunststoffteile",
            "Metallbauteile", "Stanzteile",

            # ✅ Machines (Maschinen)
            "Roboterarm", "Drehmaschine", "Stanzmaschine", "Schweißroboter",
            "Sondermaschinenbau", "Biegemaschine", "Pressmaschine",
            "Automatisierungstechnik", "Industrieroboter", "Montageroboter",

            # ✅ Production Processes (Produktionsprozesse)
            "Metallverarbeitung", "Kunststoffverarbeitung", "Zerspanungstechnik",
            "Blechbearbeitung", "Gießverfahren", "CNC-Bearbeitung",
            "Montageprozess", "Fertigungstechnik", "Industrielle Automatisierung",
            "Baugruppenfertigung", "Additive Fertigung", "Laserschweißen",
            "Präzisionsfertigung", "Werkzeugbau",

            # ✅ Contract Manufacturing (Lohnfertigung & OEM)
            "Lohnfertigung", "OEM-Produktion", "Auftragsfertigung",
            "Serienfertigung", "Auftragsbearbeitung", "Baugruppenmontage",
            "Fertigungspartner", "Zulieferproduktion", "Kleinserienfertigung",
            "Großserienfertigung", "Outsourcing", "Fertigungsdienstleister"
        ],
        weight=0.8  # Adjust the weight based on relevance priority
        )

    # Set up the configuration
    config = CrawlerRunConfig(
        deep_crawl_strategy=BestFirstCrawlingStrategy(
            max_depth=2,
            include_external=False,
            filter_chain=filter_chain,
            url_scorer=keyword_scorer
        ),
        scraping_strategy=LXMLWebScrapingStrategy(),
        stream=True,
        verbose=True
    )

    # Execute the crawl
    results = []
    async with AsyncWebCrawler() as crawler:
        async for result in await crawler.arun("https://www.profex-gruppe.de/", config=config):
            results.append(result)
            score = result.metadata.get("score", 0)
            depth = result.metadata.get("depth", 0)
            print(f"Depth: {depth} | Score: {score:.2f} | {result.url}")

    # Analyze the results
    print(f"Crawled {len(results)} high-value pages")
    print(f"Average score: {sum(r.metadata.get('score', 0) for r in results) / len(results):.2f}")

    # Group by depth
    depth_counts = {}
    for result in results:
        depth = result.metadata.get("depth", 0)
        depth_counts[depth] = depth_counts.get(depth, 0) + 1

    print("Pages crawled by depth:")
    for depth, count in sorted(depth_counts.items()):
        print(f"  Depth {depth}: {count} pages")

if __name__ == "__main__":
    asyncio.run(run_advanced_crawler())