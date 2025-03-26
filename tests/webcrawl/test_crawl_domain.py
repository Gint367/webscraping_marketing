import unittest
from unittest.mock import patch, MagicMock

from webcrawl.crawl_domain import (
    sanitize_filename,
    ensure_output_directory,
    get_path_depth,
    filter_urls_by_depth_reverse,
    filter_urls_by_depth,
    is_non_content_url,
    is_file_url,
    should_filter_by_language,
    normalize_and_filter_links,
    apply_content_filters,
    remove_duplicate_urls,
    remove_links_from_markdown,
    crawl_domain
)

class TestCrawlDomain(unittest.TestCase):

    def test_sanitize_filename(self):
        url = "https://www.example.com/path/to/page"
        expected = "example.com"
        self.assertEqual(sanitize_filename(url), expected)

    def test_ensure_output_directory(self):
        with patch("os.makedirs") as mock_makedirs:
            directory = "/path/to/output"
            ensure_output_directory(directory)
            mock_makedirs.assert_called_once_with(directory)

    def test_get_path_depth(self):
        url = "https://www.example.com/path/to/page"
        expected_depth = 3
        self.assertEqual(get_path_depth(url), expected_depth)

    def test_filter_urls_by_depth_reverse(self):
        urls = [
            "https://www.example.com/",
            "https://www.example.com/about",
            "https://www.example.com/about/team",
            "https://www.example.com/about/team/members"
        ]
        expected = ["https://www.example.com/about/team"]
        self.assertEqual(filter_urls_by_depth_reverse(urls), expected)

    def test_filter_urls_by_depth(self):
        urls = [
            "https://www.example.com/",
            "https://www.example.com/about",
            "https://www.example.com/about/team",
            "https://www.example.com/about/team/members"
        ]
        expected = [
            "https://www.example.com/",
            "https://www.example.com/about",
            "https://www.example.com/about/team"
        ]
        self.assertEqual(filter_urls_by_depth(urls, 2), expected)

    def test_is_non_content_url(self):
        url_path = "/login"
        self.assertTrue(is_non_content_url(url_path))

    def test_is_file_url(self):
        url_path = "/path/to/file.pdf"
        self.assertTrue(is_file_url(url_path))

    def test_should_filter_by_language(self):
        url = "https://www.example.com/en/about"
        self.assertTrue(should_filter_by_language(url, True, False))

    def test_normalize_and_filter_links(self):
        internal_links = ["about", "contact", "#section"]
        base_url = "https://www.example.com"
        max_links = 10
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_apply_content_filters(self):
        urls = ["https://www.example.com/about", "https://www.example.com/en/products", "https://www.example.com/contact"]
        base_url = "https://www.example.com"
        expected_urls = ["https://www.example.com/about"]
        filtered_urls, filter_counts = apply_content_filters(urls, base_url)
        self.assertEqual(filtered_urls, expected_urls)
        self.assertEqual(filter_counts['content_filtered'], 1)
        self.assertEqual(filter_counts['language_filtered'], 1)

    def test_remove_duplicate_urls(self):
        urls = [
            "https://www.example.com/about",
            "https://www.example.com/about?query=1",
            "https://www.example.com/contact"
        ]
        expected = [
            "https://www.example.com/about",
            "https://www.example.com/contact"
        ]
        self.assertEqual(remove_duplicate_urls(urls), expected)

    @patch("webcrawl.crawl_domain.AsyncWebCrawler")
    @patch("webcrawl.crawl_domain.CrawlerRunConfig")
    @patch("webcrawl.crawl_domain.collect_internal_links")
    async def test_crawl_domain(self, mock_collect_internal_links, mock_CrawlerRunConfig, mock_AsyncWebCrawler):
        mock_crawler_instance = MagicMock()
        mock_AsyncWebCrawler.return_value.__aenter__.return_value = mock_crawler_instance
        mock_collect_internal_links.return_value = ["https://www.example.com/about"]

        main_url = "https://www.example.com"
        output_dir_aggregated = "domain_content_aggregated"
        max_links = 10
        company_name = "Example Company"

        output_file, pages_crawled = await crawl_domain(main_url, output_dir_aggregated, max_links, company_name)

        self.assertEqual(pages_crawled, 2)
        self.assertTrue(output_file.endswith("example_com.md"))

    def test_remove_links_from_markdown(self):
        markdown_text = "[link text](https://example.com) and ![alt text](image_url)"
        expected = "link text and "
        self.assertEqual(remove_links_from_markdown(markdown_text), expected)

if __name__ == "__main__":
    unittest.main()