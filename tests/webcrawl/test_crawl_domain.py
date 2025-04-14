import unittest
import sys
from unittest.mock import patch, MagicMock

sys.modules['excel_reader'] = MagicMock()
sys.modules['get_company_by_category'] = MagicMock()
sys.modules['get_company_by_top1machine'] = MagicMock()

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
    
    def test_should_filter_by_language_non_german(self):
        # Test non-German language patterns
        url = "https://www.example.com/en/about"
        uses_language_codes = True
        is_base_domain = False
        self.assertTrue(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_other_lang_codes(self):
        # Test non-German language code pattern
        url = "https://www.example.com/fr/about"
        uses_language_codes = True
        is_base_domain = False
        self.assertTrue(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_generic_lang_code(self):
        # Test generic language code pattern without explicit language marker
        url = "https://www.example.com/es-mx/about"
        uses_language_codes = True
        is_base_domain = False
        self.assertTrue(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_german(self):
        # Test German language pattern
        url = "https://www.example.com/de/about"
        uses_language_codes = True
        is_base_domain = False
        self.assertFalse(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_no_lang_code(self):
        # Test URL without language code
        url = "https://www.example.com/about"
        uses_language_codes = True
        is_base_domain = False
        self.assertFalse(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_base_domain(self):
        # Test base domain URL
        url = "https://www.example.com/"
        uses_language_codes = True
        is_base_domain = True
        self.assertFalse(should_filter_by_language(url, uses_language_codes, is_base_domain))
    
    def test_should_filter_by_language_site_without_lang_codes(self):
        # Test site that doesn't use language codes
        url = "https://www.example.com/products/software"
        uses_language_codes = False
        is_base_domain = False
        self.assertFalse(should_filter_by_language(url, uses_language_codes, is_base_domain))

    def test_normalize_and_filter_links(self):
        internal_links = ["about", "contact", "#section"]
        base_url = "https://www.example.com"
        max_links = 10
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_with_string_links(self):
        """
        Test normalize_and_filter_links properly handles string links.
        """
        internal_links = ["about", "contact", "#section", "https://external-domain.com"]
        base_url = "https://www.example.com"
        max_links = 10
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_with_dict_links(self):
        """
        Test normalize_and_filter_links properly handles dictionary links with 'href' key.
        """
        internal_links = [
            {"href": "about"}, 
            {"href": "contact"}, 
            {"href": "#section"},
            {"href": "mailto:info@example.com"}
        ]
        base_url = "https://www.example.com"
        max_links = 10
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_with_url_key(self):
        """
        Test normalize_and_filter_links properly handles dictionary links with 'url' key.
        """
        internal_links = [
            {"url": "about"}, 
            {"url": "contact"}, 
            {"url": "#products"}
        ]
        base_url = "https://www.example.com"
        max_links = 10
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_protocol_relative(self):
        """
        Test normalize_and_filter_links properly handles protocol-relative URLs.
        """
        internal_links = ["//example.com/about", "//example.com/contact"]
        base_url = "https://www.example.com"
        max_links = 10
        expected = []  # Different domain, should be filtered
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

        # Same domain test
        internal_links = ["//www.example.com/about", "//www.example.com/contact"]
        expected = ["https://www.example.com/about", "https://www.example.com/contact"]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_invalid_schemes(self):
        """
        Test normalize_and_filter_links properly filters links with invalid schemes.
        """
        internal_links = [
            "mailto:info@example.com", 
            "tel:123456789", 
            "javascript:void(0)", 
            "ftp://example.com/file.txt"
        ]
        base_url = "https://www.example.com"
        max_links = 10
        expected = []
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_max_limit(self):
        """
        Test normalize_and_filter_links properly limits the number of links.
        """
        internal_links = ["page1", "page2", "page3", "page4", "page5"]
        base_url = "https://www.example.com"
        max_links = 3
        result = normalize_and_filter_links(internal_links, base_url, max_links)
        self.assertEqual(len(result), 3)
        self.assertEqual(
            result,
            ["https://www.example.com/page1", "https://www.example.com/page2", "https://www.example.com/page3"]
        )

    def test_normalize_and_filter_links_mixed_inputs(self):
        """
        Test normalize_and_filter_links with mixed types of inputs.
        """
        internal_links = [
            "about", 
            {"href": "contact"}, 
            {"url": "services"}, 
            "#section", 
            "mailto:info@example.com",
            None,  # Invalid type
            {"invalid": "key"}  # Missing href/url
        ]
        base_url = "https://www.example.com"
        max_links = 10
        expected = [
            "https://www.example.com/about", 
            "https://www.example.com/contact", 
            "https://www.example.com/services"
        ]
        self.assertEqual(normalize_and_filter_links(internal_links, base_url, max_links), expected)

    def test_normalize_and_filter_links_external_domains(self):
        """
        Test normalize_and_filter_links filters out external domains.
        """
        internal_links = [
            "about",  # Internal
            "https://www.example.com/contact",  # Internal (absolute but same domain)
            "https://external-domain.com/page",  # External
            "//external-domain.com/page"  # Protocol-relative external
        ]
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