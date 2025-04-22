"""
Unit tests for crawl_domain.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import unittest
from typing import Any
import webcrawl.crawl_domain as crawl_domain
import sys
import asyncio
from unittest.mock import patch

class TestCrawlDomain(unittest.TestCase):
    """Tests for crawl_domain.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input = 'tests/automation/webcrawl/data/valid_companies.csv'
        self.invalid_input = 'tests/automation/webcrawl/data/invalid_companies.csv'
        self.empty_input = 'tests/automation/webcrawl/data/empty_companies.csv'
        self.missing_file = 'tests/automation/webcrawl/data/missing_file.csv'
        self.output_dir = 'tests/automation/webcrawl/output/domain_content_output'
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid input
        with open(self.valid_input, 'w') as f:
            f.write('name,url\nFirma A,https://example.com\nFirma B,https://example.org\n')
        # Create sample invalid input (missing columns)
        with open(self.invalid_input, 'w') as f:
            f.write('company,website\nX,https://invalid.com\n')
        # Create empty input
        with open(self.empty_input, 'w') as f:
            f.write('name,url\n')

    def tearDown(self) -> None:
        for f in [self.valid_input, self.invalid_input, self.empty_input]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_dir):
            for file in os.listdir(self.output_dir):
                os.remove(os.path.join(self.output_dir, file))
            os.rmdir(self.output_dir)

    def test_main_validInput_createsMarkdownFiles(self):
        """main_validInput_createsMarkdownFiles_expectedMarkdownCreated: Should create markdown files for each company in valid input"""
        test_args = [
            "crawl_domain.py",
            "--excel",
            self.valid_input,
            "--output",
            self.output_dir,
        ]
        with patch.object(sys, 'argv', test_args):
            asyncio.run(crawl_domain.main())
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.md') for f in output_files), "No markdown files created for valid input")

    def test_main_invalidInput_missingColumns_raisesError(self):
        """main_invalidInput_missingColumns_raisesError_expectedException: Should raise ValueError for missing columns in input"""
        test_args = [
            "crawl_domain.py",
            "--excel",
            self.invalid_input,
            "--output",
            self.output_dir,
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(ValueError):
                asyncio.run(crawl_domain.main())

    def test_main_emptyInput_createsNoMarkdown(self):
        """main_emptyInput_createsNoMarkdown_expectedNoOutput: Should not create markdown files for empty input"""
        test_args = [
            "crawl_domain.py",
            "--excel",
            self.empty_input,
            "--output",
            self.output_dir,
        ]
        with patch.object(sys, 'argv', test_args):
            asyncio.run(crawl_domain.main())
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input")

    def test_main_missingInputFile_raisesFileNotFoundError(self):
        """main_missingInputFile_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input file"""
        test_args = [
            "crawl_domain.py",
            "--excel",
            self.missing_file,
            "--output",
            self.output_dir,
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                asyncio.run(crawl_domain.main())

if __name__ == '__main__':
    unittest.main()
