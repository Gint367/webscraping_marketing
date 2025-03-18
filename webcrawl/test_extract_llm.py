import unittest
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
import os
import json
import asyncio
from io import StringIO
import sys
import shutil

from webcrawl.extract_llm import (
    ensure_output_directory,
    process_files,
    main,
    Company
)


class TestExtractLLM(unittest.TestCase):
    
    def setUp(self):
        # Create temporary test directory
        self.test_output_dir = "test_llm_extracted_data"
        self.test_input_files = ["test_file1.md", "test_file2.md"]
        
    def tearDown(self):
        # Clean up any test files or directories created during tests
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
    
    def test_ensure_output_directory_creates_dir(self):
        with patch("os.path.exists", return_value=False), \
             patch("os.makedirs") as mock_makedirs:
            
            result = ensure_output_directory(self.test_output_dir)
            
            mock_makedirs.assert_called_once_with(self.test_output_dir)
            self.assertEqual(result, self.test_output_dir)
    
    def test_ensure_output_directory_existing(self):
        with patch("os.path.exists", return_value=True), \
             patch("os.makedirs") as mock_makedirs:
            
            result = ensure_output_directory(self.test_output_dir)
            
            mock_makedirs.assert_not_called()
            self.assertEqual(result, self.test_output_dir)
    
    @patch("webcrawl.extract_llm.AsyncWebCrawler")
    @patch("webcrawl.extract_llm.CrawlerRunConfig")
    @patch("os.path.abspath", return_value="/absolute/path/to/file.md")
    async def test_process_files_successful(self, mock_abspath, mock_config, mock_crawler):
        # Setup mocks
        mock_crawler_instance = AsyncMock()
        mock_crawler.return_value.__aenter__.return_value = mock_crawler_instance
        
        # Setup mock results with successful extraction
        mock_result1 = MagicMock()
        mock_result1.success = True
        mock_result1.url = "file:///absolute/path/to/file1.md"
        mock_result1.extracted_content = {"company_name": "Test Company"}
        
        mock_result2 = MagicMock()
        mock_result2.success = True
        mock_result2.url = "file:///absolute/path/to/file2.md"
        mock_result2.extracted_content = {"company_name": "Another Company"}
        
        mock_crawler_instance.arun_many.return_value = [mock_result1, mock_result2]
        
        # Setup mock llm_strategy
        mock_llm_strategy = MagicMock()
        mock_llm_strategy.show_usage = MagicMock()
        
        # Test with mock file paths
        with patch("builtins.open", mock_open()) as mock_file, \
             patch("os.path.basename", side_effect=lambda p: p.split('/')[-1]), \
             patch("os.path.splitext", side_effect=lambda p: (p.split('.')[0], '.md')), \
             patch("json.dump") as mock_json_dump, \
             patch("builtins.print") as mock_print:
            
            result = await process_files(self.test_input_files, mock_llm_strategy, self.test_output_dir)
            
            # Assert crawler was called correctly
            mock_crawler_instance.arun_many.assert_called_once()
            
            # Assert files were written correctly
            self.assertEqual(mock_file.call_count, 2)
            mock_json_dump.assert_called()
            
            # Assert usage stats were shown
            mock_llm_strategy.show_usage.assert_called_once()
            
            # Assert correct output messages
            mock_print.assert_any_call(f"Extracted data saved to {self.test_output_dir}/file1_extracted.json")
            mock_print.assert_any_call(f"Extracted data saved to {self.test_output_dir}/file2_extracted.json")
    
    @patch("webcrawl.extract_llm.AsyncWebCrawler")
    @patch("webcrawl.extract_llm.CrawlerRunConfig")
    @patch("os.path.abspath", return_value="/absolute/path/to/file.md")
    async def test_process_files_with_failure(self, mock_abspath, mock_config, mock_crawler):
        # Setup mocks
        mock_crawler_instance = AsyncMock()
        mock_crawler.return_value.__aenter__.return_value = mock_crawler_instance
        
        # Setup mock results with a failed extraction
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.error_message = "Extraction failed"
        
        mock_crawler_instance.arun_many.return_value = [mock_result]
        
        # Setup mock llm_strategy
        mock_llm_strategy = MagicMock()
        
        # Test with mock file paths
        with patch("builtins.open", mock_open()) as mock_file, \
             patch("builtins.print") as mock_print:
            
            await process_files(["test_file.md"], mock_llm_strategy, self.test_output_dir)
            
            # Assert error message was printed
            mock_print.assert_called_with("No content extracted from test_file.md: Extraction failed")
            
            # Assert no files were written
            mock_file.assert_not_called()
    
    @patch("webcrawl.extract_llm.ensure_output_directory")
    @patch("webcrawl.extract_llm.process_files")
    @patch("webcrawl.extract_llm.os.path.isfile", return_value=True)
    @patch("webcrawl.extract_llm.LLMExtractionStrategy")
    @patch("argparse.ArgumentParser")
    async def test_main_with_file_input(self, mock_argparser, mock_llm_strategy, 
                                        mock_isfile, mock_process_files, mock_ensure_dir):
        # Setup mock args
        mock_args = MagicMock()
        mock_args.input = "test_file.md"
        mock_args.output = self.test_output_dir
        mock_args.ext = ".md"
        mock_args.limit = None
        
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = mock_args
        mock_argparser.return_value = mock_parser
        
        # Setup mock ensure_output_directory
        mock_ensure_dir.return_value = self.test_output_dir
        
        # Setup mock process_files
        mock_process_files.return_value = []
        
        # Run main
        await main()
        
        # Assert ensure_output_directory was called
        mock_ensure_dir.assert_called_once_with(self.test_output_dir)
        
        # Assert LLMExtractionStrategy was created
        mock_llm_strategy.assert_called_once()
        
        # Assert process_files was called with the right file
        mock_process_files.assert_called_once()
        self.assertEqual(mock_process_files.call_args[0][0], ["test_file.md"])
    
    @patch("webcrawl.extract_llm.ensure_output_directory")
    @patch("webcrawl.extract_llm.process_files")
    @patch("webcrawl.extract_llm.os.path.isfile", return_value=False)
    @patch("webcrawl.extract_llm.os.path.isdir", return_value=True)
    @patch("webcrawl.extract_llm.os.walk")
    @patch("webcrawl.extract_llm.LLMExtractionStrategy")
    @patch("argparse.ArgumentParser")
    async def test_main_with_directory_input(self, mock_argparser, mock_llm_strategy, 
                                             mock_walk, mock_isdir, mock_isfile, 
                                             mock_process_files, mock_ensure_dir):
        # Setup mock args
        mock_args = MagicMock()
        mock_args.input = "test_dir"
        mock_args.output = self.test_output_dir
        mock_args.ext = ".md"
        mock_args.limit = 1
        
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = mock_args
        mock_argparser.return_value = mock_parser
        
        # Setup mock ensure_output_directory
        mock_ensure_dir.return_value = self.test_output_dir
        
        # Setup mock walk to return files
        mock_walk.return_value = [
            ("test_dir", [], ["file1.md", "file2.md", "file3.txt"])
        ]
        
        # Setup mock process_files
        mock_process_files.return_value = []
        
        # Capture stdout to check printed messages
        captured_output = StringIO()
        sys.stdout = captured_output
        
        # Run main
        await main()
        
        # Reset stdout
        sys.stdout = sys.__stdout__
        
        # Check output
        self.assertIn("Processing 1 files", captured_output.getvalue())
        
        # Assert process_files was called with the right file (limited to 1)
        mock_process_files.assert_called_once()
        self.assertEqual(mock_process_files.call_args[0][0], ["test_dir/file1.md"])
    
    @patch("webcrawl.extract_llm.ensure_output_directory")
    @patch("webcrawl.extract_llm.process_files")
    @patch("webcrawl.extract_llm.os.path.isfile", return_value=False)
    @patch("webcrawl.extract_llm.os.path.isdir", return_value=True)
    @patch("webcrawl.extract_llm.os.walk")
    @patch("webcrawl.extract_llm.LLMExtractionStrategy")
    @patch("argparse.ArgumentParser")
    async def test_main_with_empty_directory(self, mock_argparser, mock_llm_strategy, 
                                           mock_walk, mock_isdir, mock_isfile, 
                                           mock_process_files, mock_ensure_dir):
        # Setup mock args
        mock_args = MagicMock()
        mock_args.input = "test_dir"
        mock_args.output = self.test_output_dir
        mock_args.ext = ".md"
        mock_args.limit = None
        
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = mock_args
        mock_argparser.return_value = mock_parser
        
        # Setup mock ensure_output_directory
        mock_ensure_dir.return_value = self.test_output_dir
        
        # Setup mock walk to return no matching files
        mock_walk.return_value = [
            ("test_dir", [], ["file1.txt", "file2.csv"])
        ]
        
        # Capture stdout to check printed messages
        captured_output = StringIO()
        sys.stdout = captured_output
        
        # Run main
        await main()
        
        # Reset stdout
        sys.stdout = sys.__stdout__
        
        # Check output
        self.assertIn("No .md files found", captured_output.getvalue())
        
        # Assert process_files was not called
        mock_process_files.assert_not_called()
    
    @patch("webcrawl.extract_llm.ensure_output_directory")
    @patch("webcrawl.extract_llm.os.path.isfile", return_value=False)
    @patch("webcrawl.extract_llm.os.path.isdir", return_value=False)
    @patch("argparse.ArgumentParser")
    async def test_main_with_invalid_input(self, mock_argparser, mock_isdir, 
                                           mock_isfile, mock_ensure_dir):
        # Setup mock args
        mock_args = MagicMock()
        mock_args.input = "invalid_path"
        mock_args.output = self.test_output_dir
        
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = mock_args
        mock_argparser.return_value = mock_parser
        
        # Capture stdout to check printed messages
        captured_output = StringIO()
        sys.stdout = captured_output
        
        # Run main
        await main()
        
        # Reset stdout
        sys.stdout = sys.__stdout__
        
        # Check output
        self.assertIn("Error: invalid_path is not a valid file or directory", captured_output.getvalue())


if __name__ == "__main__":
    unittest.main()