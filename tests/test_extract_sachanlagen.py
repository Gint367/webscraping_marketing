import os
import unittest
from unittest.mock import call, patch, mock_open, MagicMock
import asyncio
from extract_sachanlagen import check_and_reprocess_error_files, extract_category_from_input_path, process_files

# Import the function to test - using absolute import

class TestExtractCategoryFromInputPath(unittest.TestCase):
    """Test cases for extract_category_from_input_path function"""
    
    def test_extract_category_from_directory(self):
        """Test extracting category from a directory path"""
        # Test standard pattern
        self.assertEqual(
            extract_category_from_input_path('/path/to/bundesanzeiger_local_maschinenbau'),
            'maschinenbau'
        )
        
        # Test with _output suffix
        self.assertEqual(
            extract_category_from_input_path('/path/to/bundesanzeiger_local_blechteile_output'),
            'blechteile'
        )
        
        # Test with non-matching directory
        self.assertEqual(
            extract_category_from_input_path('/path/to/normal_directory'),
            ''
        )
    
    def test_extract_category_from_file(self):
        """Test extracting category from a file path with parent directory pattern"""
        with patch('os.path.isfile', return_value=True):
            with patch('os.path.dirname', return_value='/path/to/bundesanzeiger_local_autozulieferer_output'):
                self.assertEqual(
                    extract_category_from_input_path('/path/to/bundesanzeiger_local_autozulieferer_output/file.html'),
                    'autozulieferer'
                )
            
            # Test with non-matching directory
            with patch('os.path.dirname', return_value='/path/to/normal_directory'):
                self.assertEqual(
                    extract_category_from_input_path('/path/to/normal_directory/file.html'),
                    ''
                )

class TestCheckAndReprocessErrorFiles(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test"""
        # Sample data for testing
        self.error_data_list = [{"error": True, "message": "Failed to process"}]
        self.error_data_dict = {"error": True, "message": "Failed to process"}
        self.valid_data = [{"values": {"Sachanlagen_1": "100"}, "table_name": "Aktiva", "is_Teuro": False}]
        
        # Mock for LLM strategy
        self.mock_llm_strategy = MagicMock()

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_no_json_files(self, mock_logger, mock_process_files, mock_join, mock_listdir):
        """Test when there are no JSON files in the output directory"""
        # Setup
        mock_listdir.return_value = ['file1.txt', 'file2.csv']
        mock_join.side_effect = lambda *args: '/'.join(args)
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 0)
        mock_logger.info.assert_any_call("Checking for files with errors in output_dir...")
        mock_logger.info.assert_any_call("No files with errors found")
        mock_process_files.assert_not_called()

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_no_error_files(self, mock_logger, mock_process_files, mock_json_load, 
                                mock_open, mock_join, mock_listdir):
        """Test when all JSON files are valid with no errors"""
        # Setup
        mock_listdir.return_value = ['file1.json', 'file2.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = self.valid_data
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 0)
        mock_process_files.assert_not_called()
        self.assertEqual(mock_open.call_count, 2)

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_error_files_list_format(self, mock_logger, mock_process_files, mock_walk, 
                                         mock_json_load, mock_open, mock_join, mock_listdir):
        """Test JSON files with errors in list format"""
        # Setup
        mock_listdir.return_value = ['file1.json', 'file2.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = self.error_data_list
        
        # Mock finding original files
        mock_walk.return_value = [
            ('input_dir', [], ['file1.html', 'file2.html']),
        ]
        
        # Configure process_files to return empty list
        mock_process_files.return_value = []
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 2)  # 2 files reprocessed
        mock_process_files.assert_called_once()
        # Check that both files were passed to process_files
        files_to_reprocess = mock_process_files.call_args[0][0]
        self.assertEqual(len(files_to_reprocess), 2)
        self.assertIn('input_dir/file1.html', files_to_reprocess)
        self.assertIn('input_dir/file2.html', files_to_reprocess)

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_error_files_dict_format(self, mock_logger, mock_process_files, mock_walk, 
                                         mock_json_load, mock_open, mock_join, mock_listdir):
        """Test JSON files with errors in dictionary format"""
        # Setup
        mock_listdir.return_value = ['file1.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = self.error_data_dict
        
        mock_walk.return_value = [
            ('input_dir', [], ['file1.html']),
        ]
        
        mock_process_files.return_value = []
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 1)
        mock_process_files.assert_called_once_with(
            ['input_dir/file1.html'], self.mock_llm_strategy, 'output_dir'
        )

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_original_file_not_found(self, mock_logger, mock_process_files, mock_walk, 
                                         mock_json_load, mock_open, mock_join, mock_listdir):
        """Test when error file exists but original source file cannot be found"""
        # Setup
        mock_listdir.return_value = ['file1.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = self.error_data_dict
        
        # Empty file list from os.walk
        mock_walk.return_value = [
            ('input_dir', [], []),
        ]
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 0)  # No files reprocessed
        mock_process_files.assert_not_called()
        mock_logger.warning.assert_called_with(
            "Error in file1.json, but couldn't find original file file1.html")

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open')
    @patch('extract_sachanlagen.logger')
    async def test_file_open_exception(self, mock_logger, mock_open, mock_join, mock_listdir):
        """Test exception handling when opening files"""
        # Setup
        mock_listdir.return_value = ['file1.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_open.side_effect = IOError("Could not open file")
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 0)
        mock_logger.error.assert_called_with(
            "Error reading file1.json: Could not open file")

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_mixed_error_and_valid_files(self, mock_logger, mock_process_files, mock_walk, 
                                             mock_json_load, mock_open, mock_join, mock_listdir):
        """Test a mixture of error and valid files"""
        # Setup
        mock_listdir.return_value = ['error.json', 'valid.json']
        
        # Different JSON content for different files
        def load_side_effect(f):
            filename = f.name
            if 'error.json' in filename:
                return self.error_data_dict
            else:
                return self.valid_data
                
        mock_json_load.side_effect = load_side_effect
        mock_join.side_effect = lambda *args: '/'.join(args)
        
        # Mock finding original file
        mock_walk.return_value = [
            ('input_dir', [], ['error.html']),
        ]
        
        mock_process_files.return_value = []
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 1)  # Only error file is reprocessed
        mock_process_files.assert_called_once()
        self.assertEqual(len(mock_process_files.call_args[0][0]), 1)

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_multiple_matching_original_files(self, mock_logger, mock_process_files, mock_walk, 
                                                  mock_json_load, mock_open, mock_join, mock_listdir):
        """Test when multiple copies of original file are found in different directories"""
        # Setup
        mock_listdir.return_value = ['file1.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = self.error_data_dict
        
        # Multiple files with same name in different directories
        mock_walk.return_value = [
            ('input_dir', [], ['file1.html']),
            ('input_dir/subdir', [], ['file1.html']),
        ]
        
        mock_process_files.return_value = []
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 1)  # Should process just one file
        mock_process_files.assert_called_once()
        # Should choose the first file found
        self.assertEqual(mock_process_files.call_args[0][0][0], 'input_dir/file1.html')

    @patch('extract_sachanlagen.os.listdir')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.load')
    @patch('extract_sachanlagen.os.walk')
    @patch('extract_sachanlagen.process_files')
    @patch('extract_sachanlagen.logger')
    async def test_correct_file_extension_handling(self, mock_logger, mock_process_files, mock_walk, 
                                                 mock_json_load, mock_open, mock_join, mock_listdir):
        """Test that the function correctly handles file extensions when looking for original files"""
        # Setup
        mock_listdir.return_value = ['Unterschuetz_Sondermaschinenbau_GmbH_cleaned.json']
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_json_load.return_value = {"error": True, "message": "Failed to process"}
        
        # Mock finding original file
        mock_walk.return_value = [
            ('input_dir', [], ['Unterschuetz_Sondermaschinenbau_GmbH_cleaned.html']),
        ]
        
        mock_process_files.return_value = []
        
        # Call function
        result = await check_and_reprocess_error_files('output_dir', 'input_dir', '.html', self.mock_llm_strategy)
        
        # Assertions
        self.assertEqual(result, 1)  # Should process the file
        mock_process_files.assert_called_once()
        # Check the first argument to process_files, which should be the file list
        self.assertEqual(mock_process_files.call_args[0][0][0], 
                        'input_dir/Unterschuetz_Sondermaschinenbau_GmbH_cleaned.html')
        
        # Verify no warning was logged about not finding the original file
        warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list 
                        if "couldn't find original file" in call[0][0]]
        self.assertEqual(len(warning_calls), 0, "Warning about not finding original file was logged")

class TestNumberFormatHandling(unittest.TestCase):
    """Test cases for handling number formats extracted from LLM using convert_german_number"""
    
    def test_correct_german_number_formats(self):
        """Test conversion of correctly formatted German numbers"""
        from extract_sachanlagen import convert_german_number
        from decimal import Decimal
        
        # Test cases with correct German number format
        test_cases = [
            ("3.139,74", Decimal("3139.74")),
            ("1.234.567,89", Decimal("1234567.89")),
            ("0,45", Decimal("0.45")),
            ("42", Decimal("42")),
            ("-123,45", Decimal("-123.45")),
            ("1.234", Decimal("1234"))  # No decimal part
        ]
        
        for input_str, expected in test_cases:
            result = convert_german_number(input_str)
            self.assertEqual(result, expected, f"Failed for {input_str}, got {result} expected {expected}")
    
    def test_incorrect_number_formats(self):
        """Test conversion of incorrectly formatted numbers"""
        from extract_sachanlagen import convert_german_number
        from decimal import Decimal
        
        # Test cases with incorrect formats
        test_cases = [
            # Multiple decimal separators
            ("3.139,112,74", Decimal("3139112.74")),  # Should take the last comma as decimal
            ("1,23.4,56", Decimal("1234.56")),  # Malformed with multiple separators
            
            # Mixed format cases
            ("1.234.56", Decimal("123456")),  # Using period as group separator
            
            # Numbers with extra characters that should be removed
            ("€3.139,74", Decimal("3139.74")),
            ("3.139,74€", Decimal("3139.74")),
            ("3.139,74 €", Decimal("3139.74"))
        ]
        
        for input_str, expected in test_cases:
            result = convert_german_number(input_str)
            self.assertEqual(result, expected, f"Failed for {input_str}, got {result} expected {expected}")
            
        # Test with warning logging for multiple separators
        import logging
        with self.assertLogs(level='WARNING') as logs:
            result = convert_german_number("3.139,112,74", "test_file.json")
            self.assertEqual(result, Decimal("3139112.74"))
            self.assertTrue(any("Multiple decimal separators found" in log for log in logs.output))
    
    def test_edge_cases(self):
        """Test edge cases for number format handling"""
        from extract_sachanlagen import convert_german_number
        from decimal import Decimal
        
        # Edge cases to test
        test_cases = [
            ("", Decimal("0")),           # Empty string
            (None, Decimal("0")),         # None value
            ("N/A", Decimal("0")),        # Not a number text
            ("keine Angabe", Decimal("0")),  # German for "no information"
            ("-", Decimal("0")),          # Just a dash
            ("error", Decimal("0"))       # Generic error text
        ]
        
        for input_str, expected in test_cases:
            result = convert_german_number(input_str)
            self.assertEqual(result, expected, f"Failed for '{input_str}', got {result} expected {expected}")
        
        # Test with exception logging
        import logging
        with self.assertLogs(level='WARNING') as logs:
            result = convert_german_number("not_a_number", "test_file.json")
            self.assertEqual(result, Decimal("0"))
            self.assertTrue(any("Failed to convert number" in log for log in logs.output))
    
    def test_special_cases(self):
        """Test special cases like parenthesized negative numbers"""
        from extract_sachanlagen import convert_german_number
        from decimal import Decimal
        
        # Special formatting cases
        test_cases = [
            ("(123,45)", Decimal("-123.45")),    # Negative in parentheses
            ("(1.234,56)", Decimal("-1234.56")), # Negative with thousands separator
            (" (42) ", Decimal("-42")),          # With spaces
            ("- 123,45", Decimal("-123.45"))     # Negative with space
        ]
        
        for input_str, expected in test_cases:
            result = convert_german_number(input_str)
            self.assertEqual(result, expected, f"Failed for {input_str}, got {result} expected {expected}")
    
    def test_very_large_numbers(self):
        """Test handling of very large numbers"""
        from extract_sachanlagen import convert_german_number
        from decimal import Decimal
        
        # Large number test cases
        test_cases = [
            ("1.234.567.890,12", Decimal("1234567890.12")),
            ("999.999.999.999,99", Decimal("999999999999.99")),
            ("1.000.000.000", Decimal("1000000000"))
        ]
        
        for input_str, expected in test_cases:
            result = convert_german_number(input_str)
            self.assertEqual(result, expected, f"Failed for {input_str}, got {result} expected {expected}")

class TestProcessFilesErrorHandling(unittest.TestCase):
    """Test cases for error handling in process_files function"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        self.mock_llm_strategy = MagicMock()
        self.output_dir = "/tmp/test_output"
        
    @patch('extract_sachanlagen.AsyncWebCrawler')
    @patch('extract_sachanlagen.os.path.basename')
    @patch('extract_sachanlagen.os.path.splitext')
    @patch('extract_sachanlagen.extract_company_name')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.open', new_callable=mock_open)
    @patch('extract_sachanlagen.json.dump')
    @patch('extract_sachanlagen.logger')
    async def test_nonetype_error_handling(self, mock_logger, mock_json_dump, mock_open, 
                                        mock_path_join, mock_extract_company, mock_splitext, 
                                        mock_basename, MockCrawler):
        """Test handling of NoneType error in process_files"""
        # Setup
        file_paths = ["/path/to/test_file.html"]
        file_urls = ["file:///path/to/test_file.html"]
        
        # Mock the company name extraction
        mock_extract_company.return_value = "Test Company"
        
        # Mock path operations
        mock_basename.return_value = "test_file.html"
        mock_splitext.return_value = ["test_file", ".html"]
        mock_path_join.return_value = "/tmp/test_output/test_file.json"
        
        # Create a mock result object with specific NoneType error
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.url = file_urls[0]
        mock_result.error_message = "'NoneType' object has no attribute 'find_all'"
        
        # Mock the crawler's arun_many method to return our mock result
        mock_crawler_instance = MockCrawler.return_value.__aenter__.return_value
        mock_crawler_instance.arun_many.return_value.__aiter__.return_value = [mock_result]
        
        # Import process_files function
        from extract_sachanlagen import process_files
        
        # Call the function
        await process_files(file_paths, self.mock_llm_strategy, self.output_dir)
        
        # Assertions
        mock_logger.warning.assert_any_call(
            f"[1/{len(file_paths)}] File appears to be empty or cannot be parsed: {file_paths[0]}"
        )
        
        # Verify that the error content was created correctly
        expected_error_content = [{
            "error": True,
            "error_message": "Empty file or parsing error: 'NoneType' object has no attribute 'find_all'",
            "company_name": "Test Company"
        }]
        
        mock_json_dump.assert_called_once_with(
            expected_error_content,
            mock_open.return_value.__enter__.return_value,
            indent=2,
            ensure_ascii=False
        )
        
        mock_logger.info.assert_any_call("Created error placeholder for /tmp/test_output/test_file.json")
        
    @patch('extract_sachanlagen.AsyncWebCrawler')
    @patch('extract_sachanlagen.logger')
    async def test_generic_error_handling(self, mock_logger, MockCrawler):
        """Test handling of generic errors in process_files"""
        # Setup
        file_paths = ["/path/to/test_file.html"]
        file_urls = ["file:///path/to/test_file.html"]
        
        # Create a mock result object with a generic error
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.url = file_urls[0]
        mock_result.error_message = "Some generic error occurred"
        
        # Mock the crawler's arun_many method to return our mock result
        mock_crawler_instance = MockCrawler.return_value.__aenter__.return_value
        mock_crawler_instance.arun_many.return_value.__aiter__.return_value = [mock_result]
        
        # Import process_files function
        from extract_sachanlagen import process_files
        
        # Call the function
        await process_files(file_paths, self.mock_llm_strategy, self.output_dir)
        
        # Assertions - should log a warning but not create an error file
        mock_logger.warning.assert_any_call(
            f"[1/{len(file_paths)}] No content extracted: Some generic error occurred"
        )
        
        # Verify that no info message about creating an error placeholder was logged
        create_placeholder_calls = [call for call in mock_logger.info.call_args_list 
                                 if "Created error placeholder" in call[0][0]]
        self.assertEqual(len(create_placeholder_calls), 0)

class TestProcessFilesSkipping(unittest.TestCase):
    """Test cases for the file skipping mechanism in process_files function"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        self.mock_llm_strategy = MagicMock()
        self.output_dir = "/tmp/test_output"
        self.file_paths = [
            "/path/to/file1.html",
            "/path/to/file2.html",
            "/path/to/file3.html"
        ]
        self.expected_file_urls = [
            "file:///path/to/file1.html",
            "file:///path/to/file2.html",
            "file:///path/to/file3.html"
        ]
    
    @patch('extract_sachanlagen.os.path.exists')
    @patch('extract_sachanlagen.os.path.basename')
    @patch('extract_sachanlagen.os.path.splitext')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.AsyncWebCrawler')
    @patch('extract_sachanlagen.logger')
    async def test_process_files_when_skipping_existing_output_files(self, mock_logger, 
                                                                 MockCrawler, mock_join, 
                                                                 mock_splitext, mock_basename, 
                                                                 mock_exists):
        """Test process_files when skipping mechanism is enabled (default behavior)"""
        # Setup file path mocking
        mock_basename.side_effect = ["file1.html", "file2.html", "file3.html"]
        mock_splitext.side_effect = [["file1", ".html"], ["file2", ".html"], ["file3", ".html"]]
        mock_join.side_effect = [
            os.path.join(self.output_dir, "file1.json"),
            os.path.join(self.output_dir, "file2.json"),
            os.path.join(self.output_dir, "file3.json")
        ]
        
        # Mock to indicate file1.json and file3.json already exist
        mock_exists.side_effect = [True, False, True]
        
        # Mock the crawler's behavior (should not be called for skipped files)
        mock_crawler_instance = MockCrawler.return_value.__aenter__.return_value
        mock_crawler_instance.arun_many.return_value.__aiter__.return_value = [
            # Only file2.html should be processed
            self._create_mock_result(success=True, url=self.expected_file_urls[1], content=[{"values": {}}])
        ]
        
        # Call the function with overwrite=False (default)
        await process_files(self.file_paths, self.mock_llm_strategy, self.output_dir, overwrite=False)
        
        # Verify existence check was called for each file
        mock_exists.assert_has_calls([
            call(os.path.join(self.output_dir, "file1.json")),
            call(os.path.join(self.output_dir, "file2.json")),
            call(os.path.join(self.output_dir, "file3.json"))
        ])
        
        # Verify files_to_process was filtered correctly
        expected_files_to_process = ["/path/to/file2.html"]
        
        # Verify message about skipped files
        mock_logger.info.assert_any_call("Skipping 2 files that already have output files")
        
        # Verify file URLs passed to crawler are only for non-skipped files
        expected_file_urls = ["file:///path/to/file2.html"]
        mock_crawler_instance.arun_many.assert_called_once()
        # Check that the first arg (urls) contains only non-skipped files
        self.assertEqual(mock_crawler_instance.arun_many.call_args[1]['urls'], expected_file_urls)
    
    @patch('extract_sachanlagen.os.path.exists')
    @patch('extract_sachanlagen.os.path.basename')
    @patch('extract_sachanlagen.os.path.splitext') 
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.AsyncWebCrawler')
    @patch('extract_sachanlagen.logger')
    async def test_process_files_when_overwrite_enabled(self, mock_logger, 
                                                   MockCrawler, mock_join, 
                                                   mock_splitext, mock_basename, 
                                                   mock_exists):
        """Test process_files when overwrite mechanism is enabled"""
        # Setup file path mocking
        mock_basename.side_effect = ["file1.html", "file2.html", "file3.html"]
        mock_splitext.side_effect = [["file1", ".html"], ["file2", ".html"], ["file3", ".html"]]
        mock_join.side_effect = [
            os.path.join(self.output_dir, "file1.json"),
            os.path.join(self.output_dir, "file2.json"),
            os.path.join(self.output_dir, "file3.json")
        ]
        
        # Mock to indicate file1.json and file3.json already exist
        mock_exists.side_effect = [True, False, True]
        
        # Mock the crawler's behavior (should process all files)
        mock_crawler_instance = MockCrawler.return_value.__aenter__.return_value
        mock_crawler_instance.arun_many.return_value.__aiter__.return_value = [
            # All files should be processed
            self._create_mock_result(success=True, url=self.expected_file_urls[0], content=[{"values": {}}]),
            self._create_mock_result(success=True, url=self.expected_file_urls[1], content=[{"values": {}}]),
            self._create_mock_result(success=True, url=self.expected_file_urls[2], content=[{"values": {}}])
        ]
        
        # Call the function with overwrite=True
        await process_files(self.file_paths, self.mock_llm_strategy, self.output_dir, overwrite=True)
        
        # Verify existence check was NOT called
        mock_exists.assert_not_called()
        
        # Verify all files were processed
        mock_crawler_instance.arun_many.assert_called_once()
        # Check that urls parameter contains all file paths
        self.assertEqual(mock_crawler_instance.arun_many.call_args[1]['urls'], self.expected_file_urls)
    
    @patch('extract_sachanlagen.os.path.exists')
    @patch('extract_sachanlagen.os.path.basename')
    @patch('extract_sachanlagen.os.path.splitext')
    @patch('extract_sachanlagen.os.path.join')
    @patch('extract_sachanlagen.AsyncWebCrawler')
    @patch('extract_sachanlagen.logger')
    async def test_process_files_when_all_files_should_be_skipped(self, mock_logger, 
                                                             MockCrawler, mock_join, 
                                                             mock_splitext, mock_basename, 
                                                             mock_exists):
        """Test process_files when all files should be skipped"""
        # Setup file path mocking
        mock_basename.side_effect = ["file1.html", "file2.html", "file3.html"]
        mock_splitext.side_effect = [["file1", ".html"], ["file2", ".html"], ["file3", ".html"]]
        mock_join.side_effect = [
            os.path.join(self.output_dir, "file1.json"),
            os.path.join(self.output_dir, "file2.json"),
            os.path.join(self.output_dir, "file3.json")
        ]
        
        # Mock to indicate all files already exist
        mock_exists.side_effect = [True, True, True]
        
        # Mock the crawler (should not be called at all)
        mock_crawler_instance = MockCrawler.return_value.__aenter__.return_value
        
        # Call the function with overwrite=False
        result = await process_files(self.file_paths, self.mock_llm_strategy, self.output_dir, overwrite=False)
        
        # Verify the result is an empty list
        self.assertEqual(result, [])
        
        # Verify existence check was called for each file
        mock_exists.assert_has_calls([
            call(os.path.join(self.output_dir, "file1.json")),
            call(os.path.join(self.output_dir, "file2.json")),
            call(os.path.join(self.output_dir, "file3.json"))
        ])
        
        # Verify skipping message
        mock_logger.info.assert_any_call("Skipping 3 files that already have output files")
        mock_logger.info.assert_any_call("No files to process after skipping existing outputs")
        
        # Verify crawler was not used at all
        mock_crawler_instance.arun_many.assert_not_called()
    
    @patch('extract_sachanlagen.os.path.exists')
    @patch('extract_sachanlagen.check_and_reprocess_error_files')
    @patch('extract_sachanlagen.process_files')
    async def test_main_function_passes_overwrite_parameter_correctly(self, mock_process_files, 
                                                                 mock_check_reprocess, 
                                                                 mock_exists):
        """Test that the main function passes the overwrite parameter correctly to process_files"""
        # Import here to avoid circular imports
        from extract_sachanlagen import main
        
        # Setup mocks
        mock_exists.return_value = True
        mock_process_files.return_value = []
        mock_check_reprocess.return_value = 0
        
        # Create a parser with the necessary arguments
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("input")
        parser.add_argument("--output", "-o")
        parser.add_argument("--overwrite", action="store_true", default=False)
        
        # Test with overwrite=False
        with patch('argparse.ArgumentParser.parse_args', 
                   return_value=parser.parse_args(["input_dir", "--output", "output_dir"])):
            with patch('extract_sachanlagen.LLMExtractionStrategy'):
                with patch('extract_sachanlagen.os.path.isdir', return_value=True):
                    with patch('extract_sachanlagen.os.walk', 
                               return_value=[("root", [], ["file1.html", "file2.html"])]):
                        # Run the main function with overwrite=False (default)
                        await main()
        
        # Verify process_files was called with overwrite=False
        expected_files = [os.path.join("root", "file1.html"), os.path.join("root", "file2.html")]
        mock_process_files.assert_called_once()
        args, kwargs = mock_process_files.call_args
        self.assertEqual(kwargs.get('overwrite', None), False)
        
        # Reset mocks
        mock_process_files.reset_mock()
        
        # Test with overwrite=True
        with patch('argparse.ArgumentParser.parse_args', 
                   return_value=parser.parse_args(["input_dir", "--output", "output_dir", "--overwrite"])):
            with patch('extract_sachanlagen.LLMExtractionStrategy'):
                with patch('extract_sachanlagen.os.path.isdir', return_value=True):
                    with patch('extract_sachanlagen.os.walk', 
                               return_value=[("root", [], ["file1.html", "file2.html"])]):
                        # Run the main function with overwrite=True
                        await main()
        
        # Verify process_files was called with overwrite=True
        mock_process_files.assert_called_once()
        args, kwargs = mock_process_files.call_args
        self.assertEqual(kwargs.get('overwrite', None), True)
    
    def _create_mock_result(self, success=True, url="", content=None):
        """Helper method to create a mock result object"""
        mock_result = MagicMock()
        mock_result.success = success
        mock_result.url = url
        mock_result.extracted_content = content or []
        return mock_result


# Helper function to run async tests
def run_async_test(coro):
    return asyncio.run(coro)

# Add a test runner for async tests
if __name__ == '__main__':
    unittest.main()

