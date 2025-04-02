import unittest
from unittest.mock import patch, mock_open, MagicMock, AsyncMock
import json
import os
import sys
import asyncio
from extract_sachanlagen import check_and_reprocess_error_files

# Import the function to test - using absolute import

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

# Helper function to run async tests
def run_async_test(coro):
    return asyncio.run(coro)

# Add a test runner for async tests
if __name__ == '__main__':
    unittest.main()