import unittest
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
import json
import os
import sys
import asyncio
import io
from extract_sachanlagen import extract_category_from_input_path, ServiceContainer, check_and_reprocess_error_files

# Setup for async tests
def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro(*args, **kwargs))
        finally:
            loop.close()
    return wrapper

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
    """Test cases for check_and_reprocess_error_files function using direct patching"""
    
    @async_test
    async def test_check_and_reprocess_error_files_success(self):
        """Test successful reprocessing of error files"""
        # Mock data
        error_json1 = {"error": True, "message": "Failed extraction"}
        error_json2 = {"error": True, "message": "Another failure"}
        
        # Mock file I/O
        mock_file_reads = {
            'output_dir/error1.json': json.dumps(error_json1),
            'output_dir/error2.json': json.dumps(error_json2)
        }
        
        # Create patches
        with patch('builtins.open', side_effect=lambda f, *args, **kwargs: 
                io.StringIO(mock_file_reads.get(f, ""))):
            with patch('os.path.exists', return_value=True):
                with patch('json.load', side_effect=lambda f: json.loads(f.read())):
                    # Create mock services
                    mock_logging = MagicMock()
                    mock_file_service = MagicMock()
                    mock_file_service.list_files.return_value = ["error1.json", "error2.json"]
                    mock_file_service.walk_directory.return_value = [
                        ("input_dir", [], ["error1.html", "error2.html"])
                    ]
                    
                    # Create mock container
                    mock_container = MagicMock()
                    mock_container.logging_service = mock_logging
                    mock_container.file_service = mock_file_service
                    
                    # Mock the process_files function
                    with patch('extract_sachanlagen.process_files', 
                            new=AsyncMock(return_value=["output_dir/error1.json", "output_dir/error2.json"])) as mock_process:
                        # Call the function
                        result = await check_and_reprocess_error_files(
                            'output_dir', 'input_dir', '.html',
                            MagicMock(), # llm_strategy
                            mock_container
                        )
        
        # Assertions
        self.assertEqual(result, 2)
        mock_process.assert_called_once()
        expected_paths = ["input_dir/error1.html", "input_dir/error2.html"]
        actual_paths = mock_process.call_args[0][0]
        self.assertEqual(len(actual_paths), len(expected_paths))
        for path in expected_paths:
            self.assertIn(path, actual_paths)

    @async_test
    async def test_check_and_reprocess_no_error_files(self):
        """Test when no error files are found"""
        # Mock data - valid files without errors
        valid_json1 = [{"table_name": "Aktiva", "is_Teuro": True, "values": {"Sachanlagen": "1000"}}]
        valid_json2 = [{"table_name": "Passiva", "is_Teuro": True, "values": {"Other": "2000"}}]
        
        # Mock file I/O
        mock_file_reads = {
            'output_dir/valid1.json': json.dumps(valid_json1),
            'output_dir/valid2.json': json.dumps(valid_json2)
        }
        
        # Create patches
        with patch('builtins.open', side_effect=lambda f, *args, **kwargs: 
                io.StringIO(mock_file_reads.get(f, ""))):
            with patch('os.path.exists', return_value=True):
                with patch('json.load', side_effect=lambda f: json.loads(f.read())):
                    # Create mock services
                    mock_logging = MagicMock()
                    mock_file_service = MagicMock()
                    mock_file_service.list_files.return_value = ["valid1.json", "valid2.json"]
                    
                    # Create mock container
                    mock_container = MagicMock()
                    mock_container.logging_service = mock_logging
                    mock_container.file_service = mock_file_service
                    
                    # Mock the process_files function
                    with patch('extract_sachanlagen.process_files', new=AsyncMock()) as mock_process:
                        # Call the function
                        result = await check_and_reprocess_error_files(
                            'output_dir', 'input_dir', '.html',
                            MagicMock(), # llm_strategy
                            mock_container
                        )
        
        # Assertions
        self.assertEqual(result, 0)
        mock_process.assert_not_called()
        mock_logging.info.assert_any_call("No files with errors found")
        
    @async_test
    async def test_check_and_reprocess_original_not_found(self):
        """Test when error files exist but original files not found"""
        # Mock data
        error_json = {"error": True, "message": "Failed extraction"}
        
        # Mock file I/O
        mock_file_reads = {
            'output_dir/error1.json': json.dumps(error_json),
        }
        
        # Create patches
        with patch('builtins.open', side_effect=lambda f, *args, **kwargs: 
                io.StringIO(mock_file_reads.get(f, ""))):
            with patch('os.path.exists', return_value=True):
                with patch('json.load', side_effect=lambda f: json.loads(f.read())):
                    # Create mock services
                    mock_logging = MagicMock()
                    mock_file_service = MagicMock()
                    mock_file_service.list_files.return_value = ["error1.json"]
                    # No HTML files found in input directory
                    mock_file_service.walk_directory.return_value = [("input_dir", [], [])]
                    
                    # Create mock container
                    mock_container = MagicMock()
                    mock_container.logging_service = mock_logging
                    mock_container.file_service = mock_file_service
                    
                    # Mock the process_files function
                    with patch('extract_sachanlagen.process_files', new=AsyncMock()) as mock_process:
                        # Call the function
                        result = await check_and_reprocess_error_files(
                            'output_dir', 'input_dir', '.html',
                            MagicMock(), # llm_strategy
                            mock_container
                        )
        
        # Assertions
        self.assertEqual(result, 0)
        mock_process.assert_not_called()
        
        # Check for warning about missing original file
        warning_calls = [call[0][0] for call in mock_logging.warning.call_args_list]
        matching_warnings = [call for call in warning_calls if "couldn't find original file" in call]
        self.assertTrue(len(matching_warnings) > 0, 
                       f"No warning about missing file found in: {warning_calls}")

if __name__ == '__main__':
    unittest.main()