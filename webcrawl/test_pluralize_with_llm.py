import unittest
from unittest.mock import patch, mock_open, MagicMock, call
import os
import logging
import io
import sys
from webcrawl.pluralize_with_llm import process_directory, failed_files

class TestProcessDirectory(unittest.TestCase):
    
    def setUp(self):
        # Clear the failed_files list before each test
        global failed_files
        failed_files.clear()
        
        # Set up logging to capture log messages
        self.log_output = io.StringIO()
        self.log_handler = logging.StreamHandler(self.log_output)
        logging.getLogger().addHandler(self.log_handler)
        logging.getLogger().setLevel(logging.INFO)
    
    def tearDown(self):
        # Clean up logging
        logging.getLogger().removeHandler(self.log_handler)
        self.log_output.close()
    
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_no_json_files_found(self, mock_listdir, mock_makedirs):
        # Setup
        mock_listdir.return_value = ['file1.txt', 'file2.csv']  # No JSON files
        
        # Execute
        process_directory('input_dir', 'output_dir', temperatures=[0.5, 0.7])
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertIn("No JSON files found in input_dir", self.log_output.getvalue())
    
    @patch('webcrawl.pluralize_with_llm.process_json_file')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_successful_processing(self, mock_listdir, mock_makedirs, mock_process_json_file):
        # Setup
        mock_listdir.return_value = ['file1.json', 'file2.json', 'file3.txt']
        temperatures = [0.5, 0.7]
        
        # Execute
        process_directory('input_dir', 'output_dir', temperatures)
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertEqual(mock_process_json_file.call_count, 2)  # Should be called twice for 2 JSON files
        
        # Check that process_json_file was called with the correct parameters
        expected_calls = [
            call(os.path.join('input_dir', 'file1.json'), 
                 os.path.join('output_dir', 'file1.json'), 
                 temperatures),
            call(os.path.join('input_dir', 'file2.json'), 
                 os.path.join('output_dir', 'file2.json'), 
                 temperatures)
        ]
        mock_process_json_file.assert_has_calls(expected_calls, any_order=False)
        
        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 2 JSON files to process", log_output)
        self.assertIn("All 2 files processed successfully", log_output)
    
    @patch('webcrawl.pluralize_with_llm.process_json_file')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_processing_with_failures(self, mock_listdir, mock_makedirs, mock_process_json_file):
        # Setup
        global failed_files
        mock_listdir.return_value = ['file1.json', 'file2.json', 'file3.json']
        
        # Add some failed files to simulate failures during processing
        failed_files.append((os.path.join('input_dir', 'file1.json'), 'products'))
        failed_files.append((os.path.join('input_dir', 'file2.json'), 'machines'))
        
        # Execute
        process_directory('input_dir', 'output_dir')
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertEqual(mock_process_json_file.call_count, 3)  # Should be called for all 3 JSON files
        
        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 3 JSON files to process", log_output)
        self.assertIn("FAILURE SUMMARY", log_output)
        self.assertIn("Total files with failures: 2", log_output)
        self.assertIn("Success rate: 33", log_output)
    
    @patch('webcrawl.pluralize_with_llm.process_json_file')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_processing_with_custom_temperatures(self, mock_listdir, mock_makedirs, mock_process_json_file):
        # Setup
        mock_listdir.return_value = ['file1.json']
        custom_temperatures = [0.1, 0.3, 0.9]
        
        # Execute
        @patch('webcrawl.pluralize_with_llm.process_json_file')
        @patch('os.makedirs', side_effect=Exception("Directory creation failed"))
        @patch('os.listdir')
        def test_handles_makedirs_exception(self, mock_listdir, mock_makedirs, mock_process_json_file):
            # Setup
            mock_listdir.return_value = ['file1.json']
            
            # Execute - should handle the exception during directory creation
            process_directory('input_dir', 'output_dir')
            
            # Verify process_json_file was not called due to makedirs failure
            mock_process_json_file.assert_not_called()
            
            # Check log messages for error 
            log_output = self.log_output.getvalue()
            self.assertIn("Error", log_output)
        
        @patch('webcrawl.pluralize_with_llm.process_json_file')
        @patch('os.makedirs')
        @patch('os.listdir', side_effect=Exception("Permission denied"))
        def test_handles_listdir_exception(self, mock_listdir, mock_makedirs, mock_process_json_file):
            # Setup - os.listdir will raise an exception
            
            # Execute - should handle the exception
            process_directory('input_dir', 'output_dir')
            
            # Verify process_json_file was not called due to listdir failure
            mock_process_json_file.assert_not_called()
            
            # Verify that makedirs was still called
            mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
            
            # Check log messages for error
            log_output = self.log_output.getvalue()
            self.assertIn("Error", log_output)
        
        @patch('webcrawl.pluralize_with_llm.process_json_file')
        @patch('os.makedirs')
        @patch('os.listdir')
        def test_empty_directory(self, mock_listdir, mock_makedirs, mock_process_json_file):
            # Setup - empty directory
            mock_listdir.return_value = []
            
            # Execute
            process_directory('input_dir', 'output_dir')
            
            # Verify no processing happened
            mock_process_json_file.assert_not_called()
            
            # Check log messages
            log_output = self.log_output.getvalue()
            self.assertIn("No JSON files found in input_dir", log_output)
        
        @patch('webcrawl.pluralize_with_llm.failed_files', [(os.path.join('input_dir', 'file1.json'), 'products')])
        @patch('webcrawl.pluralize_with_llm.process_json_file')
        @patch('os.makedirs') 
        @patch('os.listdir')
        def test_with_existing_failed_files(self, mock_listdir, mock_makedirs, mock_process_json_file, _):
            # Setup - directory with one file and failed_files already has an entry
            mock_listdir.return_value = ['file2.json']
            
            # Execute
            process_directory('input_dir', 'output_dir')
            
            # Verify processing happened for the new file
            mock_process_json_file.assert_called_once()
            
            # Check summary includes both the existing and new failures
            log_output = self.log_output.getvalue()
            self.assertIn("Found 1 JSON files to process", log_output)
            
            # Verify that the existing failed file is still in the summary
            self.assertIn(os.path.join('input_dir', 'file1.json'), log_output)
        
        @patch('webcrawl.pluralize_with_llm.process_json_file')
        @patch('os.makedirs')
        @patch('os.listdir')
        def test_handles_exceptions_in_processing_with_error_log(self, mock_listdir, mock_makedirs, mock_process_json_file):
            # Setup
            mock_listdir.return_value = ['file1.json']
            mock_process_json_file.side_effect = Exception("Test exception")
            
            # Execute - should not raise an exception
            process_directory('input_dir', 'output_dir')
            
            # Verify process_json_file was called despite raising exception
            mock_process_json_file.assert_called_once()
            
            # The processing should continue and complete
            log_output = self.log_output.getvalue()