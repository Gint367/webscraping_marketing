import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

# Add parent directory to path to import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from webcrawl.fill_process_type import (
    extract_category_from_filename,
    extract_category_from_folder,
    run_fill_process_type,
)


class TestFillProcessType(unittest.TestCase):
    """Tests for the fill_process_type module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temp directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        
        # Create a sample JSON file
        self.sample_data = [
            {
                "company_name": "Test Company",
                "products": ["Product1", "Product2"],
                "machines": ["Machine1"],
                "process_type": []
            }
        ]
        
        self.test_file_path = os.path.join(self.temp_dir_path, "pluralized_testcategory.json")
        with open(self.test_file_path, 'w', encoding='utf-8') as f:
            json.dump(self.sample_data, f)
            
    def tearDown(self):
        """Tear down test fixtures."""
        self.temp_dir.cleanup()

    @patch('webcrawl.fill_process_type.process_json_file')
    def test_run_fill_process_type_with_explicit_category(self, mock_process_json_file):
        """Test run_fill_process_type when category is explicitly provided."""
        # Setup
        mock_process_json_file.return_value = None
        explicit_category = "explicit_category"
        
        # Execute
        output_paths = run_fill_process_type(
            input_file=self.test_file_path,
            category=explicit_category,
            log_level="ERROR"  # Reduce log noise during tests
        )
        
        # Verify
        self.assertEqual(len(output_paths), 1)
        # Check that process_json_file was called with the explicit category
        mock_process_json_file.assert_called_once()
        _, _, kwargs = mock_process_json_file.mock_calls[0]
        self.assertEqual(kwargs['category'], explicit_category)

    @patch('webcrawl.fill_process_type.process_json_file')
    def test_run_fill_process_type_with_extracted_category(self, mock_process_json_file):
        """Test run_fill_process_type extracts category from filename when not explicitly provided."""
        # Setup
        mock_process_json_file.return_value = None
        
        # Execute
        output_paths = run_fill_process_type(
            input_file=self.test_file_path,
            log_level="ERROR"  # Reduce log noise during tests
        )
        
        # Verify
        self.assertEqual(len(output_paths), 1)
        # The filename is pluralized_testcategory.json, so the extracted category should be "testcategory"
        _, _, kwargs = mock_process_json_file.mock_calls[0]
        self.assertEqual(kwargs['category'], "testcategory")

    def test_extract_category_from_filename(self):
        """Test extract_category_from_filename extracts correctly."""
        # Test a valid filename
        filename = "pluralized_aluminiumwerke.json"
        category = extract_category_from_filename(filename)
        self.assertEqual(category, "aluminiumwerke")
        
        # Test an invalid filename
        filename = "invalid_filename.json"
        category = extract_category_from_filename(filename)
        self.assertIsNone(category)

    def test_extract_category_from_folder(self):
        """Test extract_category_from_folder extracts correctly."""
        # Test a valid folder name
        folder_path = "/path/to/llm_extracted_aluminiumwerke"
        category = extract_category_from_folder(folder_path)
        self.assertEqual(category, "aluminiumwerke")
        
        # Test another valid pattern
        folder_path = "/path/to/pluralized_kunststoffteile"
        category = extract_category_from_folder(folder_path)
        self.assertEqual(category, "kunststoffteile")
        
        # Test an invalid folder name
        folder_path = "/path/to/invalid_folder"
        category = extract_category_from_folder(folder_path)
        self.assertIsNone(category)


if __name__ == '__main__':
    unittest.main()
