import unittest
import os
import json
import tempfile
import shutil
from unittest.mock import patch, mock_open, MagicMock
from webcrawl.consolidate import sort_items

from webcrawl.consolidate import (
    process_files,
)

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Sample company data
        self.company1_data = [
            {
                "company_name": "Test Company",
                "company_url": "https://testcompany.com",
                "products": ["Product A", "Machine Product B"],
                "machines": ["Machine X", "Machine Y"],
                "process_type": ["Type 1"],
                "lohnfertigung": False
            }
        ]
        
        self.company2_data = [
            {
                "company_name": "Another Company",
                "company_url": "https://anothercompany.com",
                "products": ["Product C", "Automated Solution"],
                "machines": ["Automatic Machine Z"],
                "process_type": ["Type 2"],
                "lohnfertigung": True
            }
        ]
        
        # Create test input files
        self.file1_path = os.path.join(self.temp_dir, "company1.json")
        self.file2_path = os.path.join(self.temp_dir, "company2.json")
        
        with open(self.file1_path, 'w', encoding='utf-8') as f:
            json.dump(self.company1_data, f)
        
        with open(self.file2_path, 'w', encoding='utf-8') as f:
            json.dump(self.company2_data, f)
        
        # Create filter file
        self.filter_file_path = os.path.join(self.temp_dir, "filter.txt")
        with open(self.filter_file_path, 'w', encoding='utf-8') as f:
            f.write("automatic\nproduct a\n# Comment to ignore\n")
            
        # Output file path
        self.output_path = os.path.join(self.temp_dir, "output.json")
        
    def test_sort_items(self):
        """Test the sort_items function directly."""
        
        # Test sorting based on machine keyword
        items = ["Product A", "Machine Product B"]
        sorted_items = sort_items(items)
        self.assertEqual(sorted_items, ["Machine Product B", "Product A"])
        
        # Test sorting with duplicates
        items = ["Product A", "Product A", "Machine B"]
        sorted_items = sort_items(items)
        self.assertEqual(sorted_items, ["Machine B", "Product A"])
        
        # Test sorting with mixed cases
        items = ["product a", "Product A", "machine b", "Machine B"]
        sorted_items = sort_items(items)
        self.assertEqual(len(sorted_items), 2)  # Should be deduplicated
        self.assertTrue("Machine B" in sorted_items or "machine b" in sorted_items)
        
        # Test empty list
        items = []
        sorted_items = sort_items(items)
        self.assertEqual(sorted_items, [])

    def test_process_files_with_duplicate_companies(self):
        """Test consolidating duplicate companies across files."""
        # Create another file with same company but different data
        duplicate_company_data = [
            {
                "company_name": "Test Company",
                "company_url": "https://testcompany.com/different",
                "products": ["Product C", "Product D"],
                "machines": ["Machine Z"],
                "process_type": ["Type 3"],
                "lohnfertigung": True
            }
        ]
        
        duplicate_file_path = os.path.join(self.temp_dir, "company_duplicate.json")
        with open(duplicate_file_path, 'w', encoding='utf-8') as f:
            json.dump(duplicate_company_data, f)
        
        with patch('builtins.print') as mock_print:
            process_files(self.temp_dir, self.output_path)
            
            with open(self.output_path, 'r', encoding='utf-8') as f:
                output_data = json.load(f)
            
            # Should still have 2 companies (unique company names)
            self.assertEqual(len(output_data), 2)
            
            # Find Test Company in output and verify its merged data
            test_company = next((c for c in output_data if c['company_name'] == "Test Company"), None)
            self.assertIsNotNone(test_company)
            
            # Check if products from both files are present
            self.assertTrue(set(["Product A", "Machine Product B", "Product C", "Product D"]).issubset(
                set(test_company["products"])))
            
            # Check if machines from both files are present
            self.assertTrue(set(["Machine X", "Machine Y", "Machine Z"]).issubset(
                set(test_company["machines"])))
            
            # Check if lohnfertigung was set to True (as it was True in one of the files)
            self.assertTrue(test_company["lohnfertigung"])