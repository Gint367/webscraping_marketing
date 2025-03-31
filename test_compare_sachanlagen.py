import unittest
import os
import sys
import pandas as pd
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from pathlib import Path
from io import StringIO
from compare_sachanlagen import clean_numeric_value, find_matching_company, main

# Import functions to test

class TestCompareSachanlagen(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test"""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.temp_dir, "test_companies.csv")
        self.json_dir = os.path.join(self.temp_dir, "json_files")
        os.makedirs(self.json_dir, exist_ok=True)
        
        # Create test CSV file
        self.csv_data = """company name,location,Technische Anlagen Start,Technische Anlagen End,Sachanlagen Start,Sachanlagen End,Start Date,End Date,Note
Aluminium Oxid Stade GmbH,Stade,"59.870.412,00","61.511.716,00","86.978.383,16","89.634.071,56",01.01.2018,31.12.2018,note
Test Company GmbH,Berlin,"1.000.000,00","1.200.000,00","5.000.000,00","5.500.000,00",01.01.2021,31.12.2021,note
Mismatched Values Corp,Hamburg,"500.000,00","600.000,00","2.000.000,00","2.500.000,00",01.01.2020,31.12.2020,note"""
        
        with open(self.csv_path, "w", encoding="utf-8") as f:
            f.write(self.csv_data)
            
        # Create test JSON files
        self.create_test_json_files()

    def tearDown(self):
        """Clean up after each test"""
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def create_test_json_files(self):
        # Create Aluminium Oxid Stade GmbH test file
        aluminium_data = [
            {
                "table_name": "Aktiva",
                "values": {
                    "Sachanlagen_2018": "86.978.383,16",
                    "Sachanlagen_2017": "89.634.071,56"
                },
                "error": False
            }
        ]
        with open(os.path.join(self.json_dir, "Aluminium_Oxid_Stade_GmbH_cleaned_extracted.json"), "w", encoding="utf-8") as f:
            json.dump(aluminium_data, f)
        
        # Create Test Company GmbH test file
        test_company_data = [
            {
                "table_name": "Bilanz",
                "values": {
                    "Sachanlagen_2021": "5000000",
                    "Sachanlagen_2020": "5500000"
                },
                "error": False
            }
        ]
        with open(os.path.join(self.json_dir, "Test_Company_GmbH_cleaned_extracted.json"), "w", encoding="utf-8") as f:
            json.dump(test_company_data, f)
            
        # Create mismatched values test file
        mismatched_data = [
            {
                "table_name": "Bilanz",
                "values": {
                    "Sachanlagen_2020": "2100000", # Mismatched value
                    "Sachanlagen_2019": "2600000"  # Mismatched value
                },
                "error": False
            }
        ]
        with open(os.path.join(self.json_dir, "Mismatched_Values_Corp_cleaned_extracted.json"), "w", encoding="utf-8") as f:
            json.dump(mismatched_data, f)

    def test_clean_numeric_value(self):
        """Test the clean_numeric_value function with various input formats"""
        # Test German number format
        self.assertAlmostEqual(clean_numeric_value("1.234.567,89"), 1234567.89)
        
        # Test with quotes
        self.assertAlmostEqual(clean_numeric_value('"1.000,00"'), 1000.00)
        
        # Test with integer
        self.assertAlmostEqual(clean_numeric_value(1000), 1000.0)
        
        # Test with float
        self.assertAlmostEqual(clean_numeric_value(1000.5), 1000.5)
        
        # Test with NA value
        self.assertIsNone(clean_numeric_value('NA'))
        
        # Test with None value
        self.assertIsNone(clean_numeric_value(None))
        
        # Test with invalid string
        self.assertIsNone(clean_numeric_value("nicht verfügbar"))
        
        # Test with empty string
        self.assertIsNone(clean_numeric_value(""))

    def test_find_matching_company(self):
        """Test the find_matching_company function"""
        company_list = [
            "Aluminium Oxid Stade GmbH",
            "Test Company GmbH",
            "Another Company AG"
        ]
        
        # Test exact match
        self.assertEqual(find_matching_company("Test_Company_GmbH", company_list), "Test Company GmbH")
        
        # Test close match
        self.assertEqual(find_matching_company("Aluminium_Oxid_Stade", company_list), "Aluminium Oxid Stade GmbH")
        
        # Test no match (score below threshold)
        self.assertIsNone(find_matching_company("Completely_Different_Name", company_list))

    @patch('sys.argv', ['compare_sachanlagen.py', '--input', 'dummy_path', '--folder', 'dummy_folder'])
    @patch('compare_sachanlagen.argparse.ArgumentParser.parse_args')
    def test_main_function(self, mock_args):
        """Test the main function with mock data"""
        # Set up mock arguments
        mock_args.return_value = MagicMock(input=self.csv_path, folder=self.json_dir)
        
        # Capture stdout to check printed statistics
        captured_output = StringIO()
        sys.stdout = captured_output
        
        # Run the main function
        main()
        
        # Reset stdout
        sys.stdout = sys.__stdout__
        
        # Check if output file was created
        output_file = 'sachanlagen_comparison_results.csv'
        self.assertTrue(os.path.exists(output_file))
        
        # Check the content of the output file
        results_df = pd.read_csv(output_file)
        
        # Clean up output file
        if os.path.exists(output_file):
            os.remove(output_file)
        
        # Verify number of results
        self.assertEqual(len(results_df), 3)
        
        # Check that the output contains expected companies
        company_names = results_df['company_name'].tolist()
        self.assertIn("Aluminium Oxid Stade GmbH", company_names)
        self.assertIn("Test Company GmbH", company_names)
        self.assertIn("Mismatched Values Corp", company_names)
        
        # Check statistics in the captured output
        output_text = captured_output.getvalue()
        self.assertIn("Total JSON files: 3", output_text)
        self.assertIn("Matched companies: 3", output_text)
        self.assertIn("Matched start values: 2", output_text)
        self.assertIn("Matched end values: 2", output_text)
        self.assertIn("Mismatched start values: 1", output_text)
        self.assertIn("Mismatched end values: 1", output_text)
        
    def test_main_with_real_arguments(self):
        """Test the main function with actual command line arguments"""
        # Save original arguments
        original_argv = sys.argv
        
        try:
            # Set up test arguments
            sys.argv = ['compare_sachanlagen.py', '--input', self.csv_path, '--folder', self.json_dir]
            
            # Run the main function
            main()
            
            # Check if output file was created
            output_file = 'sachanlagen_comparison_results.csv'
            self.assertTrue(os.path.exists(output_file))
            
            # Read results
            results_df = pd.read_csv(output_file)
            
            # Check match status for Aluminium Oxid Stade GmbH
            aluminium_row = results_df[results_df['company_name'] == "Aluminium Oxid Stade GmbH"].iloc[0]
            self.assertEqual(aluminium_row['start_match_status'], "Match")
            self.assertEqual(aluminium_row['end_match_status'], "Match")
            
            # Check match status for Test Company GmbH
            test_company_row = results_df[results_df['company_name'] == "Test Company GmbH"].iloc[0]
            self.assertEqual(test_company_row['start_match_status'], "Match")
            self.assertEqual(test_company_row['end_match_status'], "Match")
            
            # Check match status for Mismatched Values Corp
            mismatched_row = results_df[results_df['company_name'] == "Mismatched Values Corp"].iloc[0]
            self.assertEqual(mismatched_row['start_match_status'], "Mismatch")
            self.assertEqual(mismatched_row['end_match_status'], "Mismatch")
            
        finally:
            # Restore original arguments
            sys.argv = original_argv
            
            # Clean up output file
            if os.path.exists('sachanlagen_comparison_results.csv'):
                os.remove('sachanlagen_comparison_results.csv')

    def test_main_with_missing_csv(self):
        """Test main function with missing CSV file"""
        # Test with non-existent CSV file
        with patch('sys.argv', ['compare_sachanlagen.py', '--input', 'nonexistent.csv', '--folder', self.json_dir]):
            # Capture stdout to check error message
            captured_output = StringIO()
            sys.stdout = captured_output
            
            main()
            
            sys.stdout = sys.__stdout__
            
            output_text = captured_output.getvalue()
            self.assertIn("Error reading CSV file", output_text)


if __name__ == "__main__":
    unittest.main()