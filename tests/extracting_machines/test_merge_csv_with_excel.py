import unittest
import pandas as pd
import numpy as np
import os
import tempfile
from unittest.mock import patch, MagicMock
import extracting_machines.merge_csv_with_excel as mcwe
from datetime import datetime


class TestMergeCsvWithExcel(unittest.TestCase):
    """
    Test suite for the merge_csv_with_excel module.
    
    This test suite validates the functionality of merging machine data from CSV files with 
    company information from Excel files. It includes tests for individual utility functions,
    data processing functions, and end-to-end integration tests.
    
    Test data includes sample company information and machine values that are used to verify
    the correct calculation of machine park sizes and proper merging of datasets.
    """

    def setUp(self):
        """
        Set up test fixtures before each test.
        
        Creates sample CSV and Excel data with test companies and machine values.
        Sets up temporary directory and files for testing to ensure tests don't 
        modify actual files or leave residual test files.
        """
        # Create sample data for testing
        self.csv_data = pd.DataFrame({
            'Company': ['Test Company', 'Another Company', 'Third GmbH & Co. KG'],
            'Machine_1': [500000, 800000, 1200000],
            'Machine_2': [300000, 600000, 900000],
            'Machine_3': [100000, 400000, 600000]
        })
        
        self.xlsx_data = pd.DataFrame({
            'Firma1': ['Test Company', 'Another Company', 'Third GmbH & Co. KG'],
            'URL': ['http://test.com', 'http://another.com', 'http://third.com'],
            'Ort': ['Berlin', 'Munich', 'Hamburg']
        })
        
        # Create temp files for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.csv_path = os.path.join(self.temp_dir.name, 'test_machines.csv')
        self.xlsx_path = os.path.join(self.temp_dir.name, 'test_companies.xlsx')
        
        # Save test data to temp files
        self.csv_data.to_csv(self.csv_path, index=False)
        self.xlsx_data.to_excel(self.xlsx_path, index=False)
    
    def tearDown(self):
        """
        Clean up test fixtures after each test.
        
        Removes the temporary directory and all files created during testing
        to ensure no test artifacts remain on the file system.
        """
        self.temp_dir.cleanup()
    
    def test_normalize_company_name(self):
        """
        Test the normalize_company_name function with various input scenarios.
        
        Scenarios tested:
        1. Standard GmbH & Co. KG variations (with different punctuation)
        2. Extra spaces in company names
        3. Non-string inputs (numeric values)
        4. None values
        
        Validates that the function correctly normalizes company names according to
        the standardization rules defined in the module.
        """
        test_cases = [
            ('Test GmbH & Co. KG.', 'Test GmbH & Co. KG'),
            ('Test GmbH & Co.KG.', 'Test GmbH & Co. KG'),
            ('Test GmbH & Co.KG', 'Test GmbH & Co. KG'),
            ('  Extra  Spaces  ', 'Extra Spaces'),
            (123, 123),  # Non-string input
            (None, None)  # None input
        ]
        
        for input_name, expected in test_cases:
            result = mcwe.normalize_company_name(input_name)
            self.assertEqual(result, expected)
    
    def test_standardize_company_name(self):
        """
        Test the standardize_company_name function for correct underscore replacement.
        
        Scenarios tested:
        1. Single underscore in company name
        2. Multiple underscores in company name
        3. No underscores in company name (should remain unchanged)
        
        Validates that the function correctly replaces underscores with spaces in
        company names, which is essential for matching companies across different data sources.
        """
        self.assertEqual(mcwe.standardize_company_name('Test_Company'), 'Test Company')
        self.assertEqual(mcwe.standardize_company_name('No_Underscores_Here'), 'No Underscores Here')
        self.assertEqual(mcwe.standardize_company_name('AlreadyStandardized'), 'AlreadyStandardized')
    
    def test_categorize_machine_park_size(self):
        """
        Test the categorize_machine_park_size function for all possible size categories.
        
        Scenarios tested:
        1. All valid size categories (10-15, 15-20, 21-40, etc.)
        2. Edge cases at category boundaries
        3. Values outside of defined categories (too small or too large)
        4. Invalid inputs (empty strings, text values, None)
        
        Validates that the function correctly categorizes machine values into
        the predefined size categories, handling all edge cases appropriately.
        """
        test_cases = [
            ('500000', '10-15'),
            ('800000', '15-20'),
            ('1000000', '21-40'),
            ('1300000', '40-60'),
            ('1600000', '60-80'),
            ('2000000', '80-120'),
            ('3000000', '120-200'),
            ('6000000', '200-350'),
            ('15000000', '350-600'),
            ('50000', 'No Match'),  # Below the lowest category
            ('100000000', 'No Match'),  # Above the highest category
            ('', ''),  # Empty string
            ('invalid', ''),  # Invalid input
            (None, '')  # None input
        ]
        
        for input_value, expected in test_cases:
            result = mcwe.categorize_machine_park_size(input_value)
            self.assertEqual(result, expected)
    
    def test_process_machine_data(self):
        """
        Test the process_machine_data function that transforms raw machine data.
        
        Scenarios tested:
        1. Correct column generation (Company, Top1_Machine, Top2_Machine, Maschinen_Park_Size)
        2. Proper company count in output
        3. Correct Maschinen_Park_Size calculation for each company based on machine values
        
        This test validates that machine data is correctly processed, top machines are
        extracted, and appropriate Maschinen_Park_Size categories are assigned based on
        machine values.
        """
        result = mcwe.process_machine_data(csv_file=self.csv_path, top_n=2)
        
        # Check if result has the right columns
        expected_columns = ['Company', 'Top1_Machine', 'Top2_Machine', 'Maschinen_Park_Size']
        self.assertListEqual(list(result.columns), expected_columns)
        
        # Check if we have the right number of companies
        self.assertEqual(len(result), 3)
        
        # Print the actual company names in the result for debugging
        print("Actual company names in result:", result['Company'].tolist())
        
        # Define expected sizes with the correct company names as they appear in result
        # Use result['Company'].unique() to get the actual company names if necessary
        expected_sizes = {
            'Test Company': '10-15',
            'Another Company': '15-20',
            'Third GmbH & Co. KG': '21-40'
        }
        
        # Check if machine park sizes are correctly calculated
        for company, expected_size in expected_sizes.items():
            matches = result[result['Company'] == company]
            self.assertFalse(matches.empty, f"Company '{company}' not found in result")
            actual_size = matches['Maschinen_Park_Size'].values[0]
            self.assertEqual(actual_size, expected_size, 
                            f"Size mismatch for {company}: expected {expected_size}, got {actual_size}")
            
    
    def test_find_best_match(self):
        """
        Test the find_best_match function that uses fuzzy matching for company names.
        
        Scenarios tested:
        1. Exact matches (should return perfect score of 1.0)
        2. Close matches with minor typos (should return match above threshold)
        3. Poor matches below threshold (should return None)
        
        This test validates the fuzzy matching algorithm used to match company names
        between different data sources, ensuring it handles similarity thresholds correctly.
        """
        company_list = ['Test Company', 'Another Company', 'Third GmbH & Co. KG']
        
        # Test exact match
        match, score = mcwe.find_best_match('Test Company', company_list)
        self.assertEqual(match, 'Test Company')
        self.assertEqual(score, 1.0)
        
        # Test close match
        match, score = mcwe.find_best_match('Test Cmpany', company_list)
        self.assertEqual(match, 'Test Company')
        self.assertGreaterEqual(score, 0.85)
        
        # Test no good match
        match, score = mcwe.find_best_match('Completely Different', company_list, threshold=0.85)
        self.assertIsNone(match)
    
    @patch('extracting_machines.merge_csv_with_excel.load_data')
    @patch('extracting_machines.merge_csv_with_excel.analyze_company_similarities')
    @patch('extracting_machines.merge_csv_with_excel.create_company_mapping')
    @patch('extracting_machines.merge_csv_with_excel.merge_datasets')
    @patch('extracting_machines.merge_csv_with_excel.save_merged_data')
    def test_main_function(self, mock_save, mock_merge, mock_mapping, mock_analyze, mock_load):
        """
        Test the main orchestration function with mocked dependencies.
        
        This test uses mocking to isolate the main function from its dependencies,
        allowing verification that the function:
        1. Calls all expected sub-functions in the correct order
        2. Passes correct parameters between function calls
        3. Handles the overall workflow properly
        
        The test verifies the integration between components without executing
        actual file operations or data processing, focusing solely on the 
        coordination logic.
        """
        # Setup mock returns
        mock_machine_data = MagicMock()
        mock_xlsx_df = MagicMock()
        mock_load.return_value = (mock_machine_data, mock_xlsx_df)
        
        mock_mapping.return_value = {'Test Company': 'Test Company'}
        mock_merged_df = MagicMock()
        mock_merge.return_value = mock_merged_df
        mock_save.return_value = 'output_file.csv'
        
        # Call the function
        mcwe.main(csv_file_path=self.csv_path, top_n=2)
        
        # Verify calls
        mock_load.assert_called_once_with(self.csv_path)
        mock_analyze.assert_called_once()
        mock_mapping.assert_called_once()
        mock_merge.assert_called_once_with(mock_xlsx_df, mock_machine_data, {'Test Company': 'Test Company'}, 2)
        mock_save.assert_called_once_with(mock_merged_df, self.csv_path)
    
    def test_integration(self):
        """
        End-to-end integration test for the entire CSV-Excel merging process.
        
        This comprehensive test:
        1. Executes the actual main function with real test files
        2. Verifies the creation of the output file with expected naming pattern
        3. Validates that the output CSV contains all required columns
        4. Checks that the Maschinen_Park_Size values are calculated correctly
           for each company based on their machine values
        5. Ensures proper cleanup of test artifacts
        
        This test validates the complete workflow from input files to output CSV,
        ensuring all components work together correctly to produce the expected result.
        """
        try:
            # Call the main function
            output_file = mcwe.main(csv_file_path=self.csv_path, top_n=2)
            
            # Get expected output filename based on current date
            current_date = datetime.now().strftime('%Y%m%d')
            expected_filename = f"merged_data_{current_date}.csv"
            
            # Verify output file was created
            self.assertTrue(os.path.exists(expected_filename), 
                           f"Output file {expected_filename} was not created")
            
            # Read the output file to validate its contents
            output_df = pd.read_csv(expected_filename)
            
            # Verify essential columns exist
            required_columns = ['Firma1', 'Maschinen_Park_Size', 'Top1_Machine', 'Top2_Machine']
            for column in required_columns:
                self.assertIn(column, output_df.columns, 
                             f"Required column {column} not found in output file")
            
            # Verify Maschinen_Park_Size column contains expected values
            self.assertGreater(len(output_df), 0, "Output file contains no data")
            
            # Check if the values match our expected sizes from test data
            # Get the companies from the output file
            for company_name in self.csv_data['Company']:
                # Find this company in the output file
                company_row = output_df[output_df['Firma1'] == company_name]
                
                if len(company_row) > 0:
                    # Get the first top machine value from our test data for this company
                    machine_value = self.csv_data.loc[self.csv_data['Company'] == company_name, 'Machine_1'].values[0]
                    expected_size = mcwe.categorize_machine_park_size(str(machine_value))
                    
                    # Get the actual size from the output file
                    actual_size = company_row['Maschinen_Park_Size'].values[0]
                    
                    # Verify the size matches what we expect
                    self.assertEqual(actual_size, expected_size,
                                   f"Maschinen_Park_Size value mismatch for {company_name}: " 
                                   f"expected {expected_size}, got {actual_size}")
            
            # Clean up the created file after testing
            if os.path.exists(expected_filename):
                os.remove(expected_filename)
                
        except Exception as e:
            # Clean up any created file even if test fails
            current_date = datetime.now().strftime('%Y%m%d')
            expected_filename = f"merged_data_{current_date}.csv"
            if os.path.exists(expected_filename):
                os.remove(expected_filename)
            self.fail(f"Integration test failed with exception: {str(e)}")


if __name__ == '__main__':
    unittest.main()
