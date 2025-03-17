import unittest
import pandas as pd
import numpy as np
import os
import tempfile
from unittest.mock import patch, MagicMock
import merge_csv_with_excel as mcwe


class TestMergeCsvWithExcel(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        # Create sample data for testing
        self.csv_data = pd.DataFrame({
            'Company': ['Test Company', 'Another_Company', 'Third GmbH & Co.KG.'],
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
        """Clean up test fixtures"""
        self.temp_dir.cleanup()
    
    def test_normalize_company_name(self):
        """Test the normalize_company_name function"""
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
        """Test the standardize_company_name function"""
        self.assertEqual(mcwe.standardize_company_name('Test_Company'), 'Test Company')
        self.assertEqual(mcwe.standardize_company_name('No_Underscores_Here'), 'No Underscores Here')
        self.assertEqual(mcwe.standardize_company_name('AlreadyStandardized'), 'AlreadyStandardized')
    
    def test_categorize_machine_park_size(self):
        """Test the categorize_machine_park_size function"""
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
        """Test the process_machine_data function"""
        result = mcwe.process_machine_data(csv_file=self.csv_path, top_n=2)
        
        # Check if result has the right columns
        expected_columns = ['Company', 'Top1_Machine', 'Top2_Machine', 'Maschinen_Park_Size']
        self.assertListEqual(list(result.columns), expected_columns)
        
        # Check if we have the right number of companies
        self.assertEqual(len(result), 3)
        
        # Check if machine park sizes are correctly calculated
        expected_sizes = {
            'Test Company': '10-15',
            'Another Company': '15-20',
            'Third GmbH & Co. KG': '21-40',
            'Wrong Company': 'No Match'
        }
        for company, expected_size in expected_sizes.items():
                actual_size = result[result['Company'] == company]['Maschinen_Park_Size'].values[0]
                self.assertEqual(actual_size, expected_size)
        
        
    
    def test_find_best_match(self):
        """Test the find_best_match function"""
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
    
    @patch('merge_csv_with_excel.load_data')
    @patch('merge_csv_with_excel.analyze_company_similarities')
    @patch('merge_csv_with_excel.create_company_mapping')
    @patch('merge_csv_with_excel.merge_datasets')
    @patch('merge_csv_with_excel.save_merged_data')
    def test_main_function(self, mock_save, mock_merge, mock_mapping, mock_analyze, mock_load):
        """Test the main function with mocks"""
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
        mock_save.assert_called_once_with(mock_merged_df)
    
    def test_integration(self):
        """Basic integration test with real files"""
        try:
            mcwe.main(csv_file_path=self.csv_path, top_n=2)
            # If we got here without exceptions, consider it a success
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Integration test failed with exception: {str(e)}")


if __name__ == '__main__':
    unittest.main()
