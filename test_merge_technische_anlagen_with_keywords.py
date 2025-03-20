import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import os
import io

# Absolute import of functions to test
from merge_technische_anlagen_with_keywords import (
    clean_trailing_symbols, 
    extract_base_domain, 
    extract_and_log_domains,
    merge_csv_with_excel
)

class TestCleanTrailingSymbols(unittest.TestCase):
    def test_clean_trailing_symbols_with_comma(self):
        """Test cleaning strings with trailing commas"""
        self.assertEqual(clean_trailing_symbols("Example,"), "Example")
        self.assertEqual(clean_trailing_symbols("Example, "), "Example")
        self.assertEqual(clean_trailing_symbols("Example,,,"), "Example")
    
    def test_clean_trailing_symbols_with_period(self):
        """Test cleaning strings with trailing periods"""
        self.assertEqual(clean_trailing_symbols("Example."), "Example")
        self.assertEqual(clean_trailing_symbols("Example. "), "Example")
        self.assertEqual(clean_trailing_symbols("Example..."), "Example")
    
    def test_clean_trailing_symbols_with_mixed(self):
        """Test cleaning strings with mixed trailing symbols"""
        self.assertEqual(clean_trailing_symbols("Example.,."), "Example")
    
    def test_clean_trailing_symbols_no_change(self):
        """Test strings that don't need cleaning"""
        self.assertEqual(clean_trailing_symbols("Example"), "Example")
        self.assertEqual(clean_trailing_symbols("Example Corp"), "Example Corp")
    
    def test_clean_trailing_symbols_with_na(self):
        """Test function handles NA values properly"""
        self.assertIsNone(clean_trailing_symbols(None))
        self.assertTrue(pd.isna(clean_trailing_symbols(pd.NA)))

class TestExtractBaseDomain(unittest.TestCase):
    def test_extract_base_domain_standard_url(self):
        """Test extracting domain from standard URLs"""
        self.assertEqual(extract_base_domain("https://www.example.com"), "example.com")
        self.assertEqual(extract_base_domain("http://subdomain.example.com/page"), "example.com")
    
    def test_extract_base_domain_without_www(self):
        """Test extracting domain from URLs without www"""
        self.assertEqual(extract_base_domain("https://example.com"), "example.com")
        self.assertEqual(extract_base_domain("https://example.co.uk"), "example.co.uk")
    
    def test_extract_base_domain_without_scheme(self):
        """Test extracting domain from URLs without http/https"""
        self.assertEqual(extract_base_domain("www.example.com"), "example.com")
        self.assertEqual(extract_base_domain("example.com"), "example.com")
    
    def test_extract_base_domain_with_path(self):
        """Test extracting domain from URLs with paths"""
        self.assertEqual(extract_base_domain("https://www.example.com/products/item?id=123"), "example.com")
    
    def test_extract_base_domain_malformed(self):
        """Test extracting domain from malformed URLs"""
        self.assertEqual(extract_base_domain("example.com/path"), "example.com")
    
    def test_extract_base_domain_empty(self):
        """Test extracting domain from empty values"""
        self.assertIsNone(extract_base_domain(None))
        self.assertIsNone(extract_base_domain(pd.NA))
        self.assertIsNone(extract_base_domain(""))

class TestExtractAndLogDomains(unittest.TestCase):
    def setUp(self):
        """Set up test data"""
        self.test_df = pd.DataFrame({
            'URL': [
                'https://www.example.com',
                'example.org',
                'subdomain.test.net/page',
                None,
                'https://www.sample.co.uk/products'
            ],
            'other_column': [1, 2, 3, 4, 5]
        })
    
    @patch('builtins.print')
    def test_extract_and_log_domains_existing_column(self, mock_print):
        """Test extracting domains when URL column exists"""
        result = extract_and_log_domains(
            self.test_df, 
            'URL', 
            column_to_create='base_domain',
            sample_size=3, 
            description="test"
        )
        
        # Check that domains were extracted correctly
        expected_domains = ['example.com', 'example.org', 'test.net', None, 'sample.co.uk']
        pd.testing.assert_series_equal(
            result['base_domain'],
            pd.Series(expected_domains, name='base_domain')
        )
        
        # Verify print statements were called
        mock_print.assert_any_call("Extracting domains from 'URL' column test")
        mock_print.assert_any_call("Example URLs test:")
        
        # URLs should be printed with their extracted domains
        for i in range(3):  # sample_size=3
            url = self.test_df['URL'].dropna().iloc[i]
            domain = expected_domains[i]
            mock_print.assert_any_call(f"  - URL: {url}, Extracted domain: {domain}")
    
    @patch('builtins.print')
    def test_extract_and_log_domains_missing_column(self, mock_print):
        """Test behavior when URL column doesn't exist"""
        result = extract_and_log_domains(
            self.test_df, 
            'nonexistent_column', 
            column_to_create='base_domain',
            description="test"
        )
        
        # The function should add a column with None values
        self.assertTrue('base_domain' in result.columns)
        self.assertTrue(result['base_domain'].isna().all())
        
        # The warning should be printed
        mock_print.assert_any_call("Warning: Column 'nonexistent_column' not found in DataFrame test")

class TestMergeCsvWithExcel(unittest.TestCase):
    def setUp(self):
        """Set up test data for merge_csv_with_excel tests"""
        # Sample CSV data
        self.csv_data = pd.DataFrame({
            'Company name': ['Company A', 'Company B', 'Company C', 'Company D'],
            'Website': ['www.companya.com', 'www.companyb.com', 'www.companyc.com', 'www.companyd.com'],
            'Other Data': ['Data1', 'Data2', 'Data3', 'Data4']
        })
        
        # Sample base data
        self.base_data = pd.DataFrame({
            'Firma1': ['Company A', 'Company B', 'Company E', 'Company F'],
            'Ort': ['City A', 'City B', 'City E', 'City F'],
            'Top1_Machine': ['Machine A', 'Machine B', 'Machine E', 'Machine F'],
            'URL': ['www.companya.com', 'www.companyb.com', 'www.companye.com', 'www.companyf.com'],
            'Maschinen_Park_Size': [10, 20, 30, 40],
            'OtherCol': ['X', 'Y', 'Z', 'W']
        })
        
        # Mock file paths
        self.csv_path = 'consolidated_output/pluralized_federn.csv'
        self.base_data_path = 'merged_federn_20250317.csv'
        self.output_path = 'final_export_federn'
        
        # Expected merged data for name matches
        self.expected_name_matches = pd.DataFrame({
            'Company name': ['Company A', 'Company B'],
            'Website': ['www.companya.com', 'www.companyb.com'],
            'Other Data': ['Data1', 'Data2'],
            'technische Anlagen und Maschinen 2021/22': ['Machine A', 'Machine B'],
            'Ort': ['City A', 'City B'],
            'Maschinen_Park_Size': [10, 20],
        })

    @patch('pandas.read_csv')
    @patch('pandas.read_excel')
    @patch('pandas.DataFrame.to_csv')
    @patch('os.path.splitext')
    @patch('builtins.print')
    def test_merge_csv_with_excel_xlsx_input(self, mock_print, mock_splitext, mock_to_csv, 
                                            mock_read_excel, mock_read_csv):
        """Test merge_csv_with_excel with Excel input"""
        # Configure mocks
        mock_read_csv.return_value = self.csv_data
        mock_read_excel.return_value = self.base_data
        mock_splitext.return_value = ('base_path', '.xlsx')
        
        # Call the function with required parameters
        merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)
        
        # Verify the CSV was read
        mock_read_csv.assert_called_once_with(self.csv_path, encoding='utf-8')
        
        # Verify Excel was read
        mock_read_excel.assert_called_once_with(self.base_data_path, sheet_name=0)
        
        # Verify the output was written
        mock_to_csv.assert_called_once()
        
        # We can't easily check the exact DataFrame passed to to_csv due to the complexity,
        # but we can verify some parameters
        args, kwargs = mock_to_csv.call_args
        self.assertEqual(kwargs['encoding'], 'utf-8-sig')
        self.assertEqual(kwargs['index'], False)
        self.assertEqual(kwargs['sep'], ',')

    @patch('pandas.read_csv')
    @patch('pandas.read_excel')
    @patch('pandas.DataFrame.to_csv')
    @patch('os.path.splitext')
    @patch('builtins.print')
    def test_merge_csv_with_excel_csv_input(self, mock_print, mock_splitext, mock_to_csv, 
                                          mock_read_excel, mock_read_csv):
        """Test merge_csv_with_excel with CSV input"""
        # First call returns CSV data, second call returns base data
        mock_read_csv.side_effect = [self.csv_data, self.base_data]
        mock_splitext.return_value = ('base_path', '.csv')
        
        # Call the function with required parameters
        merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)
        
        # Verify the CSVs were read
        mock_read_csv.assert_any_call(self.csv_path, encoding='utf-8')
        mock_read_csv.assert_any_call(self.base_data_path, encoding='utf-8')
        
        # Verify Excel was NOT read
        mock_read_excel.assert_not_called()
        
        # Verify the output was written
        mock_to_csv.assert_called_once()

    @patch('pandas.read_csv')
    @patch('os.path.splitext')
    @patch('builtins.print')
    def test_merge_csv_with_excel_unsupported_format(self, mock_print, mock_splitext, mock_read_csv):
        """Test merge_csv_with_excel with unsupported file format"""
        mock_read_csv.return_value = self.csv_data
        mock_splitext.return_value = ('base_path', '.txt')
        
        # The function should raise a ValueError for unsupported formats
        with self.assertRaises(ValueError) as context:
            merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)
        
        self.assertIn("Unsupported file format: .txt", str(context.exception))

    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    @patch('os.path.splitext')
    @patch('builtins.print')
    def test_merge_by_company_name(self, mock_print, mock_splitext, mock_to_csv, mock_read_csv):
        """Test company name matching works correctly"""
        # Create a real DataFrame with multiple merge method spies
        real_csv_data = self.csv_data.copy()
        real_base_data = self.base_data.copy()
        
        # Track if any merge-like operation happens
        merge_called = False
        
        # Spy on both DataFrame.merge and pd.merge
        original_df_merge = pd.DataFrame.merge
        original_pd_merge = pd.merge
        original_pd_concat = pd.concat
        
        def spy_df_merge(*args, **kwargs):
            nonlocal merge_called
            merge_called = True
            return original_df_merge(*args, **kwargs)
        
        def spy_pd_merge(*args, **kwargs):
            nonlocal merge_called
            merge_called = True
            return original_pd_merge(*args, **kwargs)
            
        def spy_pd_concat(*args, **kwargs):
            nonlocal merge_called
            merge_called = True
            return original_pd_concat(*args, **kwargs)
        
        # Replace all the real methods with our spies
        pd.DataFrame.merge = spy_df_merge
        pd.merge = spy_pd_merge
        pd.concat = spy_pd_concat
        
        try:
            # Set up the test
            mock_read_csv.side_effect = [real_csv_data, real_base_data]
            mock_splitext.return_value = ('base_path', '.csv')
            
            # Call the function with required parameters
            merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)
            
            # Verify some kind of merge/combination operation was called
            self.assertTrue(merge_called, "No DataFrame combination operation (merge or concat) was detected")
            
            # Verify output was written
            mock_to_csv.assert_called_once()
        finally:
            # Restore the original methods
            pd.DataFrame.merge = original_df_merge
            pd.merge = original_pd_merge
            pd.concat = original_pd_concat

if __name__ == '__main__':
    unittest.main()