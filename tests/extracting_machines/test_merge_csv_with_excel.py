import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

import extracting_machines.merge_csv_with_excel as mcwe


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

        # Create sample Sachanlagen data for testing
        self.sachanlagen_data = pd.DataFrame({
            'company_name': ['Test Company', 'Another Company', 'Third GmbH & Co. KG'],
            'sachanlagen': ['500000', '1000000', '1500000'],
            'table_name': ['Aktiva', 'Aktiva', 'Aktiva'],
            'is_teuro': ['False', 'False', 'False']
        })

        # Create temp files for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.csv_path = os.path.join(self.temp_dir.name, 'test_machines.csv')
        self.xlsx_path = os.path.join(self.temp_dir.name, 'test_companies.xlsx')
        self.sachanlagen_path = os.path.join(self.temp_dir.name, 'test_sachanlagen.csv')

        # Save test data to temp files
        self.csv_data.to_csv(self.csv_path, index=False)
        self.xlsx_data.to_excel(self.xlsx_path, index=False)
        self.sachanlagen_data.to_csv(self.sachanlagen_path, index=False)

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
        company_list = ['Test Company', 'Another Company', 'Third GmbH & Co. KG', 'Jäkel GmbH & Co. KG']

        # Test exact match
        match, score = mcwe.find_best_match('Test Company', company_list)
        print(f"Match exact: {match}, Score: {score}")
        self.assertEqual(match, 'Test Company')
        self.assertEqual(score, 1.0)

        # Test close match
        match, score = mcwe.find_best_match('Jaekel_GmbH_and_Co._KG', company_list)
        print(f"Match close: {match}, Score: {score}")
        self.assertEqual(match, 'Jäkel GmbH & Co. KG')
        self.assertGreaterEqual(score, 0.85)

        # Test no good match
        match, score = mcwe.find_best_match('Completely Different', company_list, threshold=0.85)
        print(f"Match wrong: {match}, Score: {score}")
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
        # Mock the filtered dataframe that results from the indexing operation
        filtered_df = MagicMock()
        mock_merged_df.__getitem__.return_value = filtered_df
        mock_save.return_value = 'output_file.csv'

        # Call the function
        mcwe.main(csv_file_path=self.csv_path, top_n=2)

        # Verify calls
        mock_load.assert_called_once_with(self.csv_path)
        mock_analyze.assert_called_once()
        mock_mapping.assert_called_once()
        mock_merge.assert_called_once_with(mock_xlsx_df, mock_machine_data, {'Test Company': 'Test Company'}, 2)
        mock_save.assert_called_once_with(filtered_df, self.csv_path)

    def test_load_sachanlagen_data(self):
        """
        Test the load_sachanlagen_data function for loading Sachanlagen data.

        Scenarios tested:
        1. Valid Sachanlagen CSV loading
        2. Correct company name normalization
        3. Handling of missing required columns
        4. Handling of file not found errors

        Validates that the function correctly loads Sachanlagen data from CSV files
        and properly normalizes company names.
        """
        # Test valid file loading
        result = mcwe.load_sachanlagen_data(self.sachanlagen_path)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('company_name', result.columns)
        self.assertIn('sachanlagen', result.columns)

        # Test with file that doesn't exist
        result = mcwe.load_sachanlagen_data('non_existent_file.csv')
        self.assertTrue(result.empty)

        # Test with file missing required columns
        invalid_csv_path = os.path.join(self.temp_dir.name, 'invalid_sachanlagen.csv')
        pd.DataFrame({'wrong_column': ['data']}).to_csv(invalid_csv_path, index=False)
        result = mcwe.load_sachanlagen_data(invalid_csv_path)
        self.assertTrue(result.empty)

    def test_create_sachanlagen_mapping(self):
        """
        Test the create_sachanlagen_mapping function for mapping Sachanlagen companies to Excel companies.

        Scenarios tested:
        1. Correct mapping creation for exact matches
        2. Fuzzy matching for similar company names
        3. No matching for completely different names

        Validates that the function correctly maps company names between Sachanlagen and Excel data
        using fuzzy matching algorithm.
        """
        sachanlagen_df = mcwe.load_sachanlagen_data(self.sachanlagen_path)

        # Test with exact matches
        mapping = mcwe.create_sachanlagen_mapping(sachanlagen_df, self.xlsx_data)
        self.assertEqual(len(mapping), 3)
        self.assertEqual(mapping.get('Test Company'), 'Test Company')
        self.assertEqual(mapping.get('Another Company'), 'Another Company')

        # Test with modified company names (fuzzy matching)
        modified_data = self.sachanlagen_data.copy()
        modified_data['company_name'] = ['Test_Company', 'Another_Company_GmbH', 'Third_GmbH_and_Co_KG']
        modified_path = os.path.join(self.temp_dir.name, 'modified_sachanlagen.csv')
        modified_data.to_csv(modified_path, index=False)

        modified_df = mcwe.load_sachanlagen_data(modified_path)
        mapping = mcwe.create_sachanlagen_mapping(modified_df, self.xlsx_data)
        self.assertGreaterEqual(len(mapping), 2)  # At least 2 should match with fuzzy matching

        # Check specific matches with fuzzy matching tolerance
        if 'Test_Company' in mapping:
            self.assertEqual(mapping.get('Test_Company'), 'Test Company')
        if 'Third_GmbH_and_Co_KG' in mapping:
            self.assertEqual(mapping.get('Third_GmbH_and_Co_KG'), 'Third GmbH & Co. KG')

    def test_merge_with_sachanlagen(self):
        """
        Test the merge_with_sachanlagen function for adding Sachanlagen data to merged dataset.

        Scenarios tested:
        1. Correct Sachanlagen values added to matching companies
        2. No Sachanlagen values for non-matching companies
        3. Original dataframe columns preserved

        Validates that the function correctly adds Sachanlagen values to the merged dataframe
        based on company name mapping.
        """
        # Create test merged dataframe
        merged_df = pd.DataFrame({
            'Firma1': ['Test Company', 'Another Company', 'Third GmbH & Co. KG', 'No Match Company'],
            'URL': ['http://test.com', 'http://another.com', 'http://third.com', 'http://nomatch.com'],
            'Top1_Machine': [500000, 800000, 1200000, 300000]
        })

        sachanlagen_df = mcwe.load_sachanlagen_data(self.sachanlagen_path)
        mapping = {'Test Company': 'Test Company', 'Another Company': 'Another Company', 'Third GmbH & Co. KG': 'Third GmbH & Co. KG'}

        # Merge Sachanlagen data
        result = mcwe.merge_with_sachanlagen(merged_df, sachanlagen_df, mapping)

        # Check if Sachanlagen column exists
        self.assertIn('Sachanlagen', result.columns)

        # Check if Sachanlagen values are correctly mapped
        for company in ['Test Company', 'Another Company', 'Third GmbH & Co. KG']:
            sachanlagen_value = self.sachanlagen_data.loc[self.sachanlagen_data['company_name'] == company, 'sachanlagen'].values[0]
            result_value = str(result.loc[result['Firma1'] == company, 'Sachanlagen'].values[0])
            self.assertEqual(result_value, sachanlagen_value)

        # Check if non-matching company has NaN for Sachanlagen
        self.assertTrue(pd.isna(result.loc[result['Firma1'] == 'No Match Company', 'Sachanlagen'].values[0]))

    @patch('extracting_machines.merge_csv_with_excel.load_data')
    @patch('extracting_machines.merge_csv_with_excel.analyze_company_similarities')
    @patch('extracting_machines.merge_csv_with_excel.create_company_mapping')
    @patch('extracting_machines.merge_csv_with_excel.merge_datasets')
    @patch('extracting_machines.merge_csv_with_excel.load_sachanlagen_data')
    @patch('extracting_machines.merge_csv_with_excel.create_sachanlagen_mapping')
    @patch('extracting_machines.merge_csv_with_excel.merge_with_sachanlagen')
    @patch('extracting_machines.merge_csv_with_excel.save_merged_data')
    def test_main_function_with_sachanlagen(self, mock_save, mock_merge_sachanlagen, mock_sachanlagen_mapping,
                                            mock_load_sachanlagen, mock_merge, mock_mapping, mock_analyze, mock_load):
        """
        Test the main orchestration function with Sachanlagen path and mocked dependencies.

        This test uses mocking to isolate the main function from its dependencies,
        allowing verification that the function correctly handles Sachanlagen data by:
        1. Loading Sachanlagen data when path is provided
        2. Creating Sachanlagen mapping
        3. Merging Sachanlagen data into the final output
        4. Passing correct parameters between function calls

        The test verifies the integration between components with Sachanlagen data included.
        """
        # Setup mock returns
        mock_machine_data = MagicMock()
        mock_xlsx_df = MagicMock()
        mock_load.return_value = (mock_machine_data, mock_xlsx_df)

        mock_mapping.return_value = {'Test Company': 'Test Company'}
        mock_merged_df = MagicMock()
        mock_merge.return_value = mock_merged_df

        mock_sachanlagen_df = MagicMock()
        mock_load_sachanlagen.return_value = mock_sachanlagen_df
        mock_sachanlagen_mapping.return_value = {'Test Company': 'Test Company'}

        mock_merged_with_sachanlagen_df = MagicMock()
        mock_merge_sachanlagen.return_value = mock_merged_with_sachanlagen_df

        mock_save.return_value = 'output_file.csv'

        # Call the function with Sachanlagen path
        with patch('os.path.exists', return_value=True):  # Mock os.path.exists to return True
            mcwe.main(csv_file_path=self.csv_path, top_n=2, sachanlagen_path=self.sachanlagen_path)

        # Verify calls
        mock_load.assert_called_once_with(self.csv_path)
        mock_analyze.assert_called_once()
        mock_mapping.assert_called_once()
        mock_merge.assert_called_once_with(mock_xlsx_df, mock_machine_data, {'Test Company': 'Test Company'}, 2)

        # Verify Sachanlagen-related calls
        mock_load_sachanlagen.assert_called_once_with(self.sachanlagen_path)
        mock_sachanlagen_mapping.assert_called_once_with(mock_sachanlagen_df, mock_xlsx_df)
        mock_merge_sachanlagen.assert_called_once_with(mock_merged_df, mock_sachanlagen_df, {'Test Company': 'Test Company'})

        # Verify save was called with the correct file path
        # We don't assert on the first argument (dataframe) because it's filtered in the implementation
        mock_save.assert_called_once()
        args, kwargs = mock_save.call_args
        self.assertEqual(args[1], self.csv_path)  # Check that the second argument is the csv_path

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
            mcwe.main(csv_file_path=self.csv_path, top_n=2)

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

    def test_integration_with_sachanlagen(self):
        """
        End-to-end integration test for the CSV-Excel merging process with Sachanlagen data.

        This comprehensive test:
        1. Executes the main function with test files including Sachanlagen data
        2. Verifies the creation of the output file with expected naming pattern
        3. Validates that the output CSV contains all required columns including Sachanlagen
        4. Checks that Sachanlagen values are correctly merged for each company
        5. Ensures proper cleanup of test artifacts

        This test validates the complete workflow with Sachanlagen data included.
        """
        try:
            # Call the main function with Sachanlagen path
            mcwe.main(csv_file_path=self.csv_path, top_n=2, sachanlagen_path=self.sachanlagen_path)

            # Get expected output filename based on current date
            current_date = datetime.now().strftime('%Y%m%d')
            expected_filename = f"merged_data_{current_date}.csv"

            # Verify output file was created
            self.assertTrue(os.path.exists(expected_filename),
                            f"Output file {expected_filename} was not created")

            # Read the output file to validate its contents
            output_df = pd.read_csv(expected_filename)

            # Verify Sachanlagen column exists
            self.assertIn('Sachanlagen', output_df.columns,
                          "Sachanlagen column not found in output file")

            # Check if Sachanlagen values are correctly mapped
            for company_name in self.sachanlagen_data['company_name']:
                # Find this company in the output file
                company_row = output_df[output_df['Firma1'] == company_name]

                if len(company_row) > 0:
                    # Get the Sachanlagen value from our test data for this company
                    sachanlagen_value = self.sachanlagen_data.loc[
                        self.sachanlagen_data['company_name'] == company_name, 'sachanlagen'].values[0]

                    # Get the actual value from the output file
                    actual_value = str(company_row['Sachanlagen'].values[0])

                    # Verify the value matches what we expect
                    self.assertEqual(actual_value, sachanlagen_value,
                                     f"Sachanlagen value mismatch for {company_name}: "
                                     f"expected {sachanlagen_value}, got {actual_value}")

            # Clean up the created file after testing
            if os.path.exists(expected_filename):
                os.remove(expected_filename)

        except Exception as e:
            # Clean up any created file even if test fails
            current_date = datetime.now().strftime('%Y%m%d')
            expected_filename = f"merged_data_{current_date}.csv"
            if os.path.exists(expected_filename):
                os.remove(expected_filename)
            self.fail(f"Integration test with Sachanlagen failed with exception: {str(e)}")


if __name__ == '__main__':
    unittest.main()
