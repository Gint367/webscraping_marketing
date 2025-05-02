import os
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from merge_pipeline.enrich_data import HOURS_MULTIPLIER, extract_first_number, main

# Import the function to test using absolute import


class TestEnrichData(unittest.TestCase):

    def test_extract_first_number_valid_range(self):
        """Test extract_first_number with valid range inputs"""
        self.assertEqual(extract_first_number('15-20'), 15)
        self.assertEqual(extract_first_number('100-200'), 100)
        self.assertEqual(extract_first_number('5'), 5)

    def test_extract_first_number_invalid_inputs(self):
        """Test extract_first_number with invalid inputs"""
        self.assertIsNone(extract_first_number('No Match'))
        self.assertIsNone(extract_first_number(''))
        self.assertIsNone(extract_first_number(None))
        self.assertIsNone(extract_first_number('abc'))

    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    def test_main_happy_path(self, mock_exists, mock_args, mock_to_csv, mock_read_csv):
        """Test main function with valid inputs"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv', log_level='INFO')
        mock_exists.return_value = True

        # Create a mock DataFrame
        data = {
            'Maschinen_Park_Size': ['15-20', '30', 'No Match', '10-15']
        }
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df

        # Execute
        main()

        # Assert
        # Verify the input file was read
        mock_read_csv.assert_called_once_with('test.csv', encoding='utf-8', skipinitialspace=True)

        # Verify the output file path is correct
        output_path = os.path.join(os.path.dirname('test.csv'), 'enriched_test.csv')

        # Verify to_csv was called with correct parameters
        mock_to_csv.assert_called_once_with(output_path, index=False, encoding='utf-8-sig')

        # Verify the DataFrame has the expected columns
        self.assertTrue('Maschinen_Park_var' in df.columns)
        self.assertTrue('hours_of_saving' in df.columns)

        # Verify the expected data transformations were applied - use values for comparison
        expected_values = [15, 30, None, 10]
        actual_values = df['Maschinen_Park_var'].tolist()
        for i, (expected, actual) in enumerate(zip(expected_values, actual_values)):
            if expected is None:
                # Check for pandas NA value (pd.NA or pd.isna)
                self.assertTrue(pd.isna(actual), f"Value at index {i} should be NA/None but got {actual}")
            else:
                self.assertEqual(expected, actual, f"Mismatch at index {i}")

        # Verify hours_of_saving calculation - use values for comparison
        for i in range(len(df)):
            if not pd.isna(df['Maschinen_Park_var'].iloc[i]):
                expected = df['Maschinen_Park_var'].iloc[i] * HOURS_MULTIPLIER
                actual = df['hours_of_saving'].iloc[i]
                self.assertEqual(expected, actual, f"Hours calc mismatch at index {i}")
            else:
                self.assertTrue(pd.isna(df['hours_of_saving'].iloc[i]),
                                f"Expected NA at index {i} but got {df['hours_of_saving'].iloc[i]}")

        # Verify the number of valid records
        valid_records = df['Maschinen_Park_var'].count()
        self.assertEqual(valid_records, 3)

    @patch('pandas.read_csv')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    def test_main_file_not_found(self, mock_exists, mock_args, mock_read_csv):
        """Test main function when file is not found"""
        # Setup
        mock_args.return_value = Mock(input_file='nonexistent.csv', log_level='INFO')
        mock_exists.return_value = False  # Simulate file not found

        # Execute and expect a FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            main()

    @patch('pandas.read_csv')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    def test_main_missing_required_columns(self, mock_exists, mock_args, mock_read_csv):
        """Test main function when required columns are missing"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv', log_level='INFO')
        mock_exists.return_value = True

        # Create a mock DataFrame without the required column
        data = {'Some_Other_Column': ['value1', 'value2']}
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df

        # Execute and expect a ValueError
        with self.assertRaises(ValueError) as context:
            main()

        # Assert the error message contains the expected text
        self.assertIn("Missing required columns: Maschinen_Park_Size", str(context.exception))

    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.exists')
    def test_main_error_saving_file(self, mock_exists, mock_args, mock_to_csv, mock_read_csv):
        """Test main function when there's an error saving the output file"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv', log_level='INFO')
        mock_exists.return_value = True

        # Create a mock DataFrame
        data = {'Maschinen_Park_Size': ['15-20', '30']}
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df

        # Mock to_csv to raise an exception
        mock_to_csv.side_effect = Exception("Permission denied")

        # Execute and expect a ValueError
        with self.assertRaises(ValueError) as context:
            main()

        # Assert the error message contains the expected text
        self.assertIn("Error saving output file: Permission denied", str(context.exception))

if __name__ == '__main__':
    unittest.main()
