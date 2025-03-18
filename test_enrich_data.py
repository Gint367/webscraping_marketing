import unittest
from unittest.mock import patch, Mock
import pandas as pd
import io
import os
from enrich_data import extract_first_number, main, HOURS_MULTIPLIER

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
    def test_main_happy_path(self, mock_args, mock_to_csv, mock_read_csv):
        """Test main function with valid inputs"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv')
        
        # Create a mock DataFrame
        data = {
            'Maschinen_Park_Size': ['15-20', '30', 'No Match', '10-15']
        }
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df
        
        # Execute
        with patch('sys.stdout', new=io.StringIO()) as fake_out:
            main()
        
        # Assert
        self.assertTrue(mock_read_csv.called)
        self.assertTrue(mock_to_csv.called)
        
        # Check if correct file path was used for output
        output_path = os.path.join(os.path.dirname('test.csv'), 'enriched_test.csv')
        mock_to_csv.assert_called_with(output_path, index=False, encoding='utf-8-sig')
        
        # Check stdout for expected output messages
        output = fake_out.getvalue()
        self.assertIn("Reading data from test.csv", output)
        self.assertIn("Creating Maschinen_Park_var column", output)
        self.assertIn(f"Creating hours_of_saving column (Maschinen_Park_var × {HOURS_MULTIPLIER})", output)
        self.assertIn("Data enrichment completed successfully", output)
        self.assertIn("Processed 4 records", output)
        self.assertIn("3 records with valid Maschinen_Park_var values (75.0%)", output)
    
    @patch('pandas.read_csv')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_file_not_found(self, mock_args, mock_read_csv):
        """Test main function when file is not found"""
        # Setup
        mock_args.return_value = Mock(input_file='nonexistent.csv')
        mock_read_csv.side_effect = FileNotFoundError
        
        # Execute
        with patch('sys.stdout', new=io.StringIO()) as fake_out:
            main()
        
        # Assert
        output = fake_out.getvalue()
        self.assertIn("Error: Input file 'nonexistent.csv' not found", output)
    
    @patch('pandas.read_csv')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_missing_required_columns(self, mock_args, mock_read_csv):
        """Test main function when required columns are missing"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv')
        
        # Create a mock DataFrame without the required column
        data = {'Some_Other_Column': ['value1', 'value2']}
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df
        
        # Execute
        with patch('sys.stdout', new=io.StringIO()) as fake_out:
            main()
        
        # Assert
        output = fake_out.getvalue()
        self.assertIn("Error: Missing required columns: Maschinen_Park_Size", output)
    
    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_error_saving_file(self, mock_args, mock_to_csv, mock_read_csv):
        """Test main function when there's an error saving the output file"""
        # Setup
        mock_args.return_value = Mock(input_file='test.csv')
        
        # Create a mock DataFrame
        data = {'Maschinen_Park_Size': ['15-20', '30']}
        df = pd.DataFrame(data)
        mock_read_csv.return_value = df
        
        # Mock to_csv to raise an exception
        mock_to_csv.side_effect = Exception("Permission denied")
        
        # Execute
        with patch('sys.stdout', new=io.StringIO()) as fake_out:
            main()
        
        # Assert
        output = fake_out.getvalue()
        self.assertIn("Error saving output file: Permission denied", output)
        
if __name__ == '__main__':
    unittest.main()