import unittest
import json
from unittest.mock import patch, mock_open, MagicMock
from webcrawl.convert_to_csv import convert_json_to_csv

class TestConvertToCSV(unittest.TestCase):
    
    def setUp(self):
        # Sample valid JSON data for testing
        self.valid_json_data = [
            {
                "company_name": "Test Company 1",
                "company_url": "https://example1.com",
                "lohnfertigung": True,
                "products": ["Product1", "Product2", "Product3", "Product4"],
                "machines": ["Machine1", "Machine2"],
                "process_type": ["Process1", "Process2", "Process3"]
            },
            {
                "company_name": "Test Company 2",
                "company_url": "https://example2.com",
                "lohnfertigung": False,
                "products": ["Product5"],
                "machines": [],
                "process_type": ["Process4"]
            }
        ]
        
        # Expected CSV headers
        self.expected_headers = [
            'Company name', 'Company Url', 'Lohnfertigung(True/False)',
            'Produkte_1', 'Produkte_2', 'Produkte_3',
            'Maschinen_1', 'Maschinen_2', 'Maschinen_3',
            'Prozess_1', 'Prozess_2', 'Prozess_3'
        ]

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_successful_conversion_default_output(self, mock_json_load, mock_file_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_json_load.return_value = self.valid_json_data
        
        # Call function
        result = convert_json_to_csv('input.json')
        
        # Assertions
        self.assertEqual(result, 'input.csv')
        mock_isfile.assert_called_once_with('input.json')
        mock_file_open.assert_any_call('input.json', 'r', encoding='utf-8')
        mock_file_open.assert_any_call('input.csv', 'w', newline='', encoding='utf-8-sig')

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_successful_conversion_custom_output(self, mock_json_load, mock_file_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_json_load.return_value = self.valid_json_data
        
        # Call function
        result = convert_json_to_csv('input.json', 'custom_output.csv')
        
        # Assertions
        self.assertEqual(result, 'custom_output.csv')
        mock_isfile.assert_called_once_with('input.json')
        mock_file_open.assert_any_call('input.json', 'r', encoding='utf-8')
        mock_file_open.assert_any_call('custom_output.csv', 'w', newline='', encoding='utf-8-sig')

    @patch('os.path.isfile')
    def test_input_file_not_exists(self, mock_isfile):
        # Setup mock
        mock_isfile.return_value = False
        
        # Call function
        result = convert_json_to_csv('nonexistent.json')
        
        # Assertions
        self.assertIsNone(result)
        mock_isfile.assert_called_once_with('nonexistent.json')

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_json_decode_error(self, mock_json_load, mock_file_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_json_load.side_effect = json.JSONDecodeError('Invalid JSON', '', 0)
        
        # Call function
        result = convert_json_to_csv('invalid.json')
        
        # Assertions
        self.assertIsNone(result)
        mock_isfile.assert_called_once_with('invalid.json')

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_empty_json_array(self, mock_json_load, mock_file_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_json_load.return_value = []
        
        # Call function
        result = convert_json_to_csv('empty.json')
        
        # Assertions
        self.assertEqual(result, 'empty.csv')
        mock_isfile.assert_called_once_with('empty.json')

    @patch('os.path.isfile')
    @patch('builtins.open')
    def test_file_open_error(self, mock_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_open.side_effect = FileNotFoundError()
        
        # Call function
        result = convert_json_to_csv('error.json')
        
        # Assertions
        self.assertIsNone(result)
        mock_isfile.assert_called_once_with('error.json')

    @patch('os.path.isfile')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_missing_data_in_json(self, mock_json_load, mock_file_open, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        # JSON with missing fields
        mock_json_load.return_value = [{"company_name": "Minimal Company"}]
        
        # Call function
        result = convert_json_to_csv('minimal.json')
        
        # Assertions
        self.assertEqual(result, 'minimal.csv')
        mock_isfile.assert_called_once_with('minimal.json')

    @patch('os.path.isfile')
    @patch('csv.writer')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_csv_content_format(self, mock_json_load, mock_file_open, mock_csv_writer, mock_isfile):
        # Setup mocks
        mock_isfile.return_value = True
        mock_json_load.return_value = self.valid_json_data
        writer = MagicMock()
        mock_csv_writer.return_value = writer
        
        # Call function
        result = convert_json_to_csv('test.json', 'test.csv')
        
        # Assertions
        self.assertEqual(result, 'test.csv')
        # Verify headers were written
        writer.writerow.assert_any_call(self.expected_headers)
        
        # Verify first row data
        expected_row1 = [
            'Test Company 1', 'https://example1.com', 'True',
            'Product1', 'Product2', 'Product3',
            'Machine1', 'Machine2', '',
            'Process1', 'Process2', 'Process3'
        ]
        writer.writerow.assert_any_call(expected_row1)
        
        # Verify second row data
        expected_row2 = [
            'Test Company 2', 'https://example2.com', 'False',
            'Product5', '', '',
            '', '', '',
            'Process4', '', ''
        ]
        writer.writerow.assert_any_call(expected_row2)

if __name__ == '__main__':
    unittest.main()