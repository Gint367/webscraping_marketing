import unittest
from unittest.mock import MagicMock, mock_open, patch

# filepath: /home/novoai/Documents/scraper/extracting_machines/test_generate_csv_report.py
# Import the functions to test using absolute import
from extracting_machines.generate_csv_report import (
    extract_company_name,
    extract_values,
    generate_csv_report,
)


class TestExtractCompanyName(unittest.TestCase):

    def test_empty_data(self):
        """Test extracting company name from empty data."""
        self.assertEqual(extract_company_name([]), "Unknown Company")
        self.assertEqual(extract_company_name(None), "Unknown Company")

    def test_valid_company_name(self):
        """Test extracting company name from valid data."""
        data = [{"company_name": "Test Company GmbH"}]
        self.assertEqual(extract_company_name(data), "Test Company GmbH")

    def test_missing_company_name(self):
        """Test extracting company name when field is missing."""
        data = [{"some_field": "Some value"}]
        self.assertEqual(extract_company_name(data), "Unknown Company")


class TestExtractValues(unittest.TestCase):

    def test_empty_data(self):
        """Test extracting values from empty data."""
        max_values = 3
        filter_words = ["ahk", "abschreibung"]
        values, table_name, max_val = extract_values([], max_values, filter_words)

        self.assertEqual(values, [''] * max_values)
        self.assertEqual(table_name, '')
        self.assertEqual(max_val, '')

    def test_valid_numeric_data(self):
        """Test extracting valid numeric values."""
        data = [{
            "table_name": "Machine Table",
            "header_levels": 1,
            "matching_rows": [
                {
                    "header1": ["Type", "Value"],
                    "values": {"col1": "1000", "col2": "2000"}
                }
            ]
        }]

        max_values = 2
        filter_words = ["ahk", "abschreibung"]
        values, table_name, max_val = extract_values(data, max_values, filter_words)

        self.assertEqual(values, ["1000", "2000"])
        self.assertEqual(table_name, "Machine Table")
        self.assertEqual(max_val, "2000")

    def test_filtered_headers(self):
        """Test that tables with filtered words in headers are skipped."""
        data = [{
            "table_name": "Filtered Table",
            "header_levels": 1,
            "matching_rows": [
                {
                    "header1": ["Type", "AHK Value"],
                    "values": {"col1": "1000", "col2": "2000"}
                }
            ]
        }]

        max_values = 2
        filter_words = ["ahk", "abschreibung"]
        values, table_name, max_val = extract_values(data, max_values, filter_words)

        self.assertEqual(values, ['', ''])
        self.assertEqual(table_name, '')
        self.assertEqual(max_val, '')

    def test_number_cleaning(self):
        """Test number cleaning functionality."""
        data = [{
            "table_name": "Numbers Table",
            "header_levels": 1,
            "matching_rows": [
                {
                    "header1": ["Type", "Value"],
                    "values": {
                        "col1": "1.000",  # With thousand separator
                        "col2": "2,500",  # With decimal part
                        "col3": "  3000  "  # With whitespace
                    }
                }
            ]
        }]

        max_values = 3
        filter_words = ["ahk"]
        values, table_name, max_val = extract_values(data, max_values, filter_words)

        self.assertEqual(values, ["1000", "2", "3000"])
        self.assertEqual(max_val, "3000")

    def test_values_exceeding_max(self):
        """Test when number of values exceeds max_values."""
        data = [{
            "table_name": "Big Table",
            "header_levels": 1,
            "matching_rows": [
                {
                    "header1": ["Type", "Value"],
                    "values": {"col1": "1000", "col2": "2000", "col3": "3000", "col4": "4000"}
                }
            ]
        }]

        max_values = 2
        filter_words = ["ahk"]
        values, table_name, max_val = extract_values(data, max_values, filter_words)

        # Values should be truncated to max_values
        self.assertEqual(len(values), max_values)

    def test_skip_non_numeric_values(self):
        """Test that non-numeric values are skipped."""
        data = [{
            "table_name": "Mixed Table",
            "header_levels": 1,
            "matching_rows": [
                {
                    "header1": ["Type", "Value"],
                    "values": {"col1": "1000", "col2": "not a number", "col3": "3000"}
                }
            ]
        }]

        max_values = 3
        filter_words = ["ahk"]
        values, table_name, max_val = extract_values(data, max_values, filter_words)

        self.assertEqual(values, ["1000", "3000", ""])
        self.assertEqual(max_val, "3000")


class TestGenerateCSVReport(unittest.TestCase):

    @patch('os.listdir')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    @patch('csv.writer')
    @patch('datetime.datetime')
    def test_generate_csv_report(self, mock_datetime, mock_csv_writer, mock_json_load, mock_file_open, mock_listdir):
        """Test the CSV report generation function."""
        # Setup mocks
        mock_listdir.return_value = ['file1_filtered.json', 'file2_filtered.json', 'hello_world.json']

        # Mock JSON data
        mock_json_data = [
            {"company_name": "Company A"}
        ]
        mock_json_load.return_value = mock_json_data

        # Mock CSV writer
        mock_writer = MagicMock()
        mock_csv_writer.return_value = mock_writer

        # Create a custom extract function for testing
        def test_extract_func(data, n):
            if data[0].get('company_name') == "Company A":
                return ["100", "200"], "Table A", "200"
            return [""], "", ""

        # Call the function
        generate_csv_report(
            input_dir="test_dir",
            output_file="test_output.csv",
            n=2,
            extract_func=test_extract_func
        )

        # Verify the function calls
        self.assertEqual(mock_listdir.call_count, 1)
        self.assertEqual(mock_json_load.call_count, 2)  # Two filtered files

        # Verify CSV headers
        mock_writer.writerow.assert_any_call(['Company', 'Table', 'Machine_1', 'Machine_2'])

        # Verify data rows were written
        mock_writer.writerow.assert_any_call(["Company A", "Table A", "100", "200"])


if __name__ == '__main__':
    unittest.main()
