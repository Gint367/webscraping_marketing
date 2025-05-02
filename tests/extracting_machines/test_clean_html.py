import json
import unittest
from unittest.mock import mock_open, patch

from bs4 import BeautifulSoup

# Import the functions to test using absolute import
from extracting_machines.clean_html import (
    clean_html,
    filter_word_rows,
    get_latest_subfolder,
)


class TestCleanHTML(unittest.TestCase):

    def test_empty_html(self):
        """Test clean_html with empty HTML."""
        result = clean_html("")
        self.assertIsNone(result)

    def test_no_tables(self):
        """Test clean_html with HTML that contains no tables."""
        html = "<div><p>No tables here</p></div>"
        result = clean_html(html)
        self.assertIsNone(result)

    def test_with_original_filename(self):
        """Test clean_html with original_filename parameter."""
        html = """
        <html>
            <body>
                <h2>Header</h2>
                <table>
                    <tr><td>Data</td></tr>
                </table>
            </body>
        </html>
        """
        filename = "test company"
        result = clean_html(html, original_filename=filename)

        # Check that the filename comment is included as a proper HTML comment
        expected_comment = f"<!--original_filename: {filename}-->"
        self.assertIn(expected_comment, result)

        # Check that the table and other elements are still present
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 1)
        headers = soup.find_all("h2")
        self.assertEqual(len(headers), 1)

    def test_without_original_filename(self):
        """Test clean_html without original_filename parameter."""
        html = """
        <html>
            <body>
                <h2>Header</h2>
                <table>
                    <tr><td>Data</td></tr>
                </table>
            </body>
        </html>
        """
        result = clean_html(html)

        # Check that no filename comment is included
        self.assertNotIn("<!-- original_filename:", result)

        # Check that the table and other elements are still present
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 1)

    def test_with_tables_no_filter(self):
        """Test clean_html with HTML containing tables and no filter word."""
        html = """
        <html>
            <body>
                <h2>Header 1</h2>
                <table>
                    <tr><td>Data 1</td></tr>
                </table>
                <h2>Header 2</h2>
                <table>
                    <tr><td>Data 2</td></tr>
                </table>
            </body>
        </html>
        """
        result = clean_html(html)

        # Both tables should be included
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 2)

        # Headers should be included
        headers = soup.find_all("h2")
        self.assertEqual(len(headers), 2)

    def test_with_filter_word(self):
        """Test clean_html with HTML containing tables and a filter word."""
        html = """
        <html>
            <body>
                <h2>Header 1</h2>
                <table>
                    <tr><td>Special data</td></tr>
                </table>
                <h2>Header 2</h2>
                <table>
                    <tr><td>Regular data</td></tr>
                </table>
            </body>
        </html>
        """
        result = clean_html(html, filter_word="special")

        # Only the table with "special" should be included
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 1)
        self.assertIn("Special data", tables[0].text)

    def test_skip_begin_pub_tables(self):
        """Test that tables with id='begin_pub' are skipped."""
        html = """
        <html>
            <body>
                <h2>Header 1</h2>
                <table id="begin_pub">
                    <tr><td>Should be skipped</td></tr>
                </table>
                <h2>Header 2</h2>
                <table>
                    <tr><td>Should be included</td></tr>
                </table>
            </body>
        </html>
        """
        result = clean_html(html)

        # Only the table without id="begin_pub" should be included
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 1)
        self.assertIn("Should be included", tables[0].text)

    def test_preceding_elements(self):
        """Test that preceding elements are correctly collected."""
        html = """
        <html>
            <body>
                <h1>Far header</h1>
                <p>Far paragraph</p>
                <div>
                    <h2>Near header</h2>
                    <p>First paragraph</p>
                    <p>Second paragraph</p>
                    <table>
                        <tr><td>Table data</td></tr>
                    </table>
                </div>
            </body>
        </html>
        """
        result = clean_html(html)

        # The cleaned HTML should contain the table and up to MAX_PRECEDING_ELEMENTS preceding elements
        soup = BeautifulSoup(result, "html.parser")

        # Check that we have the expected elements
        self.assertEqual(len(soup.find_all("table")), 1)
        self.assertEqual(len(soup.find_all("h2")), 1)
        self.assertEqual(len(soup.find_all("p")), 2)

        # h1 should not be included as it's beyond the MAX_PRECEDING_ELEMENTS limit
        self.assertEqual(len(soup.find_all("h1")), 0)

    def test_stop_at_previous_table(self):
        """Test that preceding element collection stops at a previous table."""
        html = """
        <html>
            <body>
                <h1>Header 1</h1>
                <table>
                    <tr><td>Previous table</td></tr>
                </table>
                <h2>Header 2</h2>
                <p>Paragraph</p>
                <table>
                    <tr><td>Target table</td></tr>
                </table>
            </body>
        </html>
        """
        result = clean_html(html)

        # The cleaned HTML should contain both tables, but the h1 should not be included with the second table
        soup = BeautifulSoup(result, "html.parser")
        tables = soup.find_all("table")
        self.assertEqual(len(tables), 2)

        # Extract all elements in the cleaned HTML
        all_elements = [el for el in soup.find_all(['h1', 'h2', 'p', 'table'])]

        # Find the position of the second table
        second_table_index = all_elements.index(tables[1])

        # Check the elements before the second table but after the first table
        preceding_elements = all_elements[all_elements.index(tables[0]) + 1:second_table_index]

        # Only h2 and p should be between the tables
        self.assertEqual(len(preceding_elements), 2)
        self.assertEqual(preceding_elements[0].name, 'h2')
        self.assertEqual(preceding_elements[1].name, 'p')
        self.assertEqual(preceding_elements[0].text, 'Header 2')
        self.assertEqual(preceding_elements[1].text, 'Paragraph')

        # Verify h1 is not between the tables
        for element in preceding_elements:
            self.assertNotEqual(element.name, 'h1')


class TestFilterWordRows(unittest.TestCase):

    def test_empty_html(self):
        """Test filter_word_rows with empty HTML."""
        result = filter_word_rows("", "search")
        self.assertEqual(result, [])

    def test_no_tables(self):
        """Test filter_word_rows with HTML that contains no tables."""
        html = "<div><p>No tables here</p></div>"
        result = filter_word_rows(html, "search")
        self.assertEqual(result, [])

    def test_no_matching_rows(self):
        """Test filter_word_rows with tables but no matching rows."""
        html = """
        <html>
            <body>
                <h2>Table Header</h2>
                <table>
                    <tr><th>Column 1</th><th>Column 2</th></tr>
                    <tr><td>Data 1</td><td>Data 2</td></tr>
                </table>
            </body>
        </html>
        """
        result = filter_word_rows(html, "special")
        self.assertEqual(result, [])

    def test_matching_rows(self):
        """Test filter_word_rows with tables containing matching rows."""
        html = """
        <html>
            <body>
                <h2>Table Header</h2>
                <table>
                    <tr><th>Column 1</th><th>Column 2</th></tr>
                    <tr><td>special data</td><td>value</td></tr>
                    <tr><td>other data</td><td>value</td></tr>
                </table>
            </body>
        </html>
        """
        result = filter_word_rows(html, "special")

        # Should find one matching table with one matching row
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "Table Header")
        self.assertEqual(len(result[0]["matching_rows"]), 1)

        # Check if values dictionary contains a key that matches "Column 1" (case-insensitive, ignoring whitespace)
        values = result[0]["matching_rows"][0]["values"]

        # Find the key that matches "Column 1" regardless of whitespace
        matching_key = None
        for key in values:
            if key.strip().lower() == "column1".lower():
                matching_key = key
                break

        # Assert that we found a matching key
        self.assertIsNotNone(matching_key, f"No key matching 'Column1' found in {list(values.keys())}")

        # Check the value using the matching key
        self.assertEqual(values[matching_key].strip(), "special data")

    def test_word_length_validation(self):
        """Test that words are validated based on length criteria."""
        html = """
        <html>
            <body>
                <h2>Table Header</h2>
                <table>
                    <tr><th>Column 1</th><th>Column 2</th></tr>
                    <tr><td>longword special</td><td>value</td></tr>
                    <tr><td>word special</td><td>value</td></tr>
                </table>
            </body>
        </html>
        """
        result = filter_word_rows(html, "special")

        # Only the second row should match as "longword" exceeds MIN_WORD_LENGTH
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]["matching_rows"]), 1)
        self.assertEqual(result[0]["matching_rows"][0]["values"]["Column1"], "word special")

    def test_result_structure(self):
        """Test the structure of the returned results."""
        html = """
        <html>
            <body>
                <h2>Table Header</h2>
                <table>
                    <thead>
                        <tr><th>Column 1</th><th>Column 2</th></tr>
                    </thead>
                    <tbody>
                        <tr><td>special data</td><td>123</td></tr>
                    </tbody>
                </table>
            </body>
        </html>
        """
        result = filter_word_rows(html, "special")

        # Check structure of the result
        self.assertEqual(len(result), 1)
        table_result = result[0]

        self.assertIn("table_name", table_result)
        self.assertIn("header_levels", table_result)
        self.assertIn("matching_rows", table_result)

        row = table_result["matching_rows"][0]
        self.assertIn("header1", row)
        self.assertIn("values", row)

        values = row["values"]

        # Find keys for Column 1 and Column 2
        col1_key = None
        col2_key = None
        for key in values:
            if key.replace(" ", "").lower() == "column1".lower():
                col1_key = key
            elif key.replace(" ", "").lower() == "column2".lower():
                col2_key = key

        self.assertIsNotNone(col1_key, f"No key matching 'Column1' found in {list(values.keys())}")
        self.assertIsNotNone(col2_key, f"No key matching 'Column2' found in {list(values.keys())}")

        self.assertEqual(values[col1_key], "special data")
        self.assertEqual(values[col2_key], "123")

    def test_complex_table_structure(self):
        """Test handling of complex table structures with nested headers."""
        html = """
        <html>
            <body>
                <h2>Complex Table</h2>
                <table>
                    <thead>
                        <tr>
                            <th rowspan="2">Item</th>
                            <th colspan="2">Details</th>
                        </tr>
                        <tr>
                            <th>Type</th>
                            <th>Value</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>special item</td>
                            <td>Type A</td>
                            <td>100</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """
        result = filter_word_rows(html, "special")

        # Check proper handling of the complex header structure
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["header_levels"], 2)  # Two levels of headers

        row = result[0]["matching_rows"][0]
        self.assertEqual(len(row["header1"]), 3)  # First header row has 3 columns (with colspan)
        self.assertEqual(len(row["header2"]), 2)  # Second header row has only Type and Value

        # Check header values are correctly captured
        self.assertEqual(row["header1"][0], "Item")
        self.assertEqual(row["header1"][1], "Details")
        self.assertEqual(row["header1"][2], "Details")  # Duplicated due to colspan

        # The second row doesn't include the Item cell because of rowspan
        self.assertEqual(row["header2"][0], "Type")
        self.assertEqual(row["header2"][1], "Value")


class TestGetLatestSubfolder(unittest.TestCase):

    @patch('os.listdir')
    @patch('os.path.isdir')
    @patch('pathlib.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_empty_directory(self, mock_file, mock_glob, mock_isdir, mock_listdir):
        """Test get_latest_subfolder with an empty directory."""
        mock_listdir.return_value = []

        result = get_latest_subfolder("dummy_path")
        self.assertIsNone(result)

    @patch('os.listdir')
    @patch('os.path.isdir')
    @patch('pathlib.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    def test_no_valid_subfolders(self, mock_file, mock_glob, mock_isdir, mock_listdir):
        """Test get_latest_subfolder with no valid subfolders."""
        mock_listdir.return_value = ["subfolder1", "subfolder2"]
        mock_isdir.return_value = True
        mock_glob.return_value = []  # No metadata files

        result = get_latest_subfolder("dummy_path")
        self.assertIsNone(result)

    @patch('os.listdir')
    @patch('os.path.isdir')
    @patch('pathlib.Path.glob')
    @patch('builtins.open')
    def test_invalid_metadata_file(self, mock_file, mock_glob, mock_isdir, mock_listdir):
        """Test get_latest_subfolder with invalid metadata files."""
        mock_listdir.return_value = ["subfolder1"]
        mock_isdir.return_value = True
        mock_glob.return_value = ["subfolder1/metadata.json"]

        # Mock open to raise an exception
        mock_file.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        # Should handle the exception gracefully and return None
        result = get_latest_subfolder("dummy_path")
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
