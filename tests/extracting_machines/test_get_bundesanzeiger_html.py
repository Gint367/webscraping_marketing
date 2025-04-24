import datetime
import os
import shutil
import tempfile
import unittest

# Import functions to test
from extracting_machines.get_bundesanzeiger_html import (
    company_folder_exists,
    extract_financial_data_from_html,
    find_latest_jahresabschluss_locally,
    parse_date_str,
    sanitize_filename,
    store_files_locally,
)


class TestBundesanzeigerFunctions(unittest.TestCase):
    """Test case class for testing functions in get_bundesanzeiger_html.py"""

    def setUp(self):
        """Set up test environment before each test"""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up after each test"""
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)

    def test_sanitize_filename(self):
        """Test the sanitize_filename function"""
        # Test with German Umlauts
        self.assertEqual(sanitize_filename("Müller GmbH"), "Mueller_GmbH")
        self.assertEqual(sanitize_filename("Größe & Co."), "Groesse_and_Co.")
        self.assertEqual(sanitize_filename("Straße/Weg"), "Strasse_Weg")
        self.assertEqual(sanitize_filename("Äpfel Öl Über"), "Aepfel_Oel_Ueber")

    def test_parse_date_str(self):
        """Test the parse_date_str function"""
        # Test with valid date string
        expected_date = datetime.datetime(2023, 3, 30, 0, 0, 0)
        self.assertEqual(parse_date_str("2023-03-30 00:00:00"), expected_date)

        # Test with invalid date string - should return default date
        default_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
        self.assertEqual(parse_date_str("invalid-date"), default_date)

    def test_company_folder_exists(self):
        """Test the company_folder_exists function"""
        # Create a company folder with content
        company = "Test Company"
        safe_company = sanitize_filename(company)
        company_path = os.path.join(self.temp_dir, safe_company)
        os.makedirs(company_path, exist_ok=True)

        # Empty folder should return False
        self.assertFalse(company_folder_exists(self.temp_dir, company))

        # Create a subfolder to simulate a report folder
        report_path = os.path.join(company_path, "Jahresabschluss_2022")
        os.makedirs(report_path, exist_ok=True)

        # Now it should return True
        self.assertTrue(company_folder_exists(self.temp_dir, company))

    def test_extract_financial_data_from_html_empty(self):
        """Test extraction function with empty HTML"""
        result = extract_financial_data_from_html("")
        self.assertEqual(result["Technische Anlagen Start"], "NA")
        self.assertEqual(result["Sachanlagen Start"], "NA")
        self.assertEqual(result["Start Date"], "-")

    def test_extract_financial_data_from_html_with_table(self):
        """Test extraction of financial data from HTML table"""
        html = """
        <html>
            <body>
                <table>
                    <tr>
                        <td>Sachanlagen</td>
                        <td>100.000</td>
                        <td>120.000</td>
                    </tr>
                    <tr>
                        <td>Technische Anlagen</td>
                        <td>50.000</td>
                        <td>60.000</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        result = extract_financial_data_from_html(html)
        self.assertEqual(result["Sachanlagen Start"], "100.000")
        self.assertEqual(result["Sachanlagen End"], "120.000")
        self.assertEqual(result["Technische Anlagen Start"], "50.000")
        self.assertEqual(result["Technische Anlagen End"], "60.000")

    def test_extract_financial_data_from_html_with_sum_row(self):
        """Test extraction of financial data with sum row pattern"""
        html = """
        <html>
            <body>
                <table>
                    <tr>
                        <td>II. Sachanlagen</td>
                        <td></td>
                        <td></td>
                    </tr>
                    <tr>
                        <td></td>
                        <td>250.000</td>
                        <td>280.000</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        result = extract_financial_data_from_html(html)
        self.assertEqual(result["Sachanlagen Start"], "250.000")
        self.assertEqual(result["Sachanlagen End"], "280.000")

    def test_extract_financial_data_from_html_with_inline_fallback(self):
        """Test extraction using inline regex fallback"""
        html = """
        <html>
            <body>
                <p>
                    II. Sachanlagen
                    Die Sachanlagen betragen zum Stichtag 300.000 400.000 Euro.
                </p>
                <p>
                    Die Technische Anlagen wurden mit
                    150.000 180.000 bewertet.
                </p>
            </body>
        </html>
        """
        result = extract_financial_data_from_html(html)
        self.assertEqual(result["Sachanlagen Start"], "300.000")
        self.assertEqual(result["Sachanlagen End"], "400.000")
        self.assertEqual(result["Technische Anlagen Start"], "150.000")
        self.assertEqual(result["Technische Anlagen End"], "180.000")

    def test_find_latest_jahresabschluss_locally(self):
        """Test finding the latest Jahresabschluss HTML file"""
        company = "Test Company"
        safe_company = sanitize_filename(company)
        company_path = os.path.join(self.temp_dir, safe_company)

        # Create multiple report folders
        report1_path = os.path.join(company_path, "Jahresabschluss_2021")
        report2_path = os.path.join(company_path, "Jahresabschluss_2022")
        report3_path = os.path.join(company_path, "Other_Report")
        os.makedirs(report1_path, exist_ok=True)
        os.makedirs(report2_path, exist_ok=True)
        os.makedirs(report3_path, exist_ok=True)

        # Create HTML files
        html_content = "<html><body>Test Report</body></html>"
        with open(os.path.join(report1_path, "Jahresabschluss_2021_raw_report.html"), "w") as f:
            f.write(html_content)
        with open(os.path.join(report2_path, "Jahresabschluss_2022_raw_report.html"), "w") as f:
            f.write(html_content)

        # Test finding the latest report
        content, path = find_latest_jahresabschluss_locally(self.temp_dir, company)
        self.assertEqual(content, html_content)
        self.assertEqual(path, report2_path)  # Should find 2022 as latest

    def test_store_files_locally(self):
        """Test storing files locally"""
        company = "Test AG"
        report_name = "Jahresabschluss 2022"
        html_content = "<html><body>Test Report</body></html>"
        txt_content = "Test Report Text"
        date_str = "2022-12-31"

        folder_path = store_files_locally(
            self.temp_dir, company, report_name, html_content, txt_content, date_str
        )

        # Check that folder was created
        self.assertTrue(os.path.exists(folder_path))

        # Check that files were created
        self.assertTrue(os.path.exists(os.path.join(
            folder_path, "Jahresabschluss_2022_raw_report.html")))
        self.assertTrue(os.path.exists(os.path.join(
            folder_path, "Jahresabschluss_2022_report.txt")))
        self.assertTrue(os.path.exists(os.path.join(
            folder_path, "Jahresabschluss_2022_metadata.json")))


if __name__ == "__main__":
    unittest.main()
