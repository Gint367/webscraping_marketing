import unittest
from unittest.mock import patch

import pandas as pd

# Absolute import of functions to test
from merge_pipeline.merge_technische_anlagen_with_keywords import (
    clean_trailing_symbols,
    extract_and_log_domains,
    extract_base_domain,
    merge_csv_with_excel,
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
        self.assertEqual(
            extract_base_domain("http://subdomain.example.com/page"), "example.com"
        )

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
        self.assertEqual(
            extract_base_domain("https://www.example.com/products/item?id=123"),
            "example.com",
        )

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
        self.test_df = pd.DataFrame(
            {
                "URL": [
                    "https://www.example.com",
                    "example.org",
                    "subdomain.test.net/page",
                    None,
                    "https://www.sample.co.uk/products",
                ],
                "other_column": [1, 2, 3, 4, 5],
            }
        )

    def test_extract_and_log_domains_existing_column(self):
        """Test extracting domains when URL column exists"""
        # No mock_print parameter anymore
        result = extract_and_log_domains(
            self.test_df,
            "URL",
            column_to_create="base_domain",
            sample_size=3,
            description="test",
        )

        # Check that domains were extracted correctly
        expected_domains = [
            "example.com",
            "example.org",
            "test.net",
            None,
            "sample.co.uk",
        ]
        pd.testing.assert_series_equal(
            result["base_domain"], pd.Series(expected_domains, name="base_domain")
        )

        # Check if the column has been created with correct values
        self.assertIn("base_domain", result.columns)
        self.assertEqual(result["base_domain"].iloc[0], "example.com")
        self.assertEqual(result["base_domain"].iloc[1], "example.org")

    def test_extract_and_log_domains_missing_column(self):
        """Test behavior when URL column doesn't exist"""
        result = extract_and_log_domains(
            self.test_df,
            "nonexistent_column",
            column_to_create="base_domain",
            description="test",
        )

        # The function should add a column with None values
        self.assertIn("base_domain", result.columns)
        self.assertTrue(result["base_domain"].isna().all())

        # New assertion: Make sure the original dataframe structure remains intact
        self.assertEqual(
            len(result.columns), 3
        )  # Original 2 columns plus the new base_domain column


class TestMergeCsvWithExcel(unittest.TestCase):
    def setUp(self):
        """Set up test data for merge_csv_with_excel tests"""
        # Sample CSV data
        self.csv_data = pd.DataFrame(
            {
                "Company name": ["Company A", "Company B", "Company C", "Company D"],
                "Website": [
                    "www.companya.com",
                    "www.companyb.com",
                    "www.companyc.com",
                    "www.companyd.com",
                ],
                "Other Data": ["Data1", "Data2", "Data3", "Data4"],
            }
        )

        # Sample base data
        self.base_data = pd.DataFrame(
            {
                "Firma1": ["Company A", "Company B", "Company E", "Company F"],
                "Ort": ["City A", "City B", "City E", "City F"],
                "Top1_Machine": ["Machine A", "Machine B", "Machine E", "Machine F"],
                "URL": [
                    "www.companya.com",
                    "www.companyb.com",
                    "www.companye.com",
                    "www.companyf.com",
                ],
                "Maschinen_Park_Size": [10, 20, 30, 40],
                "Sachanlagen": [1000, 2000, 3000, 4000],
                "OtherCol": ["X", "Y", "Z", "W"],
            }
        )

        # Mock file paths
        self.csv_path = "consolidated_output/pluralized_federn.csv"
        self.base_data_path = "merged_federn_20250317.csv"
        self.output_path = "final_export_federn"

        # Expected merged data for name matches
        self.expected_name_matches = pd.DataFrame(
            {
                "Company name": ["Company A", "Company B"],
                "Website": ["www.companya.com", "www.companyb.com"],
                "Other Data": ["Data1", "Data2"],
                "technische Anlagen und Maschinen 2021/22": ["Machine A", "Machine B"],
                "Ort": ["City A", "City B"],
                "Maschinen_Park_Size": [10, 20],
            }
        )

    @patch("pandas.read_csv")
    @patch("pandas.read_excel")
    @patch("pandas.DataFrame.to_csv")
    @patch("os.path.splitext")
    @patch("builtins.print")
    @patch("os.path.exists")
    def test_merge_csv_with_excel_xlsx_input(
        self, mock_exists, mock_print, mock_splitext, mock_to_csv, mock_read_excel, mock_read_csv
    ):
        """Test merge_csv_with_excel with Excel input"""
        # Configure mocks
        mock_exists.return_value = True  # Make os.path.exists() return True
        mock_read_csv.return_value = self.csv_data
        mock_read_excel.return_value = self.base_data
        mock_splitext.return_value = ("base_path", ".xlsx")

        # Call the function with required parameters
        merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)

        # Verify the CSV was read
        mock_read_csv.assert_called_once_with(self.csv_path, encoding="utf-8")

        # Verify Excel was read
        mock_read_excel.assert_called_once_with(self.base_data_path, sheet_name=0)

        # Verify the output was written
        mock_to_csv.assert_called_once()

        # We can't easily check the exact DataFrame passed to to_csv due to the complexity,
        # but we can verify some parameters
        args, kwargs = mock_to_csv.call_args
        self.assertEqual(kwargs["encoding"], "utf-8-sig")
        self.assertEqual(kwargs["index"], False)
        self.assertEqual(kwargs["sep"], ",")

    @patch("pandas.read_csv")
    @patch("pandas.read_excel")
    @patch("pandas.DataFrame.to_csv")
    @patch("os.path.splitext")
    @patch("builtins.print")
    @patch("os.path.exists")
    def test_merge_csv_with_excel_csv_input(
        self, mock_exists, mock_print, mock_splitext, mock_to_csv, mock_read_excel, mock_read_csv
    ):
        """Test merge_csv_with_excel with CSV input"""
        # Configure exists mock
        mock_exists.return_value = True  # Make os.path.exists() return True
        # First call returns CSV data, second call returns base data
        mock_read_csv.side_effect = [self.csv_data, self.base_data]
        mock_splitext.return_value = ("base_path", ".csv")

        # Call the function with required parameters
        merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)

        # Verify the CSVs were read
        mock_read_csv.assert_any_call(self.csv_path, encoding="utf-8")
        mock_read_csv.assert_any_call(self.base_data_path, encoding="utf-8")

        # Verify Excel was NOT read
        mock_read_excel.assert_not_called()

        # Verify the output was written
        mock_to_csv.assert_called_once()

    @patch("pandas.read_csv")
    @patch("os.path.splitext")
    @patch("builtins.print")
    @patch("os.path.exists")
    def test_merge_csv_with_excel_unsupported_format(
        self, mock_exists, mock_print, mock_splitext, mock_read_csv
    ):
        """Test merge_csv_with_excel with unsupported file format"""
        mock_exists.return_value = True  # Make os.path.exists() return True
        mock_read_csv.return_value = self.csv_data
        mock_splitext.return_value = ("base_path", ".txt")

        # The function should raise a ValueError for unsupported formats
        with self.assertRaises(ValueError) as context:
            merge_csv_with_excel(self.csv_path, self.base_data_path, self.output_path)

        self.assertIn("Unsupported file format: .txt", str(context.exception))

    @patch("pandas.read_csv")
    @patch("pandas.DataFrame.to_csv")
    @patch("os.path.splitext")
    @patch("merge_pipeline.merge_technische_anlagen_with_keywords.logger.info")
    @patch("merge_pipeline.merge_technische_anlagen_with_keywords.logger.warning")
    @patch("os.path.exists")
    def test_correct_match_tracking_with_missing_values(
        self, mock_exists, mock_log_warning, mock_log_info, mock_splitext, mock_to_csv, mock_read_csv
    ):
        """Test that companies are correctly tracked as matched even with missing technical data"""
        # Mock os.path.exists to return True
        mock_exists.return_value = True

        # Create test data where all companies should match by name
        csv_data = pd.DataFrame(
            {
                "Company name": ["Company A", "Company B", "Company C"],
                "Website": ["www.companya.com", "www.companyb.com", "www.companyc.com"],
                "Other Data": ["Data1", "Data2", "Data3"],
            }
        )

        base_data = pd.DataFrame(
            {
                "Firma1": ["Company A", "Company B", "Company C"],
                "Ort": ["City A", "City B", "City C"],
                "Top1_Machine": ["Machine A", None, "Machine C"],  # Missing value for B
                "URL": ["www.companya.com", "www.companyb.com", "www.companyc.com"],
                "Maschinen_Park_Size": [10, 20, 30],
                "Sachanlagen": [1000, None, 3000],  # Missing value for B
                "OtherCol": ["X", "Y", "Z"],
            }
        )

        # Configure mocks
        mock_read_csv.side_effect = [csv_data, base_data]
        mock_splitext.return_value = ("base_path", ".csv")

        # Call the function
        merge_csv_with_excel("test.csv", "test_base.csv", "output.csv")

        # Check for warning about missing technical data for Company B
        # Use a more flexible approach to match the warning message
        warning_found = False
        for call_args in mock_log_warning.call_args_list:
            args, _ = call_args
            if (args and isinstance(args[0], str) and
                "Technical data empty for matched company: 'Company B'" in args[0]):
                warning_found = True
                break
        
        self.assertTrue(warning_found,
            "Should have logged a warning about empty technical data for Company B")

        # Verify we reported the correct number of matched companies (3)
        # Look for the exact format of the message that the function uses
        match_found = False
        for call_args in mock_log_info.call_args_list:
            args, _ = call_args
            if args and isinstance(args[0], str) and "Matched 3 companies by name" in args[0]:
                match_found = True
                break
                
        self.assertTrue(match_found, "Should have logged that all 3 companies were matched by name")

        # Verify the function continued processing despite missing values
        # Check if the data was saved
        mock_to_csv.assert_called_once()


if __name__ == "__main__":
    unittest.main()
