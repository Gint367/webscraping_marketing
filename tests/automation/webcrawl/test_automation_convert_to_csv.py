"""
Unit tests for convert_to_csv.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import sys
import unittest
from unittest.mock import patch


class TestConvertToCsv(unittest.TestCase):
    """Tests for convert_to_csv.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_file = 'tests/automation/webcrawl/data/valid_enhanced_json.json'
        self.invalid_input_file = 'tests/automation/webcrawl/data/invalid_enhanced_json.json'
        self.empty_input_file = 'tests/automation/webcrawl/data/empty_enhanced_json.json'
        self.missing_file = 'tests/automation/webcrawl/data/missing_enhanced_json.json'
        self.output_file = 'tests/automation/webcrawl/output/converted_output.csv'
        # Create sample valid JSON file
        with open(self.valid_input_file, 'w') as f:
            f.write("""
                [
                    {
                        "company_name": "Alpha Metall GmbH",
                        "company_url": "https://www.alphametall.com",
                        "products": [
                            "Standardprofile",
                            "Aluminium-Stangen",
                            "Aluminium-Rohre"
                        ],
                        "machines": [
                            "Extrusionsmaschinen",
                            "Bearbeitungsmaschinen",
                            "Veredelungsmaschinen"
                        ],
                        "process_type": [
                            "Extrusionen",
                            "Bearbeitungen",
                            "Veredelungen"
                        ],
                        "lohnfertigung": true,
                        "error": false
                    }
                ]
                """)
        # Create sample invalid JSON file (malformed)
        with open(self.invalid_input_file, 'w') as f:
            f.write('not a json')
        # Create empty JSON file
        with open(self.empty_input_file, 'w') as f:
            f.write('')

    def tearDown(self) -> None:
        for f in [self.valid_input_file, self.invalid_input_file, self.empty_input_file]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_main_validInput_createsCsv(self):
        """main_validInput_createsCsv_expectedCsvCreated: Should create CSV output for valid input"""
        import webcrawl.convert_to_csv as convert_to_csv
        test_args = [
            "convert_to_csv.py",
            self.valid_input_file,
            "-o",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            convert_to_csv.main()
        self.assertTrue(os.path.exists(self.output_file), "CSV output was not created for valid input")

    def test_main_invalidInput_malformedJson_raisesError(self):
        """main_invalidInput_malformedJson_raisesError_expectedException: Should raise error for malformed JSON input"""
        import webcrawl.convert_to_csv as convert_to_csv
        test_args = [
            "convert_to_csv.py",
            self.invalid_input_file,
            "-o",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(Exception):
                convert_to_csv.main()

    def test_main_emptyInput_createsEmptyCsv(self):
        """main_emptyInput_createsEmptyCsv_expectedEmptyOutput: Should create empty CSV output for empty input file"""
        import webcrawl.convert_to_csv as convert_to_csv
        test_args = [
            "convert_to_csv.py",
            self.empty_input_file,
            "-o",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            convert_to_csv.main()
        self.assertTrue(os.path.exists(self.output_file), "Output file should be created for empty input file")
        with open(self.output_file, 'r', encoding='utf-8-sig') as f:
            data = f.read().strip()
            # Remove BOM if present and check for header
            if data.startswith('\ufeff'):
                data = data.lstrip('\ufeff')
            self.assertTrue(data == '' or data.startswith('Company name'), "Output file should be empty or contain only headers for empty input file")

    def test_main_missingInputFile_raisesFileNotFoundError(self):
        """main_missingInputFile_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input file"""
        import webcrawl.convert_to_csv as convert_to_csv
        test_args = [
            "convert_to_csv.py",
            self.missing_file,
            "-o",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                convert_to_csv.main()

if __name__ == '__main__':
    unittest.main()
