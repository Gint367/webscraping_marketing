"""
Unit tests for generate_csv_report.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import glob
import os
import unittest

import extracting_machines.generate_csv_report as generate_csv_report


class TestGenerateCsvReport(unittest.TestCase):
    """Tests for generate_csv_report.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/extracting_machine/data/valid_cleaned_json'
        self.invalid_input_dir = 'tests/automation/extracting_machine/data/invalid_cleaned_json'
        self.empty_input_dir = 'tests/automation/extracting_machine/data/empty_cleaned_json'
        self.missing_dir = 'tests/automation/extracting_machine/data/missing_cleaned_json'
        self.output_file = 'tests/automation/extracting_machine/output/machine_report.csv'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        # Create sample valid JSON file
        with open(os.path.join(self.valid_input_dir, 'firma_a_filtered.json'), 'w') as f:
            f.write('[{"company_name": "Firma A", "table_name": "AKTIVA", "header_levels": 2, "matching_rows": [{"header1": ["Column1", "Column2"], "values": {"col1": "1000", "col2": "2000"}}]}]')
        # Create sample invalid JSON file (malformed)
        with open(os.path.join(self.invalid_input_dir, 'firma_b_filtered.json'), 'w') as f:
            f.write('not a json')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        # Remove all test artifact CSVs with timestamp in output folder
        output_dir = os.path.dirname(self.output_file)
        base_name = os.path.splitext(os.path.basename(self.output_file))[0]
        pattern = os.path.join(output_dir, f"{base_name}_*.csv")
        for artifact in glob.glob(pattern):
            os.remove(artifact)

    def test_main_validInput_createsCsvReport(self):
        """main_validInput_createsCsvReport_expectedCsvCreated: Should create CSV report from valid cleaned JSON files"""
        filter_words = ["anschaffungs", "ahk", "abschreibung", "buchwert"]
        N = 3
        generate_csv_report.generate_csv_report(
            self.valid_input_dir,
            self.output_file,
            N,
            lambda data, n: generate_csv_report.extract_values(data, n, filter_words)
        )
        # The function appends a timestamp to the output filename
        output_pattern = self.output_file.replace('.csv', '_*.csv')
        output_files = glob.glob(output_pattern)
        self.assertTrue(output_files, "CSV report was not created for valid input")

    def test_main_invalidInput_malformedJson_raisesError(self):
        """main_invalidInput_malformedJson_raisesError_expectedException: Should raise ValueError for malformed JSON input"""
        filter_words = ["anschaffungs", "ahk", "abschreibung", "buchwert"]
        N = 3
        with self.assertRaises(Exception):
            generate_csv_report.generate_csv_report(
                self.invalid_input_dir,
                self.output_file,
                N,
                lambda data, n: generate_csv_report.extract_values(data, n, filter_words)
            )

    def test_main_emptyInput_createsNoCsv(self):
        """main_emptyInput_createsNoCsv_expectedNoOutput: Should not create CSV report for empty input directory"""
        filter_words = ["anschaffungs", "ahk", "abschreibung", "buchwert"]
        N = 3
        with self.assertRaises(FileNotFoundError):
            generate_csv_report.generate_csv_report(
                self.empty_input_dir,
                self.output_file,
                N,
                lambda data, n: generate_csv_report.extract_values(data, n, filter_words)
            )

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        filter_words = ["anschaffungs", "ahk", "abschreibung", "buchwert"]
        N = 3
        with self.assertRaises(FileNotFoundError):
            generate_csv_report.generate_csv_report(
                self.missing_dir,
                self.output_file,
                N,
                lambda data, n: generate_csv_report.extract_values(data, n, filter_words)
            )

if __name__ == '__main__':
    unittest.main()
