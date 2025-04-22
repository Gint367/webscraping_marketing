"""
Unit tests for merge_technische_anlagen_with_keywords.py automation pipeline integration.
Covers merging keyword CSV with technical equipment CSV.
"""
import os
import sys
import unittest
from unittest.mock import patch
from typing import Any
import merge_technische_anlagen_with_keywords  # Assuming this is the module to be tested

class TestMergeTechnischeAnlagenWithKeywords(unittest.TestCase):
    """Tests for merge_technische_anlagen_with_keywords.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_csv = 'tests/automation/integration/data/valid_keywords.csv'
        self.valid_base = 'tests/automation/integration/data/valid_technische.csv'
        self.invalid_csv = 'tests/automation/integration/data/invalid_keywords.csv'
        self.invalid_base = 'tests/automation/integration/data/invalid_technische.csv'
        self.empty_csv = 'tests/automation/integration/data/empty_keywords.csv'
        self.empty_base = 'tests/automation/integration/data/empty_technische.csv'
        self.missing_file = 'tests/automation/integration/data/missing.csv'
        self.output_file = 'tests/automation/integration/output/merged_output.csv'
        os.makedirs('tests/automation/integration/data', exist_ok=True)
        os.makedirs('tests/automation/integration/output', exist_ok=True)
        # Create sample valid CSVs
        with open(self.valid_csv, 'w') as f:
            f.write('company,keyword_count\nFirma A,10\n')
        with open(self.valid_base, 'w') as f:
            f.write('company,assets\nFirma A,100000\n')
        # Create invalid CSV (malformed)
        with open(self.invalid_csv, 'w') as f:
            f.write('not,a,csv')
        with open(self.invalid_base, 'w') as f:
            f.write('not a csv')
        # Create empty CSVs
        with open(self.empty_csv, 'w') as f:
            f.write('company,keyword_count\n')
        with open(self.empty_base, 'w') as f:
            f.write('company,assets\n')

    def tearDown(self) -> None:
        # Clean up data files
        for f in [self.valid_csv, self.valid_base, self.invalid_csv, self.invalid_base,
                  self.empty_csv, self.empty_base]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_main_withValidInputs_mergesIntoOutputCsv(self):
        """main_withValidInputs_mergesIntoOutputCsv_expectedCsvCreated: Should merge keyword and technical CSV to output CSV"""
        test_args = [
            "merge_technische_anlagen_with_keywords.py",
            "--csv",
            self.valid_csv,
            "--base",
            self.valid_base,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            merge_technische_anlagen_with_keywords.main()
        self.assertTrue(os.path.exists(self.output_file), "Merged output CSV was not created for valid input")

    def test_main_withMalformedCsv_raisesValueError(self):
        """main_withMalformedCsv_raisesValueError_expectedException: Should raise ValueError for malformed CSV"""
        test_args = [
            "merge_technische_anlagen_with_keywords.py",
            "--csv",
            self.invalid_csv,
            "--base",
            self.valid_base,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(ValueError):
                merge_technische_anlagen_with_keywords.main()

    def test_main_withMissingFile_raisesFileNotFoundError(self):
        """main_withMissingFile_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input file"""
        test_args = [
            "merge_technische_anlagen_with_keywords.py",
            "--csv",
            self.missing_file,
            "--base",
            self.valid_base,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                merge_technische_anlagen_with_keywords.main()

    def test_main_withEmptyCsv_generatesEmptyOutput(self):
        """main_withEmptyCsv_generatesEmptyOutput_expectedEmptyOutput: Should generate empty output CSV for empty input"""
        test_args = [
            "merge_technische_anlagen_with_keywords.py",
            "--csv",
            self.empty_csv,
            "--base",
            self.empty_base,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            merge_technische_anlagen_with_keywords.main()
        self.assertTrue(os.path.exists(self.output_file), "Output file should be created for empty input CSVs")
        self.assertEqual(os.path.getsize(self.output_file), 0, "Output file should be empty for empty input CSVs")

if __name__ == '__main__':
    unittest.main()
