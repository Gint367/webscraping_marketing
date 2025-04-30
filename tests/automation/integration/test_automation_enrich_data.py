"""
Unit tests for enrich_data.py automation pipeline integration.
Covers enriching merged data with additional metrics.
"""
import os
import sys
import unittest
from unittest.mock import patch

import merge_pipeline.enrich_data as enrich_data


class TestEnrichData(unittest.TestCase):
    """Tests for enrich_data.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input = 'tests/automation/integration/data/valid_merged.csv'
        self.invalid_input = 'tests/automation/integration/data/invalid_merged.csv'
        self.empty_input = 'tests/automation/integration/data/empty_merged.csv'
        self.missing_file = 'tests/automation/integration/data/missing.csv'
        self.output_file = 'tests/automation/integration/output/enriched_output.csv'
        os.makedirs('tests/automation/integration/data', exist_ok=True)
        os.makedirs('tests/automation/integration/output', exist_ok=True)
        # Create sample valid merged CSV
        with open(self.valid_input, 'w') as f:
            f.write('Company name,Company Url,Lohnfertigung(True/False),Produkte_1,Produkte_2,Produkte_3,Maschinen_1,Maschinen_2,Maschinen_3,Prozess_1,Prozess_2,Prozess_3,Ort,technische Anlagen und Maschinen 2021/22,Maschinen_Park_Size\n')
            f.write('MWS Friedrichshafen GmbH,https://www.mws.eu,True,Sandgüsse,Kokillengüsse,Niederdruckgüsse,9840033.0,200-350,11166783,Aluminium-Gussblöcke,Walzbarren,Sekundäraluminium,Friedrichshafen,11166783,200-350\n')
        # Create invalid CSV (malformed)
        with open(self.invalid_input, 'w') as f:
            f.write('Company name,Company Url,Lohnfertigung(True/False\n')  # Missing closing parenthesis
            f.write('MWS Friedrichshafen GmbH,"https://www.mws.eu,True,Sandgüsse')  # Unmatched quotes
        # Create empty CSV
        with open(self.empty_input, 'w') as f:
            f.write('Company name,Company Url,Lohnfertigung(True/False),Produkte_1,Produkte_2,Produkte_3,Maschinen_1,Maschinen_2,Maschinen_3,Prozess_1,Prozess_2,Prozess_3,Ort,technische Anlagen und Maschinen 2021/22,Maschinen_Park_Size\n')

    def tearDown(self) -> None:
        for f in [self.valid_input, self.invalid_input, self.empty_input]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_main_withValidInput_createsEnrichedCsv(self):
        """main_withValidInput_createsEnrichedCsv_expectedCsvCreated: Should create enriched CSV output for valid merged CSV"""
        test_args = [
            "enrich_data.py",
            self.valid_input
        ]
        with patch.object(sys, 'argv', test_args):
            enrich_data.main()
        # Output file is generated with 'enriched_' prefix in the same directory as input
        output_basename = f"enriched_{os.path.basename(self.valid_input)}"
        output_file = os.path.join(os.path.dirname(self.valid_input), output_basename)
        self.assertTrue(os.path.exists(output_file), "Enriched output CSV was not created for valid input")

    def test_main_withMalformedInput_raisesValueError(self):
        """main_withMalformedInput_raisesValueError_expectedException: Should raise ValueError for malformed merged CSV"""
        test_args = [
            "enrich_data.py",
            self.invalid_input
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(ValueError):
                enrich_data.main()

    def test_main_withMissingFile_raisesFileNotFoundError(self):
        """main_withMissingFile_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input file"""
        test_args = [
            "enrich_data.py",
            self.missing_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                enrich_data.main()

    def test_main_withEmptyInput_createsEmptyOutput(self):
        """main_withEmptyInput_createsEmptyOutput_expectedEmptyOutput: Should generate empty output CSV for empty input"""
        test_args = [
            "enrich_data.py",
            self.empty_input
        ]
        with patch.object(sys, 'argv', test_args):
            enrich_data.main()
        output_basename = f"enriched_{os.path.basename(self.empty_input)}"
        output_file = os.path.join(os.path.dirname(self.empty_input), output_basename)
        self.assertTrue(os.path.exists(output_file), "Output file should be created for empty input CSV")

if __name__ == '__main__':
    unittest.main()
