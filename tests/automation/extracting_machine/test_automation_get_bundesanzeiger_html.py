"""
Unit tests for get_bundesanzeiger_html.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import unittest
from unittest.mock import patch

import extracting_machines.get_bundesanzeiger_html as get_bundesanzeiger_html


class TestGetBundesanzeigerHtml(unittest.TestCase):
    """Tests for get_bundesanzeiger_html.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input = 'tests/automation/extracting_machine/data/valid_companies.csv'
        self.invalid_input = 'tests/automation/extracting_machine/data/invalid_companies.csv'
        self.empty_input = 'tests/automation/extracting_machine/data/empty_companies.csv'
        self.missing_file = 'tests/automation/extracting_machine/data/missing_file.csv'
        self.output_dir = 'tests/automation/extracting_machine/output/bundesanzeiger_html_output'
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid input with correct header
        with open(self.valid_input, 'w') as f:
            f.write('company name,location\nFirma A,Berlin\nFirma B,Hamburg\n')
        # Create sample invalid input (missing columns)
        with open(self.invalid_input, 'w') as f:
            f.write('company,city\nX,Y\n')
        # Create empty input with correct header
        with open(self.empty_input, 'w') as f:
            f.write('company name,location\n')

    def tearDown(self) -> None:
        for f in [self.valid_input, self.invalid_input, self.empty_input]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_dir):
            for entry in os.listdir(self.output_dir):
                entry_path = os.path.join(self.output_dir, entry)
                if os.path.isdir(entry_path):
                    # Recursively remove directory
                    for root, dirs, files in os.walk(entry_path, topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in dirs:
                            os.rmdir(os.path.join(root, name))
                    os.rmdir(entry_path)
                else:
                    os.remove(entry_path)
            os.rmdir(self.output_dir)

    def test_main_validInput_createsHtmlFolders(self):
        """main_validInput_createsHtmlFolders: Should create HTML folders for each company in valid input"""
        mock_report = {
            "1": {
                "name": "Jahresabschluss 2022",
                "date": "2022-12-31 00:00:00",
                "raw_report": "<html><body><table><tr><td>Jahresabschluss</td><td>100.000</td><td>120.000</td></tr></table></body></html>",
                "report": "Some text report"
            }
        }
        with patch("extracting_machines.get_bundesanzeiger_html.Bundesanzeiger.get_reports", return_value=mock_report):
            get_bundesanzeiger_html.main(self.valid_input, self.output_dir)
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, 'Firma_A')))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, 'Firma_B')))

    def test_main_invalidInput_missingColumns_raisesError(self):
        """main_invalidInput_missingColumns_raisesError: Should raise ValueError for missing columns"""
        with self.assertRaises(ValueError):
            get_bundesanzeiger_html.main(self.invalid_input, self.output_dir)

    def test_main_emptyInput_onlyHeaders_createsNoFolders(self):
        """main_emptyInput_onlyHeaders_createsNoFolders: Should not create any company folders for empty input"""
        get_bundesanzeiger_html.main(self.empty_input, self.output_dir)
        # Only assert that no subdirectories (company folders) exist in the output directory
        subdirs = [entry for entry in os.listdir(self.output_dir)
                   if os.path.isdir(os.path.join(self.output_dir, entry))]
        self.assertEqual(len(subdirs), 0)

    def test_main_fileNotFound_raisesFileNotFoundError(self):
        """main_fileNotFound_raisesFileNotFoundError: Should raise FileNotFoundError for missing file"""
        with self.assertRaises(FileNotFoundError):
            get_bundesanzeiger_html.main(self.missing_file, self.output_dir)

if __name__ == '__main__':
    unittest.main()
