"""
Unit tests for consolidate.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import sys
import unittest
from unittest.mock import patch


class TestConsolidate(unittest.TestCase):
    """Tests for consolidate.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/webcrawl/data/valid_pluralized_json_dir'
        self.invalid_input_dir = 'tests/automation/webcrawl/data/invalid_pluralized_json_dir'
        self.empty_input_dir = 'tests/automation/webcrawl/data/empty_pluralized_json_dir'
        self.missing_dir = 'tests/automation/webcrawl/data/missing_pluralized_json_dir'
        self.output_file = 'tests/automation/webcrawl/output/consolidated_output.json'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        # Create sample valid JSON file
        with open(os.path.join(self.valid_input_dir, 'firma_a.json'), 'w') as f:
            f.write('{"keywords": ["Maschinen", "Produkte"]}')
        # Create sample invalid JSON file (malformed)
        with open(os.path.join(self.invalid_input_dir, 'firma_b.json'), 'w') as f:
            f.write('not a json')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def test_main_validInput_createsConsolidatedJson(self):
        """main_validInput_createsConsolidatedJson_expectedJsonCreated: Should create consolidated JSON output for valid input"""
        import webcrawl.consolidate as consolidate
        test_args = [
            "consolidate.py",
            self.valid_input_dir,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            consolidate.main()
        self.assertTrue(os.path.exists(self.output_file), "Consolidated JSON output was not created for valid input")

    def test_main_invalidInput_malformedJson_raisesError(self):
        """main_invalidInput_malformedJson_raisesError_expectedException: Should raise error for malformed JSON input"""
        import webcrawl.consolidate as consolidate
        test_args = [
            "consolidate.py",
            self.invalid_input_dir,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(Exception):
                consolidate.main()

    def test_main_emptyInput_createsEmptyJson(self):
        """main_emptyInput_createsEmptyJson_expectedEmptyOutput: Should create empty JSON output for empty input directory"""
        import webcrawl.consolidate as consolidate
        test_args = [
            "consolidate.py",
            self.empty_input_dir,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            consolidate.main()
        self.assertTrue(os.path.exists(self.output_file), "Output file should be created for empty input directory")
        with open(self.output_file, 'r', encoding='utf-8') as f:
            data = f.read().strip()
            self.assertTrue(data == '[]' or data == '', "Output file should be empty or contain an empty list for empty input directory")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        import webcrawl.consolidate as consolidate
        test_args = [
            "consolidate.py",
            self.missing_dir,
            "--output",
            self.output_file
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                consolidate.main()

if __name__ == '__main__':
    unittest.main()
