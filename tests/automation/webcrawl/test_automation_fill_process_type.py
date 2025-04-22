"""
Unit tests for fill_process_type.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import sys
import unittest
from unittest.mock import patch
from typing import Any

class TestFillProcessType(unittest.TestCase):
    """Tests for fill_process_type.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_file = 'tests/automation/webcrawl/data/valid_consolidated_json.json'
        self.invalid_input_file = 'tests/automation/webcrawl/data/invalid_consolidated_json.json'
        self.empty_input_file = 'tests/automation/webcrawl/data/empty_consolidated_json.json'
        self.missing_file = 'tests/automation/webcrawl/data/missing_consolidated_json.json'
        self.output_dir = 'tests/automation/webcrawl/output/enhanced_output'
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid JSON file
        with open(self.valid_input_file, 'w') as f:
            f.write('{"keywords": ["Maschinen"], "process_type": null}')
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
        if os.path.exists(self.output_dir):
            for file in os.listdir(self.output_dir):
                os.remove(os.path.join(self.output_dir, file))
            os.rmdir(self.output_dir)

    def test_main_validInput_createsEnhancedJson(self):
        """main_validInput_createsEnhancedJson_expectedJsonCreated: Should create enhanced JSON output for valid input"""
        import webcrawl.fill_process_type as fill_process_type
        test_args = [
            "fill_process_type.py",
            "--input-file",
            self.valid_input_file,
            "--output-dir",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            fill_process_type.main()
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No enhanced JSON output created for valid input")

    def test_main_invalidInput_malformedJson_raisesError(self):
        """main_invalidInput_malformedJson_raisesError_expectedException: Should raise error for malformed JSON input"""
        import webcrawl.fill_process_type as fill_process_type
        test_args = [
            "fill_process_type.py",
            "--input-file",
            self.invalid_input_file,
            "--output-dir",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(Exception):
                fill_process_type.main()

    def test_main_emptyInput_createsNoOutput(self):
        """main_emptyInput_createsNoOutput_expectedNoOutput: Should not create output for empty input file"""
        import webcrawl.fill_process_type as fill_process_type
        test_args = [
            "fill_process_type.py",
            "--input-file",
            self.empty_input_file,
            "--output-dir",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            fill_process_type.main()
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input file")

    def test_main_missingInputFile_raisesFileNotFoundError(self):
        """main_missingInputFile_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input file"""
        import webcrawl.fill_process_type as fill_process_type
        test_args = [
            "fill_process_type.py",
            "--input-file",
            self.missing_file,
            "--output-dir",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                fill_process_type.main()

if __name__ == '__main__':
    unittest.main()
