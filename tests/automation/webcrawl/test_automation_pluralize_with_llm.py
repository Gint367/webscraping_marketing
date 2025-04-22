"""
Unit tests for pluralize_with_llm.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import sys
import asyncio
import unittest
from unittest.mock import patch
from typing import Any

class TestPluralizeWithLlm(unittest.TestCase):
    """Tests for pluralize_with_llm.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/webcrawl/data/valid_llm_json_dir'
        self.invalid_input_dir = 'tests/automation/webcrawl/data/invalid_llm_json_dir'
        self.empty_input_dir = 'tests/automation/webcrawl/data/empty_llm_json_dir'
        self.missing_dir = 'tests/automation/webcrawl/data/missing_llm_json_dir'
        self.output_dir = 'tests/automation/webcrawl/output/pluralized_output'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid JSON file
        with open(os.path.join(self.valid_input_dir, 'firma_a.json'), 'w') as f:
            f.write('{"keywords": ["Maschine", "Produkt"]}')
        # Create sample invalid JSON file (malformed)
        with open(os.path.join(self.invalid_input_dir, 'firma_b.json'), 'w') as f:
            f.write('not a json')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir, self.output_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)

    def test_main_validInput_createsPluralizedJson(self):
        """main_validInput_createsPluralizedJson_expectedJsonCreated: Should create pluralized JSON output for valid input"""
        import webcrawl.pluralize_with_llm as pluralize_with_llm
        test_args = [
            "pluralize_with_llm.py",
            "--input",
            self.valid_input_dir,
            "--output",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            pluralize_with_llm.main()
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No pluralized JSON output created for valid input")

    def test_main_invalidInput_malformedJson_raisesError(self):
        """main_invalidInput_malformedJson_raisesError_expectedException: Should raise error for malformed JSON input"""
        import webcrawl.pluralize_with_llm as pluralize_with_llm
        test_args = [
            "pluralize_with_llm.py",
            "--input",
            self.invalid_input_dir,
            "--output",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(Exception):
                pluralize_with_llm.main()

    def test_main_emptyInput_createsNoOutput(self):
        """main_emptyInput_createsNoOutput_expectedNoOutput: Should not create output for empty input directory"""
        import webcrawl.pluralize_with_llm as pluralize_with_llm
        test_args = [
            "pluralize_with_llm.py",
            "--input",
            self.empty_input_dir,
            "--output",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            pluralize_with_llm.main()
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input directory")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        import webcrawl.pluralize_with_llm as pluralize_with_llm
        test_args = [
            "pluralize_with_llm.py",
            "--input",
            self.missing_dir,
            "--output",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                pluralize_with_llm.main()

if __name__ == '__main__':
    unittest.main()
