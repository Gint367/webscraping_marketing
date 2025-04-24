"""
Unit tests for fill_process_type.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import json
import os
import sys
import unittest
from unittest.mock import patch


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
            f.write('[{"company_name": "BAGR Berliner Aluminiumwerk GmbH", "company_url": "https://www.bagr.de", "products": ["Aluminium-Gussblöcke", "Walzbarren", "Sekundäraluminium"], "machines": ["Schmelzöfen", "Gießöfen", "Porössteine"], "process_type": ["Schmelzen", "Gießen", "Raffinieren"], "lohnfertigung": false, "error": false}]')
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
            with self.assertRaises(json.JSONDecodeError):
                fill_process_type.main()

    def test_main_emptyInput_createsNoOutput(self):
        """main_emptyInput_createsNoOutput_expectedNoOutput: Should raise error for empty input file"""
        import webcrawl.fill_process_type as fill_process_type
        test_args = [
            "fill_process_type.py",
            "--input-file",
            self.empty_input_file,
            "--output-dir",
            self.output_dir
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(json.JSONDecodeError):
                fill_process_type.main()

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


class TestRunFillProcessType(unittest.TestCase):
    """Unit tests for run_fill_process_type function in fill_process_type.py."""

    def setUp(self):
        import webcrawl.fill_process_type as fill_process_type
        self.module = fill_process_type
        self.valid_input_file = 'tests/automation/webcrawl/data/valid_run_fill_process_type.json'
        self.invalid_input_file = 'tests/automation/webcrawl/data/invalid_run_fill_process_type.json'
        self.empty_input_file = 'tests/automation/webcrawl/data/empty_run_fill_process_type.json'
        self.output_dir = 'tests/automation/webcrawl/output/test_run_fill_process_type_out'
        os.makedirs(self.output_dir, exist_ok=True)
        # Valid input: list of companies
        with open(self.valid_input_file, 'w') as f:
            json.dump([
                {"company_name": "Test AG", "products": ["Produkt1"], "machines": ["Maschine1"], "process_type": None}
            ], f)
        # Invalid input: not JSON
        with open(self.invalid_input_file, 'w') as f:
            f.write('not a json')
        # Empty input
        with open(self.empty_input_file, 'w') as f:
            f.write('')

    def tearDown(self):
        for f in [self.valid_input_file, self.invalid_input_file, self.empty_input_file]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_dir):
            for file in os.listdir(self.output_dir):
                os.remove(os.path.join(self.output_dir, file))
            os.rmdir(self.output_dir)

    def test_run_fill_process_type_validInput_expectedOutputFile(self):
        """run_fill_process_type_validInput_expectedOutputFile: Should create output file for valid input"""
        output_files = self.module.run_fill_process_type(
            input_file=self.valid_input_file,
            output_dir=self.output_dir,
            log_level="INFO"
        )
        self.assertTrue(any(f.endswith('.json') for f in map(os.path.basename, output_files)))
        for f in output_files:
            self.assertTrue(os.path.exists(f))

    def test_run_fill_process_type_invalidInput_expectedJsonDecodeError(self):
        """run_fill_process_type_invalidInput_expectedJsonDecodeError: Should raise JSONDecodeError for malformed input"""
        with self.assertRaises(json.JSONDecodeError):
            self.module.run_fill_process_type(
                input_file=self.invalid_input_file,
                output_dir=self.output_dir,
                log_level="INFO"
            )

    def test_run_fill_process_type_emptyInput_expectedJsonDecodeError(self):
        """run_fill_process_type_emptyInput_expectedJsonDecodeError: Should raise JSONDecodeError for empty input file"""
        with self.assertRaises(json.JSONDecodeError):
            self.module.run_fill_process_type(
                input_file=self.empty_input_file,
                output_dir=self.output_dir,
                log_level="INFO"
            )

    def test_run_fill_process_type_missingInputFile_expectedFileNotFoundError(self):
        """run_fill_process_type_missingInputFile_expectedFileNotFoundError: Should raise FileNotFoundError for missing input file"""
        with self.assertRaises(FileNotFoundError):
            self.module.run_fill_process_type(
                input_file='tests/automation/webcrawl/data/does_not_exist.json',
                output_dir=self.output_dir,
                log_level="INFO"
            )


if __name__ == '__main__':
    unittest.main()
