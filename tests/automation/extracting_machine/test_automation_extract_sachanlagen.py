"""
Unit tests for extract_sachanlagen.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import subprocess
import sys
import unittest


class TestExtractSachanlagen(unittest.TestCase):
    """Tests for extract_sachanlagen.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/extracting_machine/data/valid_sachanlagen_html'
        self.invalid_input_dir = 'tests/automation/extracting_machine/data/invalid_sachanlagen_html'
        self.empty_input_dir = 'tests/automation/extracting_machine/data/empty_sachanlagen_html'
        self.missing_dir = 'tests/automation/extracting_machine/data/missing_sachanlagen_html'
        self.output_dir = 'tests/automation/extracting_machine/output/sachanlagen_output'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid HTML file
        valid_table_path = 'tests/automation/extracting_machine/valid_table.html'
        with open(valid_table_path, 'r') as infile:
            valid_html_content = infile.read()
        with open(os.path.join(self.valid_input_dir, 'firma_a.html'), 'w') as outfile:
            outfile.write(valid_html_content)

        # Create sample invalid HTML file (irrelevant content)
        with open(os.path.join(self.invalid_input_dir, 'firma_b.html'), 'w') as f:
            f.write('<html><body>irrelevant content</body></html>')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir, self.output_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)

    def test_main_validInput_createsSachanlagenJsonAndCsv(self):
        """main_validInput_createsSachanlagenJsonAndCsv_expectedJsonAndCsvCreated: Should create Sachanlagen JSON and summary CSV output for valid input"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/extract_sachanlagen.py')),
            self.valid_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input")
        self.assertTrue(any(f.endswith('.csv') for f in output_files), "No CSV output created for valid input")

    def test_main_invalidInput_noRelevantData_createsNoJsonOrCsv(self):
        """main_invalidInput_noRelevantData_createsNoJsonOrCsv_expectedNoOutput: Should not create JSON or CSV output for irrelevant input"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/extract_sachanlagen.py')),
            self.invalid_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertFalse(any(f.endswith('.json') for f in output_files), "JSON output should not be created for irrelevant input")
        self.assertFalse(any(f.endswith('.csv') for f in output_files), "CSV output should not be created for irrelevant input")

    def test_main_emptyInput_createsNoOutput(self):
        """main_emptyInput_createsNoOutput_expectedNoOutput: Should not create any output for empty input directory"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/extract_sachanlagen.py')),
            self.empty_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input directory")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/extract_sachanlagen.py')),
            self.missing_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0, "Script should fail for missing input directory")
        self.assertIn("does not exist", result.stdout + result.stderr)

if __name__ == '__main__':
    unittest.main()
