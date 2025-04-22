"""
Unit tests for clean_html.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import subprocess
import sys
import unittest

class TestCleanHtml(unittest.TestCase):
    """Tests for clean_html.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/extracting_machine/data/valid_html_dir'
        self.invalid_input_dir = 'tests/automation/extracting_machine/data/invalid_html_dir'
        self.empty_input_dir = 'tests/automation/extracting_machine/data/empty_html_dir'
        self.missing_dir = 'tests/automation/extracting_machine/data/missing_html_dir'
        self.output_dir = 'tests/automation/extracting_machine/output/cleaned_html_output'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid HTML file
        with open(os.path.join(self.valid_input_dir, 'firma_a.html'), 'w') as f:
            f.write('<html><body>technische anlagen</body></html>')
        # Create sample invalid HTML file (not containing relevant data)
        with open(os.path.join(self.invalid_input_dir, 'firma_b.html'), 'w') as f:
            f.write('<html><body>irrelevant content</body></html>')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir, self.output_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)

    def test_main_validInput_createsCleanedJson(self):
        """main_validInput_createsCleanedJson_expectedJsonCreated: Should create cleaned JSON output for relevant HTML files"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../extracting_machines/clean_html.py')),
            self.valid_input_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input")

    def test_main_invalidInput_noRelevantData_createsNoJson(self):
        """main_invalidInput_noRelevantData_createsNoJson_expectedNoOutput: Should not create JSON output for irrelevant input"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../extracting_machines/clean_html.py')),
            self.invalid_input_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertFalse(any(f.endswith('.json') for f in output_files), "JSON output should not be created for irrelevant input")

    def test_main_emptyInput_createsNoJson(self):
        """main_emptyInput_createsNoJson_expectedNoOutput: Should not create JSON output for empty input directory"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../extracting_machines/clean_html.py')),
            self.empty_input_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input directory")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        result = subprocess.run([
            sys.executable, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../extracting_machines/clean_html.py')),
            self.missing_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0, "Script should fail for missing input directory")
        self.assertIn("Usage", result.stdout + result.stderr)

if __name__ == '__main__':
    unittest.main()
