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
        os.makedirs(self.output_dir, exist_ok=True)
        # Setup valid input: valid_input_dir/CompanyA/2025-01-01/
        company = 'CompanyA'
        date_folder = '2025-01-01'
        company_dir = os.path.join(self.valid_input_dir, company, date_folder)
        os.makedirs(company_dir, exist_ok=True)
        valid_table_path = 'tests/automation/extracting_machine/valid_table.html'
        with open(valid_table_path, 'r') as infile:
            valid_html_content = infile.read()
        with open(os.path.join(company_dir, 'firma_a.html'), 'w') as outfile:
            outfile.write(valid_html_content)
        with open(os.path.join(company_dir, '2025-01-01_metadata.json'), 'w') as f:
            f.write('{"date": "2025-01-01T00:00:00", "company_name": "CompanyA"}')
        # Setup invalid input: invalid_input_dir/CompanyB/2025-01-01/
        company_b_dir = os.path.join(self.invalid_input_dir, 'CompanyB', date_folder)
        os.makedirs(company_b_dir, exist_ok=True)
        with open(os.path.join(company_b_dir, 'firma_b.html'), 'w') as f:
            f.write('<html><body>irrelevant content</body></html>')
        with open(os.path.join(company_b_dir, '2025-01-01_metadata.json'), 'w') as f:
            f.write('{"date": "2025-01-01T00:00:00", "company_name": "CompanyB"}')
        # Setup empty input dir
        os.makedirs(self.empty_input_dir, exist_ok=True)

    def tearDown(self) -> None:
        import shutil
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir, self.output_dir]:
            if os.path.exists(d):
                shutil.rmtree(d)

    def test_main_validInput_createsCleanedJson(self):
        """main_validInput_createsCleanedJson_expectedJsonCreated: Should create cleaned JSON output for relevant HTML files"""
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/clean_html.py'))
        result = subprocess.run([
            sys.executable, script_path,
            '--input_dir', self.valid_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input")

    def test_main_invalidInput_noRelevantData_createsNoJson(self):
        """main_invalidInput_noRelevantData_createsNoJson_expectedNoOutput: Should not create JSON output for irrelevant input"""
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/clean_html.py'))
        result = subprocess.run([
            sys.executable, script_path,
            '--input_dir', self.invalid_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertFalse(any(f.endswith('.json') for f in output_files), "JSON output should not be created for irrelevant input")

    def test_main_emptyInput_createsNoJson(self):
        """main_emptyInput_createsNoJson_expectedNoOutput: Should not create JSON output for empty input directory"""
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/clean_html.py'))
        result = subprocess.run([
            sys.executable, script_path,
            '--input_dir', self.empty_input_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Script failed: {result.stderr}")
        output_files = os.listdir(self.output_dir)
        self.assertFalse(any(f.endswith('.json') for f in output_files), "JSON output should not be created for empty input")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../scraper/extracting_machines/clean_html.py'))
        result = subprocess.run([
            sys.executable, script_path,
            '--input_dir', self.missing_dir,
            '--output_dir', self.output_dir
        ], cwd=os.getcwd(), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Input directory", result.stderr)

if __name__ == '__main__':
    unittest.main()
