"""
Unit tests for get_company_by_category.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import unittest

import pandas as pd

from extracting_machines.get_company_by_category import (
    main as get_company_by_category_main,
)


class TestGetCompanyByCategory(unittest.TestCase):
    """Tests for get_company_by_category.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input = 'tests/automation/extracting_machine/data/valid_companies.xlsx'
        self.invalid_input = 'tests/automation/extracting_machine/data/invalid_companies.xlsx'
        self.empty_input = 'tests/automation/extracting_machine/data/empty_companies.xlsx'
        self.missing_file = 'tests/automation/extracting_machine/data/missing_file.xlsx'
        self.category = 'Maschinenbau'
        self.output_csv = 'tests/automation/extracting_machine/output/company_Maschinenbau_BA.csv'
        os.makedirs('tests/automation/extracting_machine/data', exist_ok=True)
        os.makedirs('tests/automation/extracting_machine/output', exist_ok=True)
        # Create sample valid input
        df = pd.DataFrame({
            'Firma1': ['Firma A', 'Firma B'],
            'Ort': ['Berlin', 'Hamburg'],
            'Kategorie': ['Maschinenbau', 'IT'],
            'URL': ['http://example.com/a', 'http://example.com/b'],
        })
        df.to_excel(self.valid_input, index=False)
        # Create sample invalid input (missing columns)
        df_invalid = pd.DataFrame({'Firma1': ['X'], 'city': ['Y']})
        df_invalid.to_excel(self.invalid_input, index=False)
        # Create empty input
        pd.DataFrame().to_excel(self.empty_input, index=False)

    def tearDown(self) -> None:
        for f in [self.valid_input, self.invalid_input, self.empty_input]:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(self.output_csv):
            os.remove(self.output_csv)

    def test_main_validInput_returnsFilteredCSV(self):
        """main valid input returns filtered CSV with correct companies"""
        get_company_by_category_main(self.valid_input, self.category, self.output_csv)
        self.assertTrue(os.path.exists(self.output_csv))
        df = pd.read_csv(self.output_csv)
        self.assertIn('Firma A', df['company name'].values)
        self.assertNotIn('Firma B', df['company name'].values)

    def test_main_invalidInput_missingColumns_raisesError(self):
        """main invalid input (missing columns) raises error"""
        with self.assertRaises(ValueError):
            get_company_by_category_main(self.invalid_input, self.category, self.output_csv)

    def test_main_emptyInput_onlyHeaders_raisesValueError(self):
        """main when input is only headers should raise ValueError for missing required columns"""
        with self.assertRaises(ValueError):
            get_company_by_category_main(self.empty_input, self.category, self.output_csv)

    def test_main_fileNotFound_raisesFileNotFoundError(self):
        """main file not found raises FileNotFoundError"""
        with self.assertRaises(FileNotFoundError):
            get_company_by_category_main(self.missing_file, self.category, self.output_csv)

if __name__ == '__main__':
    unittest.main()
