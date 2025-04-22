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
        with open(os.path.join(company_dir, 'firma_a.html'), 'w') as f:
            f.write('''<html><h3 class="b_teil" id="jp_Bilanz_zum_31._Dezember_2018">
         Bilanz zum 31. Dezember 2018
        </h3>
        <p>
         <b>
          AKTIVA
         </b>
        </p>
        <table border="0" cellpadding="2" cellspacing="0" class="std_table" width="900">
         <colgroup>
          <col/>
          <col width="15%"/>
          <col width="15%"/>
          <col width="15%"/>
         </colgroup>
         <thead>
          <tr bgcolor="#cdcdce">
           <td style="vertical-align:left;text-align:left;">
           </td>
           <td style="vertical-align:right;text-align:right;">
           </td>
           <td style="vertical-align:right;text-align:right;">
            31.12.2018
           </td>
           <td style="vertical-align:right;text-align:right;">
            31.12.2017
           </td>
          </tr>
          <tr bgcolor="#cdcdce">
           <td style="vertical-align:left;text-align:left;">
           </td>
           <td style="vertical-align:right;text-align:right;">
            EUR
           </td>
           <td style="vertical-align:right;text-align:right;">
            EUR
           </td>
           <td style="vertical-align:right;text-align:right;">
            EUR
           </td>
          </tr>
         </thead>
         <tbody>
          <tr>
           <td style="text-align:left;">
            A ANLAGEVERMÖGEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            I. Immaterielle Vermögensgegenstände
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            Entgeltlich erworbene Software sowie sonstige Rechte
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            2.854.647,00
           </td>
           <td style="text-align:right;">
            3.034.603,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            II. Sachanlagen
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            1. Grundstücke, grundstücksgleiche Rechte und Bauten einschließlich der Bauten auf
                     fremden Grundstücken
           </td>
           <td style="text-align:right;">
            20.402.578,36
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            20.318.424,85
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            2. Technische Anlagen und Maschinen
           </td>
           <td style="text-align:right;">
            59.870.412,00
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            61.511.716,00
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            3. Andere Anlagen, Betriebs- und Geschäftsausstattung
           </td>
           <td style="text-align:right;">
            2.624.087,00
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            2.577.080,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            4. Geleistete Anzahlungen und Anlagen im Bau
           </td>
           <td style="text-align:right;">
            4.081.305,80
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            5.226.850,71
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            86.978.383,16
           </td>
           <td style="text-align:right;">
            89.634.071,56
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            III. Finanzanlagen
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            Beteiligungen
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            8.300,00
           </td>
           <td style="text-align:right;">
            8.300,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            89.841.330,16
           </td>
           <td style="text-align:right;">
            92.676.974,56
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            B. UMLAUFVERMÖGEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            I. Vorräte
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            1. Roh-, Hilfs- und Betriebsstoffe
           </td>
           <td style="text-align:right;">
            9.254.342,50
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            8.895.357,89
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            2. Unfertige Erzeugnisse
           </td>
           <td style="text-align:right;">
            20.046.396,16
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            16.550.795,88
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            3. Fertige Erzeugnisse
           </td>
           <td style="text-align:right;">
            823.000,00
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            823.000,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            4. Emissionsberechtigungen
           </td>
           <td style="text-align:right;">
            2.126.177,06
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            1.209.301,84
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            32.249.915,72
           </td>
           <td style="text-align:right;">
            27.478.455,61
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            II. Forderungen und sonstige Vermögensgegenstände
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            1. Forderungen aus Lieferungen und Leistungen
           </td>
           <td style="text-align:right;">
            417.101,51
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            197.968,10
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            2. Forderungen gegen Gesellschafter
           </td>
           <td style="text-align:right;">
            33.696.620,51
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            42.128.617,65
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            davon aus Lieferungen und Leistungen: EUR 33.696.620,51 (Vj.: EUR 42.128.617,65)
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            3. Sonstige Vermögensgegenstände
           </td>
           <td style="text-align:right;">
            8.680.004,66
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            7.832.544,39
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            42.793.726,68
           </td>
           <td style="text-align:right;">
            50.159.130,14
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            III. Kassenbestand und Guthaben bei Kreditinstituten
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            41.614.070,40
           </td>
           <td style="text-align:right;">
            19.357.175,09
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            116.657.712,80
           </td>
           <td style="text-align:right;">
            96.994.760,84
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            C. RECHNUNGSABGRENZUNGSPOSTEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            928.825,90
           </td>
           <td style="text-align:right;">
            644.134,65
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            D. AKTIVER UNTERSCHIEDSBETRAG AUS DER VERMÖGENSVERRECHNUNG
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            41.876,48
           </td>
           <td style="text-align:right;">
            41.485,38
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            207.469.745, 34
           </td>
           <td style="text-align:right;">
            190.357.355, 43
           </td>
          </tr>
          <tr bgcolor="#ffffff">
           <td style="text-align:left;">
            <p class="tbl_p_fl">
             <b>
              PASSIVA
             </b>
            </p>
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#cdcdce" style="vertical-align:bottom;">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            31.12.2018
           </td>
           <td style="text-align:right;">
            31.12.2017
           </td>
          </tr>
          <tr bgcolor="#cdcdce" style="vertical-align:bottom;">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
            EUR
           </td>
           <td style="text-align:right;">
            EUR
           </td>
           <td style="text-align:right;">
            EUR
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            A. EIGENKAPITAL
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            I. Gezeichnetes Kapital
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            36.000.000,00
           </td>
           <td style="text-align:right;">
            36.000.000,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            II. Gewinnrücklagen
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            Andere Gewinnrücklagen
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            26.949.653,09
           </td>
           <td style="text-align:right;">
            26.949.653,09
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            III. Gewinnvortrag
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            22.764.448,15
           </td>
           <td style="text-align:right;">
            21.451.727,82
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            IV. Jahresüberschuss
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            379.989,34
           </td>
           <td style="text-align:right;">
            1.312.720,33
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            86.094.090,58
           </td>
           <td style="text-align:right;">
            85.714.101,24
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            B. RÜCKSTELLUNGEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            1. Rückstellungen für Pensionen und ähnliche Verpflichtungen
           </td>
           <td style="text-align:right;">
            50.775.992,00
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            45.332.534,00
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            2. Steuerrückstellungen
           </td>
           <td style="text-align:right;">
            3.822.604,67
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            1.224.400,00
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            3. Sonstige Rückstellungen
           </td>
           <td style="text-align:right;">
            53.337.228.82
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            45.530.162.70
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            107.935.8449
           </td>
           <td style="text-align:right;">
            92.087.096,70
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            C. VERBINDLICHKEITEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            1. Verbindlichkeiten aus Lieferungen und Leistungen
           </td>
           <td style="text-align:right;">
            12.937.559,83
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            12.106.159,90
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            davon mit einer Restlaufzeit bis zu einem Jahr: EUR 12.937.559,83 (Vj.: EUR 12.106159,90)
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            2. Sonstige Verbindlichkeiten
           </td>
           <td style="text-align:right;">
            500.612,06
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            396.234,90
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
            davon mit einer Restlaufzeit bis zu einem Jahr: EUR 497.969,74 (Vj.: EUR 396.234,90)
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            davon aus Steuern: EUR 349.995,98 (Vj.: EUR 310.682,91)
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            13.438.171,89
           </td>
           <td style="text-align:right;">
            12.502.394,80
           </td>
          </tr>
          <tr>
           <td style="text-align:left;">
            D. RECHNUNGSABGRENZUNGSPOSTEN
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            1.657,38
           </td>
           <td style="text-align:right;">
            53.762,69
           </td>
          </tr>
          <tr bgcolor="#f0f0f0">
           <td style="text-align:left;">
           </td>
           <td style="text-align:right;">
           </td>
           <td style="text-align:right;">
            207.469.745 34
           </td>
           <td style="text-align:right;">
            190.357.355 43
           </td>
          </tr>
         </tbody>
        </table></table>\n''')
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
