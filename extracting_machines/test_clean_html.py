import pytest
import json
from pathlib import Path
import tempfile
import shutil
from extracting_machines.clean_html import clean_html, filter_word_rows, get_latest_subfolder

# Test data path
TEST_DATA_DIR = Path("test_bundesanzeiger_local_data")

@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture to ensure test data directory exists"""
    TEST_DATA_DIR.mkdir(exist_ok=True)
    return TEST_DATA_DIR

@pytest.fixture
def sample_company_structure(test_data_dir):
    """Creates a sample company folder structure with test data"""
    company_dir = test_data_dir / "test_company"
    company_dir.mkdir(exist_ok=True)
    
    # Create two subfolders with different dates
    old_folder = company_dir / "2023_01_01"
    new_folder = company_dir / "2023_02_01"
    old_folder.mkdir(exist_ok=True)
    new_folder.mkdir(exist_ok=True)
    
    # Create metadata files
    old_metadata = {
        "name": "Old Report",
        "date": "2023-01-01T10:00:00"
    }
    new_metadata = {
        "name": "New Report",
        "date": "2023-02-01T10:00:00"
    }
    
    with open(old_folder / "metadata.json", "w", encoding='utf-8') as f:
        json.dump(old_metadata, f)
    with open(new_folder / "metadata.json", "w", encoding='utf-8') as f:
        json.dump(new_metadata, f)
    
    # Create test HTML files
    test_html = """
    <h1>Test Report</h1>
    <table>
        <tr><th>Category</th><th>Description</th></tr>
        <tr><td>Assets</td><td>Technische Anlagen</td></tr>
    </table>
    """
    
    with open(new_folder / "report.html", "w", encoding='utf-8') as f:
        f.write(test_html)
        
    yield company_dir
    
    # Cleanup
    shutil.rmtree(company_dir)

@pytest.fixture
def sample_html():
    with open("sample_html.html", "r") as file:
        return file.read()
    
def test_clean_html_empty():
    assert clean_html("") is None
    assert clean_html("<html></html>") is None

def test_clean_html_no_tables():
    html = "<html><p>No tables here</p></html>"
    assert clean_html(html) is None

def test_clean_html_with_filter():
    html = """
    <h1>Header</h1>
    <table><tr><td>technische anlagen</td></tr></table>
    """
    result = clean_html(html, filter_word="technische anlagen")
    assert "technische anlagen" in result
    assert "<h1>Header</h1>" in result

# sample has table at the root!
def test_filter_word_rows_basic(sample_html):
    results = filter_word_rows(sample_html, "technische anlagen")
    assert len(results) == 5
    assert results[0]['table_name'] == "Unknown Table"
    assert len(results[0]['matching_rows']) == 1

def test_filter_word_rows_headers():
    html = """
    <table>
        <thead>
            <tr><th>Col1</th><th colspan="2">Col2</th></tr>
        </thead>
        <tbody>
            <tr><td>Data1</td><td>technische anlagen</td><td>Data3</td></tr>
        </tbody>
    </table>
    """
    results = filter_word_rows(html, "technische anlagen")
    row = results[0]['matching_rows'][0]
    assert row['header1'] == ['Col1', 'Col2', 'Col2']

@pytest.fixture
def temp_company_folder():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

def test_get_latest_subfolder(sample_company_structure):
    latest = get_latest_subfolder(str(sample_company_structure))
    assert Path(latest).name == "2023_02_01"
    
def test_get_latest_subfolder_empty(test_data_dir):
    empty_company = test_data_dir / "empty_company"
    empty_company.mkdir(exist_ok=True)
    assert get_latest_subfolder(str(empty_company)) is None
    empty_company.rmdir()

def test_get_latest_subfolder_invalid_json(temp_company_folder):
    subfolder = temp_company_folder / "sub1"
    subfolder.mkdir()
    
    with open(subfolder / "metadata.json", "w") as f:
        f.write("invalid json")
        
    assert get_latest_subfolder(str(temp_company_folder)) is None

def create_test_html_with_tables():
    return """
    <html>
    <body>
        <h1>Company Report</h1>
        <table>
            <tr><th>Assets</th><th>Value</th></tr>
            <tr><td>Technische Anlagen</td><td>1000000</td></tr>
            <tr><td>Other Assets</td><td>500000</td></tr>
        </table>
        <table>
            <tr><th>Liabilities</th><th>Amount</th></tr>
            <tr><td>Bank Loans</td><td>300000</td></tr>
        </table>
    </body>
    </html>
    """

@pytest.fixture
def complete_test_structure(test_data_dir):
    """Creates a complete test structure with multiple companies and reports"""
    # Create companies
    companies = ["TestCompany1", "TestCompany2"]
    dates = ["2023_01_01", "2023_02_01"]
    
    created_paths = []
    
    for company in companies:
        company_dir = test_data_dir / company
        company_dir.mkdir(exist_ok=True)
        created_paths.append(company_dir)
        
        for date in dates:
            report_dir = company_dir / date
            report_dir.mkdir(exist_ok=True)
            
            # Create metadata
            metadata = {
                "name": f"{company} Report",
                "date": f"{date.replace('_', '-')}T10:00:00"
            }
            
            with open(report_dir / "metadata.json", "w", encoding='utf-8') as f:
                json.dump(metadata, f)
            
            # Create HTML report
            with open(report_dir / "report.html", "w", encoding='utf-8') as f:
                f.write(create_test_html_with_tables())
    
    yield test_data_dir
    
    # Cleanup created directories
    for path in created_paths:
        shutil.rmtree(path)

def test_full_folder_processing(complete_test_structure):
    """Integration test for processing multiple companies and reports"""
    # Test processing TestCompany1
    company_path = complete_test_structure / "TestCompany1"
    latest_folder = get_latest_subfolder(str(company_path))
    
    assert latest_folder is not None
    assert Path(latest_folder).name == "2023_02_01"
    
    # Read and process the HTML
    html_path = Path(latest_folder) / "report.html"
    with open(html_path, "r", encoding='utf-8') as f:
        html_content = f.read()
    
    # Test HTML cleaning with filter
    cleaned_html = clean_html(html_content, filter_word="technische anlagen")
    assert cleaned_html is not None
    assert "Technische Anlagen" in cleaned_html
    assert "1000000" in cleaned_html
    
    # Test row filtering
    filtered_rows = filter_word_rows(html_content, "technische anlagen")
    assert len(filtered_rows) > 0
    assert filtered_rows[0]['table_name'] == "Company Report"  # Updated assertion
    assert len(filtered_rows[0]['matching_rows']) == 1
    assert "1000000" in str(filtered_rows[0]['matching_rows'][0])
