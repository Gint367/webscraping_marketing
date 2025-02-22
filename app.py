import os
import re
import json
import time
import unicodedata
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from deutschland.bundesanzeiger import Bundesanzeiger
import sys

def sanitize_filename(name: str) -> str:
    """Sanitizes strings (company names, report names) by replacing spaces, ampersands, German Umlauts, etc."""
    umlaut_map = {
        "ä": "ae", "ö": "oe", "ü": "ue", 
        "Ä": "Ae", "Ö": "Oe", "Ü": "Ue", 
        "ß": "ss",
    }
    for umlaut, replacement in umlaut_map.items():
        name = name.replace(umlaut, replacement)
    name = unicodedata.normalize("NFKD", name)
    name = name.replace(" ", "_").replace("&", "and")
    name = name.replace("/", "_")
    return name

def parse_date_str(date_str: str) -> datetime.datetime:
    """
    Attempts to parse a date like '2023-03-30 00:00:00' into a Python datetime object.
    If parsing fails, returns a default far-past datetime (to avoid errors).
    """
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.datetime(1970, 1, 1, 0, 0, 0)

import re
from bs4 import BeautifulSoup

def extract_financial_data_from_html(raw_html: str, debug: bool = False) -> dict:
    data = {
        "Company Name": "Unknown",
        "Start Date": "-",
        "End Date": "-",
        "Technische Anlagen Start": "NA",
        "Technische Anlagen End": "NA",
        "Sachanlagen Start": "NA",
        "Sachanlagen End": "NA",
    }
    if not raw_html:
        if debug:
            print("[DEBUG] No HTML provided. Returning defaults.")
        return data

    soup = BeautifulSoup(raw_html, "html.parser")
    full_text = soup.get_text("\n")

    # 1) Extract dates
    date_pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
    found_dates = date_pattern.findall(full_text)
    if debug:
        print(f"[DEBUG] Found potential dates: {found_dates}")
    if len(found_dates) >= 2:
        data["Start Date"] = found_dates[0]
        data["End Date"]   = found_dates[1]

    numeric_regex = re.compile(r"^[\d.,]+\d$")

    def get_two_numeric_cells(cells):
        """
        Return (num1, num2) if exactly two of them match numeric_regex, else None.
        """
        numeric_vals = [c.strip() for c in cells if numeric_regex.match(c.strip())]
        if len(numeric_vals) == 2:
            return numeric_vals[0], numeric_vals[1]
        return None

    def parse_table_for_sach_tech(table):
        rows = table.find_all("tr")
        table_data = []
        for tr in rows:
            cell_texts = [td.get_text(strip=True) for td in tr.find_all("td")]
            table_data.append(cell_texts)

        for r_idx, row_cells in enumerate(table_data):
            if not row_cells:
                continue
            first_cell = row_cells[0].lower()

            # -- Technische Anlagen => same row only
            if data["Technische Anlagen Start"] == "NA" and "technische anlagen" in first_cell:
                if debug:
                    print(f"[DEBUG] Found row {r_idx} with 'technische anlagen': {row_cells}")
                pair = get_two_numeric_cells(row_cells)
                if pair:
                    data["Technische Anlagen Start"], data["Technische Anlagen End"] = pair
                    if debug:
                        print(f"[DEBUG] => Tech numeric pair: {pair}")

            # -- Sachanlagen => same row or sum row
            if data["Sachanlagen Start"] == "NA" and re.match(r"^(?:ii\.?|2\.)?\s*sachanlagen", first_cell, re.IGNORECASE):
                if debug:
                    print(f"[DEBUG] Found row {r_idx} with 'sachanlagen': {row_cells}")
                
                # Try to extract numeric data from the same row
                pair = get_two_numeric_cells(row_cells)
                if pair:
                    # ✅ Found correct row, STOP searching further
                    data["Sachanlagen Start"], data["Sachanlagen End"] = pair
                    if debug:
                        print(f"[DEBUG] => Found Sach numeric pair on SAME row: {pair}")
                    return  # Stop further searching
                
                # Otherwise, continue searching for the sum row
                if debug:
                    print(f"[DEBUG] => Checking subsequent rows from {r_idx+1} for sum row")
                for sub_idx in range(r_idx + 1, len(table_data)):
                    sub_cells = table_data[sub_idx]
                    if not sub_cells:
                        continue
                    # If first cell is empty/blank, then check for 2 numeric columns
                    if not sub_cells[0].strip():
                        pair2 = get_two_numeric_cells(sub_cells)
                        if pair2:
                            # ✅ Found sum row, STOP searching further
                            data["Sachanlagen Start"], data["Sachanlagen End"] = pair2
                            if debug:
                                print(f"[DEBUG] => Found Sach sum row at {sub_idx}: {pair2}")
                            return  # Stop further searching

    # 2) Parse tables
    tables = soup.find_all("table")
    if debug:
        print(f"[DEBUG] Found {len(tables)} table(s).")
    for t_idx, table in enumerate(tables):
        if debug:
            print(f"[DEBUG] Checking table #{t_idx}")
        parse_table_for_sach_tech(table)
        if (data["Sachanlagen Start"] != "NA" and 
            data["Technische Anlagen Start"] != "NA"):
            if debug:
                print("[DEBUG] Found both. Stopping table parse.")
            break

    # 3) Inline fallback with DOTALL (crosses newlines)
    if data["Sachanlagen Start"] == "NA" or data["Technische Anlagen Start"] == "NA":
        if debug:
            print("[DEBUG] Doing final inline fallback approach with DOTALL.")
        inline_sach = re.search(r"(?is)\b(?:i\.?|ii\.?:1\.?|2\.)?\s*sachanlagen.*?\s+([\d.,]+)\s+([\d.,]+)", full_text)
        if inline_sach and data["Sachanlagen Start"] == "NA":
            data["Sachanlagen Start"] = inline_sach.group(1)
            data["Sachanlagen End"]   = inline_sach.group(2)
            if debug:
                print(f"[DEBUG] => Inline fallback => Sach: {inline_sach.groups()}")

        inline_tech = re.search(r"(?is)\btechnische anlagen.*?\s+([\d.,]+)\s+([\d.,]+)", full_text)
        if inline_tech and data["Technische Anlagen Start"] == "NA":
            data["Technische Anlagen Start"] = inline_tech.group(1)
            data["Technische Anlagen End"]   = inline_tech.group(2)
            if debug:
                print(f"[DEBUG] => Inline fallback => Tech: {inline_tech.groups()}")

    return data



def store_files_locally(base_dir: str, company: str, report_name: str, raw_html: str, txt_report: str, date_str: str) -> str:
    """
    Stores the raw HTML + minimal JSON metadata locally.
    (We skip text files in this example, since we directly parse from HTML now.)
    Returns the folder path.
    """
    safe_company = sanitize_filename(company)
    safe_report = sanitize_filename(report_name)

    folder_path = os.path.join(base_dir, safe_company, safe_report)
    os.makedirs(folder_path, exist_ok=True)

    # Write raw HTML
    if raw_html:
        html_file = os.path.join(folder_path, f"{safe_report}_raw_report.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(raw_html)

    # Write report
    if txt_report:
        txt_report_file = os.path.join(folder_path, f"{safe_report}_report.txt")
        with open(txt_report_file, "w", encoding="utf-8") as f:
            f.write(txt_report)

    # Write minimal metadata
    metadata_file = os.path.join(folder_path, f"{safe_report}_metadata.json")
    metadata = {"name": report_name, "date": date_str.isoformat() if hasattr(date_str, "isoformat") else str(date_str)}
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

    return folder_path

def get_reports_with_retry(company: str) -> dict:
    """
    Repeatedly tries to fetch 'data = ba.get_reports(company)' with a 10-second
    delay before each attempt. If an exception or invalid data is returned,
    it retries indefinitely.
    """
    
    while True:
        ba = Bundesanzeiger()
        time.sleep(10)
        try:
            data = ba.get_reports(company)
            if not data:
                print(f"[WARN] No or empty data for {company}, retrying...")
            else:
                return data
        except AttributeError as e:
            # Specific case for 'NoneType' errors (move to next company)
            if "'NoneType' object has no attribute" in str(e):
                print(f"[ERROR] '{company}' returned NoneType. Skipping to next company.")
                return {}  # Return an empty dict to indicate failure but move on

            # Otherwise, retry
            print(f"[ERROR] AttributeError while fetching data for {company}: {e}")
            print("[INFO] Retrying...")

def process_company(company: str, base_dir: str) -> dict:
    """
    Fetches ALL reports for a given company (retrying if needed),
    sorts them by date (converted to datetime), stores them locally,
    and extracts data from the LATEST Jahresabschluss.
    Prints the names of all found reports, even if none are used.
    """
    # Default result for CSV columns
    result_latest = {
        "Technische Anlagen Start": "NA",
        "Technische Anlagen End": "NA",
        "Sachanlagen Start": "NA",
        "Sachanlagen End": "NA",
        "Start Date": "-",
        "End Date": "-",
        "Note": ""
    }

    # 1) Get all data with retry
    data = get_reports_with_retry(company)
    # data is typically a dict with some keys -> each is a report

    # 2) Print names of all found reports
    all_names = []
    for v in data.values():
        if isinstance(v, dict):
            nm = v.get("name", "Unnamed Report")
            all_names.append(nm)

    # Convert everything to a list
    all_reports = [r for r in data.values() if isinstance(r, dict)]
    if not all_reports:
        print(f"[INFO] No valid (dict) reports to store for {company}.")
        return result_latest

    # 3) Sort by actual datetime instead of the raw string
    def sort_key(rep):
        raw_date = rep.get("date", "")  # e.g. "2023-03-30 00:00:00"
        dt_obj = parse_date_str(raw_date)
        return dt_obj

    all_reports.sort(key=sort_key, reverse=True)

    # 4) Identify the LATEST Jahresabschluss (if any)
    jahresabschluss_reports = [r for r in all_reports if "Jahresabschluss" in r.get("name", "")]

    if jahresabschluss_reports:
        print(f"[INFO] Found the following reports for {company}: {len(jahresabschluss_reports)}")
    else:
        print(f"[INFO] No dictionary reports found for {company} (data might be incomplete).")

    latest_report = jahresabschluss_reports[0] if jahresabschluss_reports else None

    report_name_count = {}

    # 5) Store ALL reports locally
    for report in jahresabschluss_reports:
        r_name = report.get("name", "Unknown Report")
        r_date = report.get("date", "")
        raw_html = report.get("raw_report", "")
        txt_report = report.get("report", "")

        # Ensure unique name
        if r_name in report_name_count:
            report_name_count[r_name] += 1
            r_name = f"{r_name}_{report_name_count[r_name]}"  # Append trailing number
        else:
            report_name_count[r_name] = 1
            r_name = r_name  # First occurrence keeps original name

        folder_path = store_files_locally(base_dir, company, r_name, raw_html, txt_report, r_date)

        # If this is the LATEST Jahresabschluss, parse it for data
        if report == latest_report and raw_html:
            parsed = extract_financial_data_from_html(raw_html, debug=False)
            result_latest["Technische Anlagen Start"] = parsed["Technische Anlagen Start"]
            result_latest["Technische Anlagen End"] = parsed["Technische Anlagen End"]
            result_latest["Sachanlagen Start"] = parsed["Sachanlagen Start"]
            result_latest["Sachanlagen End"] = parsed["Sachanlagen End"]
            result_latest["Start Date"] = parsed["Start Date"]
            result_latest["End Date"] = parsed["End Date"]
            result_latest["Note"] = folder_path  # local folder path

    return result_latest


def main(input_csv: str):
    """
    Reads companies from 'companies.csv', processes each, and writes extracted
    data to 'companies_output.csv'.  
    This version:
      - sorts by datetime, 
      - prints all report names,
      - extracts from HTML rather than .txt,
      - retries on fetch errors,
      - waits 10s before each fetch attempt.
    """
    if not os.path.exists(input_csv):
        print(f"[ERROR] Input file '{input_csv}' not found.")
        sys.exit(1)

    input_name = os.path.splitext(os.path.basename(input_csv))[0]
    output_csv = f"{input_name}_output.csv"
    base_dir = "bundesanzeiger_local_data"
    os.makedirs(base_dir, exist_ok=True)

    # Columns we want in the output
    output_columns = [
        "company name",
        "Technische Anlagen Start",
        "Technische Anlagen End",
        "Sachanlagen Start",
        "Sachanlagen End",
        "Start Date",
        "End Date",
        "Note"
    ]

    # 1) If 'companies_output.csv' does NOT exist, create it with just a header row
    if not os.path.exists(output_csv):
        empty_df = pd.DataFrame(columns=output_columns)
        empty_df.to_csv(output_csv, index=False)

    # 2) Read the input CSV with company names
    df_input = pd.read_csv(input_csv)

    # 3) For each company, process and append a single row to the output file
    for _, row in df_input.iterrows():
        company_name = str(row["company name"]).strip()
        print(f"\n[INFO] Processing: {company_name}")

        # Call your existing function to get extracted data
        extracted = process_company(company_name, base_dir)  # must be defined elsewhere

        # Build a dict for the new row
        # (Note: "company name" column wasn't in your original 'needed_cols', but we add it for clarity.)
        new_row = {
            "company name": company_name,
            "Technische Anlagen Start": extracted["Technische Anlagen Start"],
            "Technische Anlagen End": extracted["Technische Anlagen End"],
            "Sachanlagen Start": extracted["Sachanlagen Start"],
            "Sachanlagen End": extracted["Sachanlagen End"],
            "Start Date": extracted["Start Date"],
            "End Date": extracted["End Date"],
            "Note": extracted["Note"],
        }

        # Convert to a DataFrame (just 1 row) for easy CSV append
        new_df = pd.DataFrame([new_row])

        # 4) Append to the output CSV in 'append' mode, no header, no index
        new_df.to_csv(output_csv, mode='a', header=False, index=False)

    print(f"[DONE] Results written to: {output_csv}")
    print(f"Folders created under: {base_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python app.py <input_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    main(input_csv)
