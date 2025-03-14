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
import random


def read_csv_with_encoding(input_csv):
    try:
        # Try reading with UTF-8 encoding
        df_input = pd.read_csv(input_csv, encoding="utf-8")
    except UnicodeDecodeError:
        print(
            f"{get_timestamp()} [WARN] UTF-8 decoding failed. Trying with ISO-8859-1 encoding..."
        )
        try:
            # Try reading with ISO-8859-1 encoding
            df_input = pd.read_csv(input_csv, encoding="ISO-8859-1")
        except UnicodeDecodeError:
            print(
                f"{get_timestamp()} [ERROR] ISO-8859-1 decoding failed. Trying with Windows-1252 encoding..."
            )
            try:
                # Try reading with Windows-1252 encoding
                df_input = pd.read_csv(input_csv, encoding="Windows-1252")
            except UnicodeDecodeError as e:
                print(
                    f"{get_timestamp()} [ERROR] Failed to read CSV file with multiple encodings: {e}"
                )
                sys.exit(1)
    return df_input


def sanitize_filename(name: str) -> str:
    """Sanitizes strings (company names, report names) by replacing spaces, ampersands, German Umlauts, etc."""
    umlaut_map = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
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
        data["End Date"] = found_dates[1]

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
            if (
                data["Technische Anlagen Start"] == "NA"
                and "technische anlagen" in first_cell
            ):
                if debug:
                    print(
                        f"[DEBUG] Found row {r_idx} with 'technische anlagen': {row_cells}"
                    )
                pair = get_two_numeric_cells(row_cells)
                if pair:
                    data["Technische Anlagen Start"], data["Technische Anlagen End"] = (
                        pair
                    )
                    if debug:
                        print(f"[DEBUG] => Tech numeric pair: {pair}")

            # -- Sachanlagen => same row or sum row
            if data["Sachanlagen Start"] == "NA" and re.match(
                r"^(?:ii\.?|2\.)?\s*sachanlagen", first_cell, re.IGNORECASE
            ):
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
                    print(
                        f"[DEBUG] => Checking subsequent rows from {r_idx + 1} for sum row"
                    )
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
                                print(
                                    f"[DEBUG] => Found Sach sum row at {sub_idx}: {pair2}"
                                )
                            return  # Stop further searching

    # 2) Parse tables
    tables = soup.find_all("table")
    if debug:
        print(f"[DEBUG] Found {len(tables)} table(s).")
    for t_idx, table in enumerate(tables):
        if debug:
            print(f"[DEBUG] Checking table #{t_idx}")
        parse_table_for_sach_tech(table)
        if (
            data["Sachanlagen Start"] != "NA"
            and data["Technische Anlagen Start"] != "NA"
        ):
            if debug:
                print("[DEBUG] Found both. Stopping table parse.")
            break

    # 3) Inline fallback with DOTALL (crosses newlines)
    if data["Sachanlagen Start"] == "NA" or data["Technische Anlagen Start"] == "NA":
        if debug:
            print("[DEBUG] Doing final inline fallback approach with DOTALL.")
        inline_sach = re.search(
            r"(?is)\b(?:i\.?|ii\.?:1\.?|2\.)?\s*sachanlagen.*?\s+([\d.,]+)\s+([\d.,]+)",
            full_text,
        )
        if inline_sach and data["Sachanlagen Start"] == "NA":
            data["Sachanlagen Start"] = inline_sach.group(1)
            data["Sachanlagen End"] = inline_sach.group(2)
            if debug:
                print(f"[DEBUG] => Inline fallback => Sach: {inline_sach.groups()}")

        inline_tech = re.search(
            r"(?is)\btechnische anlagen.*?\s+([\d.,]+)\s+([\d.,]+)", full_text
        )
        if inline_tech and data["Technische Anlagen Start"] == "NA":
            data["Technische Anlagen Start"] = inline_tech.group(1)
            data["Technische Anlagen End"] = inline_tech.group(2)
            if debug:
                print(f"[DEBUG] => Inline fallback => Tech: {inline_tech.groups()}")

    return data


def store_files_locally(
    base_dir: str,
    company: str,
    report_name: str,
    raw_html: str,
    txt_report: str,
    date_str: str,
) -> str:
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
    metadata = {
        "name": report_name,
        "date": date_str.isoformat()
        if hasattr(date_str, "isoformat") else str(date_str),
        "company_name": company,
    }
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

    return folder_path


def get_timestamp():
    """Returns the current timestamp in a readable format."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_reports_with_retry(
    company: str,
    max_retries: int = 5,
    max_delay_seconds: int = 300,
    backoff_factor: float = 2.0,
) -> dict:
    """
    Fetches reports with exponential backoff retry mechanism.

    Args:
        company: Company name to search for
        max_retries: Maximum number of retries before giving up (default: 5)
        max_delay_seconds: Maximum delay in seconds between retries (default: 300, or 5 minutes)
        backoff_factor: Exponential factor for backoff calculation (default: 2.0)

    Returns:
        Dictionary with report data if successful, empty dict otherwise
    """
    attempt = 0

    while True:
        attempt += 1
        ba = Bundesanzeiger()

        # Calculate delay based on attempt number with exponential backoff
        if attempt > 1:
            # Calculate base delay with exponential backoff: factor^(attempt-1)
            delay = min(max_delay_seconds, 10 * (backoff_factor ** (attempt - 2)))

            # Add a small random jitter (±15%) to avoid thundering herd problem
            jitter = random.uniform(0.85, 1.15)
            actual_delay = delay * jitter

            print(
                f"{get_timestamp()} [RETRY] Attempt {attempt}/{max_retries} for {company}. "
                f"Waiting {actual_delay:.2f} seconds..."
            )
            time.sleep(actual_delay)

        try:
            data = ba.get_reports(company)
            if not data:
                print(f"{get_timestamp()} [WARN] No or empty data for {company}")
                if attempt >= max_retries:
                    print(
                        f"{get_timestamp()} [ERROR] Maximum retries ({max_retries}) reached for {company}. Giving up."
                    )
                    return {}
                print(
                    f"{get_timestamp()} [INFO] Will retry ({attempt}/{max_retries})..."
                )
                continue

            return data

        except AttributeError as e:
            # Specific case for 'NoneType' errors (move to next company)
            if "'NoneType' object has no attribute" in str(e):
                print(
                    f"{get_timestamp()} [ERROR] '{company}' returned NoneType. Skipping to next company."
                )
                return {}  # Return an empty dict to indicate failure but move on

            # Otherwise, retry if we haven't exceeded max_retries
            print(
                f"{get_timestamp()} [ERROR] AttributeError while fetching data for {company}: {e}"
            )
            if attempt >= max_retries:
                print(
                    f"{get_timestamp()} [ERROR] Maximum retries ({max_retries}) reached for {company}. Giving up."
                )
                return {}
            print(f"{get_timestamp()} [INFO] Will retry ({attempt}/{max_retries})...")

        except Exception as e:
            # Handle any other exceptions
            print(
                f"{get_timestamp()} [ERROR] Exception while fetching data for {company}: {e}"
            )
            if attempt >= max_retries:
                print(
                    f"{get_timestamp()} [ERROR] Maximum retries ({max_retries}) reached for {company}. Giving up."
                )
                return {}
            print(f"{get_timestamp()} [INFO] Will retry ({attempt}/{max_retries})...")


def company_folder_exists(base_dir: str, company: str) -> bool:
    """
    Checks if a folder for the company exists and is not empty.
    Returns True if the folder exists and has content, False otherwise.
    """
    safe_company = sanitize_filename(company)
    company_folder = os.path.join(base_dir, safe_company)

    if not os.path.exists(company_folder):
        return False

    # Check if the folder is not empty
    if os.path.isdir(company_folder):
        # Check if there's at least one directory (indicating a report folder)
        for _, dirs, files in os.walk(company_folder):
            if dirs or files:  # If there are any subdirectories or files
                return True
            break  # Only check the top level

    return False


def find_latest_jahresabschluss_locally(base_dir: str, company: str) -> tuple:
    """
    Searches for the latest Jahresabschluss HTML file in the company's local folder.
    Returns (html_content, folder_path) if found, otherwise (None, None).
    """
    safe_company = sanitize_filename(company)
    company_folder = os.path.join(base_dir, safe_company)

    if not os.path.exists(company_folder) or not os.path.isdir(company_folder):
        return None, None

    # Find all report folders
    report_folders = []
    for item in os.listdir(company_folder):
        item_path = os.path.join(company_folder, item)
        if os.path.isdir(item_path) and "jahresabschluss" in item.lower():
            report_folders.append(item_path)

    if not report_folders:
        return None, None

    # Sort by folder name as a proxy for sorting by date (most recent last)
    report_folders.sort()
    latest_folder = report_folders[-1]

    # Find HTML file in the latest folder
    html_file = None
    for file in os.listdir(latest_folder):
        if file.endswith(".html") and "raw_report" in file:
            html_file = os.path.join(latest_folder, file)
            break

    if not html_file or not os.path.exists(html_file):
        return None, None

    # Read HTML content
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content, latest_folder
    except Exception as e:
        print(f"{get_timestamp()} [ERROR] Failed to read HTML file {html_file}: {e}")
        return None, None


def process_company(
    company: str,
    base_dir: str,
    max_retries: int = 5,
    max_delay_seconds: int = 300,
    backoff_factor: float = 2.0,
    location: str = None,
) -> dict:
    """
    Fetches ALL reports for a given company (retrying if needed),
    sorts them by date (converted to datetime), stores them locally,
    and extracts data from the LATEST Jahresabschluss.
    Prints the names of all found reports, even if none are used.

    If the company folder already exists, will extract data from local files
    without making API calls.

    Args:
        company: Company name to search for
        base_dir: Directory to store data
        max_retries: Maximum number of retries
        max_delay_seconds: Maximum delay between retries
        backoff_factor: Exponential factor for backoff
        location: Optional location of the company
    """
    # Default result for CSV columns
    result_latest = {
        "Technische Anlagen Start": "NA",
        "Technische Anlagen End": "NA",
        "Sachanlagen Start": "NA",
        "Sachanlagen End": "NA",
        "Start Date": "-",
        "End Date": "-",
        "Note": "",
    }

    # Create search term combining company name and location if provided
    search_term = company
    if location and location.strip():
        search_term = (
            f"{company} {location.strip().split()[0]}"  # Use first word of location
        )

    # Check if company folder already exists and is not empty
    if company_folder_exists(base_dir, company):
        print(
            f"{get_timestamp()} [INFO] {company} folder exists - using local data for extraction."
        )
        result_latest["Note"] = "Folder exists | Used local data | "

        # Try to find and extract data from local HTML file
        html_content, folder_path = find_latest_jahresabschluss_locally(
            base_dir, company
        )
        if html_content:
            print(
                f"{get_timestamp()} [INFO] Found local HTML for {company}, extracting data..."
            )
            parsed = extract_financial_data_from_html(html_content, debug=False)
            result_latest["Technische Anlagen Start"] = parsed[
                "Technische Anlagen Start"
            ]
            result_latest["Technische Anlagen End"] = parsed["Technische Anlagen End"]
            result_latest["Sachanlagen Start"] = parsed["Sachanlagen Start"]
            result_latest["Sachanlagen End"] = parsed["Sachanlagen End"]
            result_latest["Start Date"] = parsed["Start Date"]
            result_latest["End Date"] = parsed["End Date"]
            result_latest["Note"] = (
                result_latest["Note"] + folder_path
            )  # local folder path
        else:
            print(
                f"{get_timestamp()} [WARN] No suitable HTML found locally for {company}"
            )
            result_latest["Note"] = "Local data exists but no suitable HTML found"

        return result_latest

    # 1) Get all data with retry and exponential backoff - using search_term (company + location)
    data = get_reports_with_retry(
        search_term, max_retries, max_delay_seconds, backoff_factor
    )
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
        print(
            f"{get_timestamp()} [INFO] No valid (dict) reports to store for {company}."
        )
        return result_latest

    # 3) Sort by actual datetime instead of the raw string
    def sort_key(rep):
        raw_date = rep.get("date", "")  # e.g. "2023-03-30 00:00:00"
        dt_obj = parse_date_str(raw_date)
        return dt_obj

    all_reports.sort(key=sort_key, reverse=True)

    # 4) Identify the LATEST Jahresabschluss (if any)
    jahresabschluss_reports = [
        r for r in all_reports if "Jahresabschluss" in r.get("name", "")
    ]

    if jahresabschluss_reports:
        print(
            f"{get_timestamp()} [INFO] Found the following reports for {company}: {len(jahresabschluss_reports)}"
        )
    else:
        print(
            f"{get_timestamp()} [INFO] No dictionary reports found for {company} (data might be incomplete)."
        )

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

        folder_path = store_files_locally(
            base_dir, company, r_name, raw_html, txt_report, r_date
        )

        # If this is the LATEST Jahresabschluss, parse it for data
        if report == latest_report and raw_html:
            parsed = extract_financial_data_from_html(raw_html, debug=False)
            result_latest["Technische Anlagen Start"] = parsed[
                "Technische Anlagen Start"
            ]
            result_latest["Technische Anlagen End"] = parsed["Technische Anlagen End"]
            result_latest["Sachanlagen Start"] = parsed["Sachanlagen Start"]
            result_latest["Sachanlagen End"] = parsed["Sachanlagen End"]
            result_latest["Start Date"] = parsed["Start Date"]
            result_latest["End Date"] = parsed["End Date"]
            result_latest["Note"] = folder_path  # local folder path

    return result_latest


def main(
    input_csv: str,
    base_dir: str = "bundesanzeiger_local_data",
    max_retries: int = 5,
    max_delay_seconds: int = 300,
    backoff_factor: float = 2.0,
):
    """
    Reads companies from 'companies.csv', processes each, and writes extracted
    data to 'companies_output.csv'.
    CSV: "company name" (mandatory) & "location" (optional)
    This version:
      - reads both company name and location (if available)
      - skips companies that already have a folder with content,
      - sorts by datetime,
      - prints all report names,
      - extracts from HTML rather than .txt,
      - retries on fetch errors with exponential backoff,
      - configurable retry parameters.
      - configurable base directory for storing data
    """
    if not os.path.exists(input_csv):
        print(f"{get_timestamp()} [ERROR] Input file '{input_csv}' not found.")
        sys.exit(1)

    input_name = os.path.splitext(os.path.basename(input_csv))[0]
    output_csv = f"{input_name}_output.csv"
    os.makedirs(base_dir, exist_ok=True)

    # Columns we want in the output
    output_columns = [
        "company name",
        "location",
        "Technische Anlagen Start",
        "Technische Anlagen End",
        "Sachanlagen Start",
        "Sachanlagen End",
        "Start Date",
        "End Date",
        "Note",
    ]

    # 1) If 'companies_output.csv' does NOT exist, create it with just a header row
    if not os.path.exists(output_csv):
        empty_df = pd.DataFrame(columns=output_columns)
        empty_df.to_csv(output_csv, index=False)

    # 2) Read the input CSV with company names, with more robust parsing options
    try:
        df_input = read_csv_with_encoding(input_csv)
    except pd.errors.ParserError as e:
        print(f"{get_timestamp()} [WARN] Error parsing CSV with default settings: {e}")
        print(f"{get_timestamp()} [INFO] Trying with different parsing settings...")
        try:
            # Try reading with quoting and different engine
            df_input = pd.read_csv(
                input_csv,
                quoting=pd.io.common.csv.QUOTE_NONE,
                engine="python",
                error_bad_lines=False,
                warn_bad_lines=True,
            )
        except Exception:
            # If that fails, try reading with a single column
            print(f"{get_timestamp()} [INFO] Trying with single column reading...")
            df_input = pd.read_csv(input_csv, header=0, names=["company name"])

    # Make sure we have the expected column
    if "company name" not in df_input.columns:
        print(
            f"{get_timestamp()} [ERROR] Could not find 'company name' column in the input file"
        )
        sys.exit(1)

    # 3) For each company, process and append a single row to the output file
    for _, row in df_input.iterrows():
        company_name = str(row["company name"]).strip()
        # Get location if it exists in the CSV, otherwise use None
        location = (
            str(row["location"]).strip()
            if "location" in row and not pd.isna(row["location"])
            else None
        )

        location_info = f" (location: {location})" if location else ""
        print(f"\n{get_timestamp()} [INFO] Processing: {company_name}{location_info}")

        # Call your existing function to get extracted data with retry parameters
        extracted = process_company(
            company_name,
            base_dir,
            max_retries,
            max_delay_seconds,
            backoff_factor,
            location,
        )

        # Build a dict for the new row
        new_row = {
            "company name": company_name,
            "location": location if location else "",
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
        new_df.to_csv(output_csv, mode="a", header=False, index=False)

    print(f"{get_timestamp()} [DONE] Results written to: {output_csv}")
    print(f"{get_timestamp()} [INFO] Folders created under: {base_dir}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            f"{get_timestamp()} [ERROR] Usage: python get_bundesanzeiger_html.py <input_csv> [output_dir] [max_retries] [max_delay_seconds] [backoff_factor]"
        )
        print(f"{get_timestamp()} [INFO]  Default base_dir: bundesanzeiger_local_data")
        sys.exit(1)

    input_csv = sys.argv[1]

    # Parse optional base_dir and retry parameters
    base_dir = sys.argv[2] if len(sys.argv) > 2 else "bundesanzeiger_local_data"
    max_retries = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    max_delay_seconds = (
        int(sys.argv[4]) if len(sys.argv) > 4 else 300
    )  # 5 minutes default
    backoff_factor = float(sys.argv[5]) if len(sys.argv) > 5 else 2.0

    print(
        f"{get_timestamp()} [CONFIG] Using base_dir='{base_dir}', max_retries={max_retries}, "
        + f"max_delay={max_delay_seconds}s, backoff_factor={backoff_factor}"
    )
    main(input_csv, base_dir, max_retries, max_delay_seconds, backoff_factor)
