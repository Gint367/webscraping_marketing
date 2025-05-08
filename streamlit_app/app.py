import io
import json
import logging
import multiprocessing
import os
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import streamlit as st
from regex import F

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# Import from master_pipeline.py
from master_pipeline import (  # noqa: E402
    run_pipeline,
)

# FOR LLM: DO NOT CHANGE PRINTS TO LOGGING
# --- Page Configuration (Must be the first Streamlit command) ---
st.set_page_config(
    layout="wide", page_title="Company Enrichment Tool"
)  # Added page config

# --- Constants for Column Validation ---
REQUIRED_COLUMNS_MAP = {
    "company name": ["company name", "firma1"],
    "location": ["location", "ort"],
    "url": ["url"],
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)


# --- Helper Functions ---
def validate_columns(
    df_columns: list[str],
) -> tuple[dict[str, tuple[bool, str | None]], bool]:
    """
    Checks if required columns (or their aliases) exist in the DataFrame columns.

    Args:
        df_columns: A list of column names from the DataFrame.

    Returns:
        A tuple containing:
        - A dictionary where keys are the canonical required column names
          and values are tuples: (found_status: bool, actual_name_found: str | None).
        - A boolean indicating if all required columns were found.
    """
    validation_results = {}
    all_found = True
    normalized_df_columns = {
        col.lower().strip(): col for col in df_columns
    }  # Store original casing

    for canonical_name, aliases in REQUIRED_COLUMNS_MAP.items():
        found = False
        actual_name = None
        for alias in aliases:
            if alias.lower() in normalized_df_columns:
                found = True
                actual_name = normalized_df_columns[
                    alias.lower()
                ]  # Get original casing
                break
        validation_results[canonical_name] = (found, actual_name)
        if not found:
            all_found = False
    return validation_results, all_found


# --- Session State Initialization ---
def init_session_state():
    """
    Initializes session state variables if they don't exist.
    Also handles specific state adjustments based on input method.

    Returns:
        bool: True if this was the first full initialization, False otherwise.
    """
    # Sentinel key to check if this is the first time defaults are being applied in this session.
    is_first_full_init = "_app_defaults_initialized" not in st.session_state

    defaults = {
        "page": "Input",
        "company_list": None,  # Will store list of dicts for processing
        "uploaded_file_data": None,  # Stores the uploaded file object
        "manual_input_df": pd.DataFrame(
            columns=["company name", "location", "url"]
        ),  # For data editor
        "input_method": "File Upload",  # Default input method
        "config": {},
        "job_status": "Idle",
        "progress": 0,
        "current_phase": "",
        "results": None,
        "log_messages": [],
        "log_queue": None,
        "status_queue": None,
        "pipeline_process": None,
        "pipeline_config": None,
        "error_message": None,
        "artifacts": None,
        "testing_mode": False,  # Flag to disable st.rerun() calls during tests
        # Auto-refresh configuration
        "auto_refresh_enabled": True,  # Auto-refresh logs by default
        "refresh_interval": 3.0,  # Default refresh interval in seconds
    }

    # Apply defaults if keys don't exist
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # Set the sentinel AFTER initializing the main values
    if is_first_full_init:
        st.session_state["_app_defaults_initialized"] = True
        print("Default session state values initialized for the new user session.")

    # Specific state adjustments that might need to occur on reruns
    if st.session_state.get("input_method") == "Manual Input":
        if not isinstance(st.session_state.get("manual_input_df"), pd.DataFrame):
            st.session_state["manual_input_df"] = pd.DataFrame(
                columns=["company name", "location", "url"]
            )
            logging.info(
                "Re-initialized 'manual_input_df' as it was not a DataFrame in Manual Input mode."
            )

    # Safeguard: Ensure 'log_messages' is always a list
    if not isinstance(st.session_state.get("log_messages"), list):
        st.session_state["log_messages"] = []
        logging.warning("Re-initialized 'log_messages' as it was not a list.")

    return is_first_full_init


init_session_state()


# --- Logging Handler for Streamlit ---
class StreamlitLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)

            # Ensure log_messages exists and is a list in session_state
            if not isinstance(st.session_state.get("log_messages"), list):
                st.session_state["log_messages"] = []

            # Use key access for log_messages
            st.session_state["log_messages"].append(msg)
            # Keep only the last N messages if needed
            # max_log_entries = 100
            # st.session_state['log_messages'] = st.session_state['log_messages'][-max_log_entries:]
        except (KeyError, AttributeError, Exception):
            # Pass the record to handleError as expected by the logging framework
            self.handleError(record)


# Add the handler to the root logger AFTER initial state setup
streamlit_handler = StreamlitLogHandler()
logging.getLogger().addHandler(streamlit_handler)


# --- Pipeline Processing in Separate Process ---
def run_pipeline_in_process(
    config: Dict[str, Any], log_queue: Queue, status_queue: Queue
):
    """
    Run the pipeline in a separate process.

    Args:
        config: Configuration for the pipeline
        log_queue: Queue for passing log messages back to the main process
        status_queue: Queue for sending status updates to the main process
    """
    # Configure logging to capture pipeline logs and send to queue and file
    root_logger = logging.getLogger()

    # Create log directory if it doesn't exist
    log_dir = os.path.join(project_root, "logfiles")
    os.makedirs(log_dir, exist_ok=True)

    # Generate log filename with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"pipeline_{timestamp}.log")

    # Add a handler for the log queue
    class QueueHandler(logging.Handler):
        def emit(self, record):
            log_queue.put(record)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set up file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    root_logger.addHandler(file_handler)

    # Set up queue handler
    queue_handler = QueueHandler()
    queue_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(logging.INFO)

    # Log the file location so it's available in the queue
    logging.info(f"Pipeline logs are being saved to: {log_file_path}")

    # Send initial status
    status_queue.put({"status": "Running", "progress": 0, "phase": "Initializing"})

    try:
        print(f"Pipeline process started with config: {config}")
        logging.info("Pipeline process started")
        status_queue.put(
            {"status": "Running", "progress": 10, "phase": "Starting Pipeline"}
        )

        # Execute the pipeline
        final_output = run_pipeline(config)

        # Send success status with final output path
        status_queue.put(
            {
                "status": "Completed",
                "progress": 100,
                "phase": "Finished",
                "output_path": final_output,
            }
        )
        print(f"Pipeline process completed successfully, output at: {final_output}")
        logging.info(f"Pipeline completed successfully, output at: {final_output}")

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Pipeline process failed: {error_msg}")
        print(f"Pipeline process failed: {error_msg}")
        status_queue.put(
            {"status": "Error", "progress": 0, "phase": "Failed", "error": error_msg}
        )


# Function to monitor queues and update session state
def process_queue_messages():
    """
    Process messages from the log and status queues, updating the Streamlit session state.
    This should be called on each Streamlit rerun.
    """
    # Process log messages
    logs_updated = False
    if "log_queue" in st.session_state and st.session_state["log_queue"] is not None:
        try:
            while not st.session_state["log_queue"].empty():
                record = st.session_state["log_queue"].get_nowait()
                if record:
                    log_message = f"{record.asctime if hasattr(record, 'asctime') else ''} - {record.levelname if hasattr(record, 'levelname') else ''} - {record.getMessage() if hasattr(record, 'getMessage') else str(record)}"
                    st.session_state["log_messages"].append(log_message)
                    logs_updated = True
        except Exception as e:
            log_error_msg = f"Error processing log queue: {e}"
            st.session_state["log_messages"].append(log_error_msg)
            logs_updated = True

    # Set flag in session state to indicate new logs available
    if logs_updated:
        st.session_state["logs_updated"] = True

    # Process status updates
    if (
        "status_queue" in st.session_state
        and st.session_state["status_queue"] is not None
    ):
        try:
            while not st.session_state["status_queue"].empty():
                status_update = st.session_state["status_queue"].get_nowait()
                if status_update:
                    # Update job status
                    if "status" in status_update:
                        st.session_state["job_status"] = status_update["status"]

                    # Update progress
                    if "progress" in status_update:
                        st.session_state["progress"] = status_update["progress"]

                    # Update phase
                    if "phase" in status_update:
                        st.session_state["current_phase"] = status_update["phase"]

                    # Handle completion
                    if (
                        status_update.get("status") == "Completed"
                        and "output_path" in status_update
                    ):
                        output_path = status_update["output_path"]
                        try:
                            # Load the results
                            results_df = pd.read_csv(output_path)
                            st.session_state["results"] = results_df
                        except Exception as e:
                            st.session_state["log_messages"].append(
                                f"Error loading results: {e}"
                            )

                    # Handle error
                    if (
                        status_update.get("status") == "Error"
                        and "error" in status_update
                    ):
                        st.session_state["error_message"] = status_update["error"]
        except Exception as e:
            st.session_state["log_messages"].append(
                f"Error processing status queue: {e}"
            )


# --- UI Sections ---
def display_input_section():
    """Displays the UI for data input using radio buttons and data editor."""
    st.header("1. Input Data")
    st.write("Choose your input method:")

    # Radio button to select input method
    input_method = st.radio(
        "Select Input Method:",
        ("File Upload", "Manual Input"),
        key="input_method_choice",
        horizontal=True,
        label_visibility="collapsed",
    )

    # Check if we need to update the input method
    if (
        "input_method" not in st.session_state
        or st.session_state["input_method"] != input_method
    ):
        # For real app, but not for tests
        if hasattr(st.session_state, "running_test") and st.session_state.running_test:
            # In test mode, just update state without rerun
            st.session_state["input_method"] = input_method
        else:
            # Normal app flow
            old_method = st.session_state.get("input_method")
            clear_other_input(input_method)
            st.session_state["input_method"] = input_method

            # Only rerun if input method actually changed (not on first load)
            # Skip rerun if in testing mode
            if (
                old_method
                and old_method != input_method
                and not st.session_state.get("testing_mode", False)
            ):
                st.rerun()

    # Flag to track validation status for uploaded file
    validation_passed_all = True  # Assume true unless file upload fails validation

    if input_method == "File Upload":
        st.subheader("Upload Company List")
        with st.expander("Required Input Columns & Example"):
            st.markdown(
                """
                The input data (either from a file or manual entry) must contain the following columns:
                - **company name**: The name of the company.
                - **location**: The location of the company (e.g., city, country).
                - **url**: The company's website URL.

                **Example:**
                | company name | location   | url                 |
                |--------------|------------|---------------------|
                | Acme Corp    | New York   | http://www.acme.com |
                | Beta Ltd     | London     | http://www.beta.uk  |

                Column names are case-insensitive and leading/trailing spaces will be removed.
                Aliases are also accepted for some columns (e.g., "firma1" for "company name", "ort" for "location").
                """
            )
        current_file_in_state = st.session_state.get("uploaded_file_data")

        if current_file_in_state is None:
            # --- Show File Uploader ---
            uploaded_file = st.file_uploader(
                "Upload a CSV or Excel file",  # Standard label
                type=["csv", "xlsx"],
                key="file_uploader_widget",  # Use a distinct key for the widget
                accept_multiple_files=False,
            )

            if uploaded_file is not None:
                # File has just been uploaded
                st.session_state["uploaded_file_data"] = uploaded_file
                # Clear other input method's state
                st.session_state["manual_input_df"] = pd.DataFrame(
                    columns=["company name", "location", "url"]
                )
                st.session_state["company_list"] = (
                    None  # Clear processed list as input changed
                )
                print(f"File selected: {uploaded_file.name}")
                st.success(
                    f"File '{uploaded_file.name}' selected."
                )  # Use standard quotes

                # Only rerun if not in testing mode
                if not st.session_state.get("testing_mode", False):
                    st.rerun()  # Rerun immediately to show the 'Change File' button state

        else:
            # --- Show File Info ---
            st.success(f"Selected file: **{current_file_in_state.name}**")

            # --- Display Preview and Validation ---
            st.write("Preview & Column Validation:")
            preview_df = None
            header_columns = []  # Store header columns found
            # validation_passed_all = True # Reset here for this specific file check

            try:
                # Read header and first 5 rows together
                current_file_in_state.seek(0)  # Ensure we start from the beginning

                if current_file_in_state.name.endswith(".csv"):
                    bytesio = io.BytesIO(current_file_in_state.getvalue())
                    try:
                        # Try auto-detect separator, let pandas handle encoding from bytes
                        temp_df = pd.read_csv(
                            bytesio, nrows=6, sep=None, engine="python"
                        )
                        # Check if columns were read correctly, sometimes sep=None gives one wrong col
                        if len(temp_df.columns) <= 1 and "," in temp_df.columns[0]:
                            logging.warning(
                                "Auto-detected separator might be wrong, trying comma explicitly."
                            )
                            bytesio.seek(0)
                            temp_df = pd.read_csv(bytesio, nrows=6, sep=",")
                        elif len(temp_df.columns) <= 1 and ";" in temp_df.columns[0]:
                            logging.warning(
                                "Auto-detected separator might be wrong, trying semicolon explicitly."
                            )
                            bytesio.seek(0)
                            temp_df = pd.read_csv(bytesio, nrows=6, sep=";")

                    except Exception as e_read:
                        logging.warning(
                            f"CSV read failed with auto/comma separator: {e_read}. Trying semicolon."
                        )
                        # Reset and try semicolon if default failed
                        bytesio.seek(0)
                        try:
                            temp_df = pd.read_csv(bytesio, nrows=6, sep=";")
                        except Exception as e_read_semi:
                            logging.error(
                                f"CSV read failed with common separators: {e_read_semi}",
                                exc_info=True,
                            )
                            raise ValueError(
                                "Could not parse CSV file. Check format, encoding, and separator."
                            ) from e_read_semi

                    if not temp_df.empty:
                        header_columns = temp_df.columns.tolist()
                        preview_df = temp_df.head(
                            5
                        )  # Take the first 5 rows for preview
                    else:
                        # Check if file has content but pandas couldn't parse columns/rows
                        bytesio.seek(0)
                        file_content_sample = bytesio.read(200).decode(errors="ignore")
                        if file_content_sample.strip():
                            logging.warning(
                                f"Pandas read CSV resulted in empty DataFrame, but file has content. Sample: {file_content_sample[:100]}..."
                            )
                            st.warning(
                                "Could not parse rows/columns correctly. Please check CSV format (separator, quotes, encoding)."
                            )
                        else:
                            logging.warning(
                                "Pandas read CSV resulted in empty DataFrame, file appears empty."
                            )
                            st.warning("File appears to be empty.")
                        validation_passed_all = False

                elif current_file_in_state.name.endswith((".xls", ".xlsx")):
                    bytesio = io.BytesIO(current_file_in_state.getvalue())
                    # Read header and first 5 rows
                    temp_df = pd.read_excel(
                        bytesio, nrows=6
                    )  # Reads header + 5 data rows
                    if not temp_df.empty:
                        header_columns = temp_df.columns.tolist()
                        preview_df = temp_df.head(5)
                    else:
                        logging.warning(
                            "Pandas read Excel resulted in empty DataFrame."
                        )
                        st.warning("File appears empty or the first sheet has no data.")
                        validation_passed_all = False

                else:
                    st.warning("Cannot preview this file type.")
                    validation_passed_all = False  # Cannot validate if cannot preview

                # --- Perform Validation ---
                if header_columns:  # Check if we actually got columns
                    column_validation, validation_passed_all_cols = validate_columns(
                        header_columns
                    )
                    # st.markdown("---") # Separator
                    # st.write("**Required Column Status:**")
                    for canonical_name, (
                        found,
                        actual_name,
                    ) in column_validation.items():
                        aliases_str = "/".join(REQUIRED_COLUMNS_MAP[canonical_name])
                        if found:
                            st.success(
                                f"✔️ Found: **{canonical_name}** (as '{actual_name}')"
                            )
                        else:
                            st.error(
                                f"❌ Missing: **{canonical_name}** (expected one of: {aliases_str})"
                            )
                    # st.markdown("---") # Separator
                    # Update overall validation status based on columns
                    if not validation_passed_all_cols:
                        validation_passed_all = False
                elif (
                    validation_passed_all
                ):  # Only show error if no other warning/error was raised during read
                    # No columns found during read attempt
                    st.error(
                        "Could not detect columns in the file. Please ensure the file is correctly formatted."
                    )
                    validation_passed_all = False

                # --- Display Preview DataFrame ---
                if (
                    preview_df is not None and not preview_df.empty
                ):  # Check if preview has rows
                    st.dataframe(preview_df, use_container_width=True)
                elif header_columns:  # Header found, but no data rows in the first 5
                    st.info(
                        "File has columns, but no data rows found in the preview (first 5 rows)."
                    )
                # If header_columns is empty, the error message above was already shown

                # Reset pointer for the actual processing function later
                current_file_in_state.seek(0)

            except Exception as e:
                st.error(f"Could not read or preview the file: {e}")
                logging.error(
                    f"Error previewing file {current_file_in_state.name}: {e}",
                    exc_info=True,
                )  # Add traceback
                validation_passed_all = False  # Error means validation fails

            # --- Change File Button ---
            if st.button("Change File"):
                st.session_state["uploaded_file_data"] = None
                st.session_state["company_list"] = None  # Clear processed list
                print("User clicked 'Change File'. Clearing uploaded file.")

                # Only rerun if not in testing mode
                if not st.session_state.get("testing_mode", False):
                    st.rerun()

    elif input_method == "Manual Input":
        st.subheader("Enter Data Manually")
        st.write("Add or edit company details below:")

        # Initialize DataFrame in session state if it doesn't exist or is None
        if (
            "manual_input_df" not in st.session_state
            or st.session_state["manual_input_df"] is None
        ):
            st.session_state["manual_input_df"] = pd.DataFrame(
                columns=["company name", "location", "url"]
            )

        # Use st.data_editor for manual input
        edited_df = st.data_editor(
            st.session_state["manual_input_df"],
            num_rows="dynamic",
            key="manual_data_editor",
            column_config={  # Optional: Add specific configurations if needed
                "company name": st.column_config.TextColumn(
                    "Company Name", required=True
                ),
                "location": st.column_config.TextColumn("Location", required=True),
                "url": st.column_config.LinkColumn(
                    "URL", required=True, validate="^https?://"
                ),
            },
            hide_index=True,
            use_container_width=True,
        )

        # Update session state with the edited data
        st.session_state["manual_input_df"] = edited_df

        # Clear uploaded file data when manual input is used
        st.session_state["uploaded_file_data"] = None
        st.session_state["company_list"] = (
            None  # Clear processed list until "Start" is clicked
        )

        if not edited_df.empty:
            logging.info(f"Manual input data updated. Rows: {len(edited_df)}")
            # Convert DataFrame to list of dicts for potential downstream use
            # This conversion can happen here or just before processing
            # st.session_state['company_list'] = edited_df.to_dict('records')

    # --- Processing Trigger ---
    st.divider()
    # Disable button if validation failed for uploaded file OR if no file/manual data
    processing_disabled = False
    if st.session_state["input_method"] == "File Upload":
        if st.session_state.get("uploaded_file_data") is None:
            processing_disabled = True  # No file uploaded
        elif not validation_passed_all:
            processing_disabled = True  # File uploaded but failed validation
            st.warning(
                "Cannot start processing. Please upload a file with all required columns (or fix the current one)."
            )
    elif st.session_state["input_method"] == "Manual Input":
        manual_df = st.session_state.get("manual_input_df")
        if manual_df is None or manual_df.empty:
            processing_disabled = True  # No manual data entered

    if st.button("Start Processing", type="primary", disabled=processing_disabled):
        # No need for the inner check if button is correctly disabled
        process_data()


def clear_other_input(selected_method):
    """Clears the session state of the non-selected input method."""
    if selected_method == "File Upload":
        st.session_state["manual_input_df"] = pd.DataFrame(
            columns=["company name", "location", "url"]
        )
        st.session_state["company_list"] = None
        print("Switched to File Upload, cleared manual input state.")
    elif selected_method == "Manual Input":
        st.session_state["uploaded_file_data"] = None
        # Reset file uploader widget state if possible (Streamlit might handle this)
        # st.session_state['file_uploader'] = None # May cause issues, test carefully
        st.session_state["company_list"] = None
        print("Switched to Manual Input, cleared file upload state.")


def process_data():
    """Processes the data from the selected input method."""
    st.toast("process_data called")
    st.session_state["job_status"] = "Processing"
    st.session_state["results"] = None  # Clear previous results

    # Don't reset logs completely, just add a separator and new start message
    # This ensures log history is preserved across reruns during a session
    st.session_state["log_messages"].append("-" * 40)  # Separator
    st.session_state["log_messages"].append("Processing started...")
    print("Processing started.")

    data_to_process = None

    if (
        st.session_state["input_method"] == "File Upload"
        and st.session_state["uploaded_file_data"]
    ):
        uploaded_file = st.session_state["uploaded_file_data"]
        logging.info(f"Processing uploaded file: {uploaded_file.name}")
        try:
            if uploaded_file.name.endswith(".csv"):
                # Use StringIO to treat the byte stream as a text file
                stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                df = pd.read_csv(stringio)
            elif uploaded_file.name.endswith((".xls", ".xlsx")):
                # Read excel directly from bytes
                df = pd.read_excel(uploaded_file)
            else:
                st.error("Unsupported file type.")
                st.session_state["job_status"] = "Error"
                logging.error("Unsupported file type uploaded.")
                return

            # --- Data Validation and Formatting ---
            # Ensure required columns exist (case-insensitive check)
            required_cols = ["company name", "location", "url"]
            df.columns = df.columns.str.lower().str.strip()  # Normalize column names
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.error(
                    f"Uploaded file is missing required columns: {', '.join(missing_cols)}"
                )
                st.session_state["job_status"] = "Error"
                logging.error(f"Uploaded file missing columns: {missing_cols}")
                return

            # Select and rename columns to ensure consistency
            df = df[required_cols]
            df.rename(
                columns={  # Ensure exact column names if needed downstream
                    "company name": "company name",
                    "location": "location",
                    "url": "url",
                },
                inplace=True,
            )

            # Basic cleaning (optional, adapt as needed)
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df.dropna(
                subset=required_cols, inplace=True
            )  # Drop rows with missing required values

            if df.empty:
                st.warning("No valid data found in the uploaded file after cleaning.")
                st.session_state["job_status"] = "Completed (No Data)"
                logging.warning("No valid data in uploaded file.")
                return

            data_to_process = df.to_dict("records")
            st.session_state["company_list"] = data_to_process  # Store the final list
            logging.info(
                f"Successfully parsed {len(data_to_process)} records from file."
            )

        except Exception as e:
            st.error(f"Error reading or processing file: {e}")
            st.session_state["job_status"] = "Error"
            logging.error(f"Error processing file {uploaded_file.name}: {e}")
            return

    elif st.session_state["input_method"] == "Manual Input":
        manual_df = st.session_state.get("manual_input_df")
        if manual_df is not None and not manual_df.empty:
            # Basic cleaning (optional, adapt as needed)
            manual_df = manual_df.map(lambda x: x.strip() if isinstance(x, str) else x)
            # Validate required columns (data editor should enforce this, but double-check)
            required_cols = ["company name", "location", "url"]
            manual_df.dropna(
                subset=required_cols, inplace=True
            )  # Drop rows missing required values

            if manual_df.empty:
                st.warning("No valid data entered manually after cleaning.")
                st.session_state["job_status"] = "Completed (No Data)"
                logging.warning("No valid data in manual input.")
                return

            data_to_process = manual_df.to_dict("records")
            st.session_state["company_list"] = data_to_process  # Store the final list
            logging.info(f"Processing {len(data_to_process)} manually entered records.")
        else:
            st.warning("No manual data entered.")
            st.session_state["job_status"] = "Idle"  # Or "Completed (No Data)"
            logging.warning("Start Processing clicked with no manual data.")
            return
    else:
        st.warning("No data provided. Please upload a file or enter data manually.")
        st.session_state["job_status"] = "Idle"
        logging.warning(
            "Start Processing clicked with no data source selected or data provided."
        )
        return

    # --- Prepare Pipeline Configuration ---
    if data_to_process:
        st.info(f"Starting enrichment for {len(data_to_process)} companies...")
        logging.info(f"Data prepared for pipeline: {len(data_to_process)} records.")

        try:
            # Create a temporary CSV file for the pipeline
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                temp_csv_path = temp_file.name
                df = pd.DataFrame(data_to_process)
                df.to_csv(temp_csv_path, index=False)
                logging.info(f"Temporary input CSV created at {temp_csv_path}")

            # Create output directory if it doesn't exist
            output_dir = os.path.join(project_root, "outputs")
            os.makedirs(output_dir, exist_ok=True)

            # Prepare the configuration
            pipeline_config = {
                "input_csv": temp_csv_path,
                "output_dir": output_dir,
                "category": st.session_state["config"].get("category"),
                "log_level": "INFO",  # This can be configured via UI if needed
                "skip_llm_validation": True,  # Adjust as needed or make configurable via UI
            }

            # Set up the queues for communication between processes
            manager = Manager()
            log_queue = manager.Queue()
            status_queue = manager.Queue()

            # Store the queues in session state for monitoring
            st.session_state["log_queue"] = log_queue
            st.session_state["status_queue"] = status_queue
            st.session_state["pipeline_config"] = pipeline_config

            # Start the pipeline in a separate process
            p = Process(
                target=run_pipeline_in_process,
                args=(pipeline_config, log_queue, status_queue),
            )
            p.daemon = True  # Set as daemon so it terminates when the main process ends
            p.start()

            # Store the process in session state
            st.session_state["pipeline_process"] = p

            # Update job status
            st.session_state["job_status"] = "Running"
            st.session_state["progress"] = 10
            st.session_state["current_phase"] = "Starting Pipeline"

            print(f"Pipeline process started with PID: {p.pid}")
            logging.info(f"Pipeline process started with PID: {p.pid}")

        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")
            st.session_state["job_status"] = "Error"
            logging.error(f"Failed to start pipeline: {e}", exc_info=True)
            # Clean up any temporary files
            try:
                if "temp_csv_path" in locals():
                    os.unlink(temp_csv_path)
            except Exception:
                pass

    else:
        # This case should ideally be caught earlier, but as a fallback:
        st.warning("No data available to process.")
        st.session_state["job_status"] = "Idle"
        logging.warning("process_data called but data_to_process was empty.")


def display_config_section():
    """Displays the UI for configuration settings."""
    st.header("2. Configuration")
    st.write("Configure scraping and enrichment parameters.")

    # Store current config values before UI interaction
    prev_depth = st.session_state["config"].get("depth", 2)
    prev_llm = st.session_state["config"].get("llm_provider", "OpenAI")
    prev_api_key = st.session_state["config"].get("api_key", "")

    # UI elements for configuration
    st.session_state["config"]["depth"] = st.slider("Crawling Depth", 1, 5, prev_depth)
    st.session_state["config"]["llm_provider"] = st.selectbox(
        "LLM Provider",
        ["OpenAI", "Anthropic", "Gemini", "Mock"],
        index=["OpenAI", "Anthropic", "Gemini", "Mock"].index(prev_llm)
        if prev_llm in ["OpenAI", "Anthropic", "Gemini", "Mock"]
        else 0,
    )
    st.session_state["config"]["api_key"] = st.text_input(
        "API Key", value=prev_api_key, type="password"
    )

    # Only log if configuration values have actually changed
    if (
        prev_depth != st.session_state["config"].get("depth")
        or prev_llm != st.session_state["config"].get("llm_provider")
        or prev_api_key != st.session_state["config"].get("api_key")
    ):
        logging.info(
            f"Configuration updated: Depth={st.session_state['config'].get('depth')}, LLM={st.session_state['config'].get('llm_provider')}"
        )


def display_monitoring_section():
    """Displays the job monitoring and log output."""
    st.header("3. Monitoring")
    st.write("Track the progress of the scraping and enrichment process.")

    # Initial queue processing is still needed outside the fragment
    process_queue_messages()

    # Create a fragment for status information that auto-refreshes
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def display_status_info():
        # Process messages from queues inside fragment to ensure fresh data
        process_queue_messages()

        # Check if the pipeline process is still running
        if (
            "pipeline_process" in st.session_state
            and st.session_state["pipeline_process"]
        ):
            p = st.session_state["pipeline_process"]
            if p.is_alive():
                if st.session_state["job_status"] != "Running":
                    st.session_state["job_status"] = "Running"
            else:
                # Process completed or terminated
                if st.session_state["job_status"] == "Running":
                    # Process ended but status wasn't updated properly
                    # This could happen if the process crashed unexpectedly
                    st.session_state["job_status"] = "Completed"
                    logging.info("Pipeline process ended")

        # Display current status and phase
        status_color = {
            "Idle": "blue",
            "Running": "orange",
            "Completed": "green",
            "Error": "red",
        }.get(st.session_state["job_status"], "blue")

        st.markdown(
            f"**Status:** <span style='color:{status_color}'>{st.session_state['job_status']}</span>",
            unsafe_allow_html=True,
        )

        if "current_phase" in st.session_state and st.session_state["current_phase"]:
            st.markdown(f"**Current Phase:** {st.session_state['current_phase']}")

        # Display progress bar
        if (
            st.session_state["job_status"] == "Running"
            and "progress" in st.session_state
        ):
            st.progress(st.session_state["progress"] / 100)

        # Display error message if present
        if "error_message" in st.session_state and st.session_state["error_message"]:
            st.error(f"Error: {st.session_state['error_message']}")

    # Call the status fragment
    display_status_info()

    # Move Cancel button into the status fragment to ensure it refreshes with status
    # The button should only appear when the job is actually running
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def display_cancel_button():
        # Only show cancel button if job is running
        if (
            st.session_state["job_status"] == "Running"
            and "pipeline_process" in st.session_state
        ):
            if st.button("Cancel Processing"):
                try:
                    p = st.session_state["pipeline_process"]
                    if p and p.is_alive():
                        p.terminate()
                        logging.info("Pipeline process terminated by user")
                    st.session_state["job_status"] = "Cancelled"
                    st.warning("Processing cancelled by user")
                except Exception as e:
                    logging.error(f"Error cancelling process: {e}")
                    st.error(f"Error cancelling process: {e}")

    # Call the cancel button fragment
    display_cancel_button()

    # Display log directory location
    log_dir = os.path.join(project_root, "logfiles")
    if os.path.exists(log_dir):
        st.info(f"Log files are being saved to: {log_dir}")

        # Get the latest log file in the directory (if available)
        try:
            log_files = sorted(
                [f for f in os.listdir(log_dir) if f.startswith("pipeline_")],
                key=lambda x: os.path.getmtime(os.path.join(log_dir, x)),
                reverse=True,
            )
            if log_files:
                latest_log = log_files[0]
                st.success(f"Latest log file: {latest_log}")

        except Exception as e:
            st.warning(f"Could not list log files: {e}")

    # Display logs with auto-refresh capability
    st.subheader("Live Logs")

    # Add auto-refresh controls in an expander
    with st.expander("Log Auto-Refresh Settings"):
        col1, col2 = st.columns([1, 3])
        with col1:
            auto_refresh = st.toggle(
                "Auto-refresh enabled",
                value=st.session_state.get("auto_refresh_enabled", True),
                key="auto_refresh_toggle",
            )
            # Update session state when toggle changes
            st.session_state["auto_refresh_enabled"] = auto_refresh

        with col2:
            refresh_rate = st.slider(
                "Refresh interval (seconds)",
                min_value=1.0,
                max_value=10.0,
                value=st.session_state.get("refresh_interval", 3.0),
                step=0.5,
                key="refresh_slider",
            )
            # Update session state when slider changes
            st.session_state["refresh_interval"] = refresh_rate

            # Show refresh status - ensure refresh_rate is a number for formatting
            try:
                refresh_rate_display = float(refresh_rate)
                st.caption(
                    f"Logs will refresh every {refresh_rate_display:.1f} seconds"
                )
            except (TypeError, ValueError):
                # Fallback for tests where refresh_rate might be a mock
                st.caption("Logs will refresh automatically")

    # Create a fragment with auto-refresh using the built-in run_every parameter
    # This eliminates the need for manual tracking of refresh time
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def display_live_logs():
        # Process messages from queues
        process_queue_messages()

        # Display logs in reverse order (newest first)
        # Wrap logs in a container with a fixed height and scrollbar
        log_container = st.container(height=300)  # Adjust height as needed
        with log_container:
            log_messages_to_display = st.session_state.get("log_messages", [])
            for msg in reversed(
                log_messages_to_display
            ):  # Display newest first at the top of the scroll
                st.text(msg)

    # Call the fragment function
    display_live_logs()


def display_output_section():
    """Displays the results and download options."""
    st.header("4. Output")

    # Process any pending messages to ensure we have the latest results
    process_queue_messages()

    # Final results tab
    st.write("View and download the enriched data, logs, and intermediate artifacts.")

    tab1, tab2 = st.tabs(["Final Results", "Pipeline Artifacts"])

    with tab1:
        # Display final results
        results_data = st.session_state.get("results")  # Use .get for safer access

        if results_data is not None and not results_data.empty:
            st.subheader("Final Enriched Data")
            st.dataframe(results_data, use_container_width=True)

            # Prepare data for download
            @st.cache_data  # Cache the conversion to avoid re-running on every interaction
            def convert_df_to_csv(df):
                # IMPORTANT: Cache the conversion to prevent computation on every rerun
                return df.to_csv(index=False).encode("utf-8")

            csv_data = convert_df_to_csv(results_data)

            st.download_button(
                label="Download Results as CSV",
                data=csv_data,
                file_name="enriched_company_data.csv",
                mime="text/csv",
                key="download-csv",
            )
        elif st.session_state["job_status"] == "Running":
            st.info("Processing is ongoing. Results will appear here when complete.")
        elif st.session_state["job_status"] in ["Error", "Completed (No Data)"]:
            st.warning(
                "No results to display. Check the Monitoring section for status and logs."
            )
        else:
            st.info("No results yet. Input data and start processing.")

    with tab2:
        st.subheader("Pipeline Artifacts")

        # Get pipeline output path from session state
        if (
            "pipeline_config" in st.session_state
            and st.session_state["pipeline_config"]
        ):
            output_dir = st.session_state["pipeline_config"].get("output_dir")
            if output_dir and Path(output_dir).exists():
                st.write(f"Pipeline output directory: `{output_dir}`")

                # List all files in the output directory
                output_path = Path(output_dir)
                all_runs = [d for d in output_path.glob("pipeline_run_*") if d.is_dir()]

                if all_runs:
                    # Sort runs by modification time (newest first)
                    all_runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

                    # Select run
                    selected_run = st.selectbox(
                        "Select Run:",
                        all_runs,
                        format_func=lambda p: f"{p.name} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(p.stat().st_mtime))})",
                    )

                    if selected_run:
                        # List phases in this run
                        phases = [d for d in selected_run.glob("*") if d.is_dir()]
                        if phases:
                            st.write("### Available Phases")

                            # Create tabs for each phase
                            phase_tabs = st.tabs([p.name for p in phases])

                            # Display files for each phase
                            for i, phase_dir in enumerate(phases):
                                with phase_tabs[i]:
                                    st.write(f"#### Files from {phase_dir.name}")

                                    # List all files in the phase directory
                                    phase_files = list(phase_dir.glob("**/*"))
                                    phase_files = [
                                        f for f in phase_files if f.is_file()
                                    ]

                                    if phase_files:
                                        # Create a table of files
                                        file_data = []
                                        for file_path in phase_files:
                                            rel_path = file_path.relative_to(phase_dir)
                                            size = file_path.stat().st_size
                                            size_str = (
                                                f"{size / 1024:.1f} KB"
                                                if size > 1024
                                                else f"{size} bytes"
                                            )
                                            file_data.append(
                                                {
                                                    "File": str(rel_path),
                                                    "Size": size_str,
                                                    "Path": str(file_path),
                                                }
                                            )

                                        # Display as a DataFrame
                                        df_files = pd.DataFrame(file_data)
                                        st.dataframe(df_files, use_container_width=True)

                                        # Create download buttons for each file
                                        selected_file = st.selectbox(
                                            "Select file to download:",
                                            phase_files,
                                            format_func=lambda p: p.name,
                                            key=f"select_{phase_dir.name}",
                                        )

                                        if selected_file:
                                            try:
                                                file_contents = (
                                                    selected_file.read_bytes()
                                                )
                                                st.download_button(
                                                    label=f"Download {selected_file.name}",
                                                    data=file_contents,
                                                    file_name=selected_file.name,
                                                    mime="application/octet-stream",
                                                    key=f"download_{phase_dir.name}_{selected_file.name}",
                                                )
                                            except Exception as e:
                                                st.error(f"Error reading file: {e}")
                                    else:
                                        st.info("No files found in this phase.")
                        else:
                            st.info("No phase directories found in this run.")
                else:
                    st.info("No pipeline runs found in the output directory.")
            else:
                st.info("Output directory not found or not yet created.")
        else:
            st.info("No pipeline has been run yet. Start processing data first.")


# --- Sidebar Navigation ---


def handle_navigation():
    """Callback function to update the page state."""
    st.session_state["page"] = st.session_state["navigation_choice"]
    print(f"Navigation handled, page set to: {st.session_state['page']}")


st.sidebar.title("Navigation")
page_options = ["Input", "Configuration", "Monitoring", "Output"]

# Use a separate key for the radio widget and an on_change callback
st.sidebar.radio(
    "Go to",
    options=page_options,
    key="navigation_choice",  # Key for the widget's state
    on_change=handle_navigation,  # Function to call when the value changes
    # The index is now set based on the main 'page' state, ensuring consistency
    index=page_options.index(st.session_state["page"]),
)

# --- Main App Logic ---
if __name__ == "__main__":
    # Initialize multiprocessing support
    multiprocessing.set_start_method("fork", force=True)

    # Process any queue messages on each rerun if pipeline is running
    if st.session_state["job_status"] == "Running":
        process_queue_messages()

    # Check if process is done
    if (
        "pipeline_process" in st.session_state
        and st.session_state["pipeline_process"] is not None
    ):
        p = st.session_state["pipeline_process"]
        if not p.is_alive() and st.session_state["job_status"] == "Running":
            # Final processing of any remaining messages
            process_queue_messages()

            # If status wasn't updated by the queue messages, update it now
            if st.session_state["job_status"] == "Running":
                st.session_state["job_status"] = "Completed"
                st.session_state["progress"] = 100
                logging.info("Pipeline process completed")

    # Display the selected page
    page = st.session_state["page"]
    if page == "Input":
        display_input_section()
    elif page == "Configuration":
        display_config_section()
    elif page == "Monitoring":
        display_monitoring_section()
    elif page == "Output":
        display_output_section()

    print(f"Displayed page: {page}")
