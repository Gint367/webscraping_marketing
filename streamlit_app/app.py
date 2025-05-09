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
        "artifacts": None,
        "testing_mode": False,  # Flag to disable rerun
        # Auto-refresh configuration
        "auto_refresh_enabled": True,  # Auto-refresh logs by default
        "refresh_interval": 3.0,  # Default refresh interval in seconds
        # Job management
        "active_jobs": {},  # Dictionary of job_id -> job_data for all active/recent jobs
        "selected_job_id": None,  # Currently selected job for viewing details
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

    # No longer need to check global log_messages as we've moved to per-job logging

    return is_first_full_init


init_session_state()


# --- Logging Handler for Streamlit ---
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        # Create log directory if it doesn't exist
        self.log_dir = os.path.join(project_root, "logfiles")
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file_path = os.path.join(self.log_dir, "streamlit_app.log")

    def emit(self, record: logging.LogRecord):
        """
        Emit a log record to the appropriate location in Streamlit session state and to the app log file.
        - If a job is selected and exists in active_jobs, append to that job's log_messages.
        - If 'log_messages' exists at the top level of session_state (legacy), append there as well.
        """
        try:
            msg = self.format(record)

            # Get the currently selected job id
            selected_job_id = st.session_state.get("selected_job_id")

            # If a job is selected, add the log to that job's log list
            if selected_job_id and selected_job_id in st.session_state.get(
                "active_jobs", {}
            ):
                job_data = st.session_state["active_jobs"][selected_job_id]
                if "log_messages" not in job_data:
                    job_data["log_messages"] = []
                job_data["log_messages"].append(msg)

            # Legacy support: if 'log_messages' exists at the top level, append there too
            if "log_messages" in st.session_state and isinstance(
                st.session_state["log_messages"], list
            ):
                st.session_state["log_messages"].append(msg)

            # Always write to the application log file
            with open(self.log_file_path, "a") as f:
                f.write(f"{msg}\n")

        except (KeyError, AttributeError, Exception) as e:
            # Pass the record to handleError as expected by the logging framework
            print(f"Error in StreamlitLogHandler: {str(e)}")
            self.handleError(record)


# Add the handler to the root logger AFTER initial state setup
streamlit_handler = StreamlitLogHandler()
logging.getLogger().addHandler(streamlit_handler)


# --- Job Management Utilities ---
def generate_job_id() -> str:
    """
    Generate a unique job ID combining timestamp.

    Returns:
        str: A unique job ID string
    """

    # Generate timestamp with milliseconds
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    # Combine for a unique ID
    return f"job_{timestamp}"


def cancel_job(job_id: str) -> bool:
    """
    Cancel a running job by its ID.

    Args:
        job_id: The ID of the job to cancel

    Returns:
        bool: True if job was cancelled, False if it couldn't be found or was already completed
    """
    if job_id not in st.session_state.get("active_jobs", {}):
        return False

    job_data = st.session_state["active_jobs"][job_id]

    # Only attempt to cancel if process exists and is running
    if job_data.get("process") and job_data.get("process").is_alive():
        try:
            # Terminate the process
            job_data["process"].terminate()

            # Update job status
            job_data["status"] = "Cancelled"
            job_data["phase"] = "Terminated by user"
            job_data["end_time"] = time.time()

            logging.info(f"Job {job_id} was cancelled by user")
            return True
        except Exception as e:
            logging.error(f"Failed to cancel job {job_id}: {e}")
            return False

    return False


# --- Pipeline Processing in Separate Process ---
def run_pipeline_in_process(
    config: Dict[str, Any], log_queue: Queue, status_queue: Queue, job_id: str = None
):
    """
    Run the pipeline in a separate process.

    Args:
        config: Configuration for the pipeline
        log_queue: Queue for passing log messages back to the main process
        status_queue: Queue for sending status updates to the main process
        job_id: The unique ID for this pipeline job
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
    status_data = {"status": "Running", "progress": 0, "phase": "Initializing"}
    if job_id:
        status_data["job_id"] = job_id
    status_queue.put(status_data)

    try:
        print(f"Pipeline process started with config: {config}")
        logging.info(
            f"Pipeline process started for job {job_id}"
            if job_id
            else "Pipeline process started"
        )

        status_data = {
            "status": "Running",
            "progress": 10,
            "phase": "Starting Pipeline",
        }
        if job_id:
            status_data["job_id"] = job_id
        status_queue.put(status_data)

        # Execute the pipeline
        final_output = run_pipeline(config)

        # Send success status with final output path
        status_data = {
            "status": "Completed",
            "progress": 100,
            "phase": "Finished",
            "output_path": final_output,
        }
        if job_id:
            status_data["job_id"] = job_id
        status_queue.put(status_data)

        print(f"Pipeline process completed successfully, output at: {final_output}")
        logging.info(
            f"Pipeline process completed successfully for job {job_id}, output at: {final_output}"
            if job_id
            else f"Pipeline completed successfully, output at: {final_output}"
        )

    except Exception as e:
        error_msg = str(e)
        logging.error(
            f"Pipeline process failed for job {job_id}: {error_msg}"
            if job_id
            else f"Pipeline process failed: {error_msg}"
        )
        print(f"Pipeline process failed: {error_msg}")

        status_data = {
            "status": "Error",
            "progress": 0,
            "phase": "Failed",
            "error": error_msg,
        }
        if job_id:
            status_data["job_id"] = job_id
        status_queue.put(status_data)


# Function to monitor queues and update session state
def process_queue_messages():
    """
    Process messages from the log and status queues for all active jobs,
    updating the Streamlit session state.
    This should be called on each Streamlit rerun.
    """
    # Initialize active_jobs if it doesn't exist
    if "active_jobs" not in st.session_state:
        st.session_state["active_jobs"] = {}

    # Process messages for all active jobs
    any_logs_updated = False

    # Process all jobs' status queues
    for job_id, job_data in st.session_state["active_jobs"].items():
        if "status_queue" in job_data and job_data["status_queue"] is not None:
            try:
                while not job_data["status_queue"].empty():
                    status_update = job_data["status_queue"].get_nowait()
                    if status_update:
                        # Update status fields
                        if "status" in status_update:
                            job_data["status"] = status_update["status"]
                        if "progress" in status_update:
                            job_data["progress"] = status_update["progress"]
                        if "phase" in status_update:
                            job_data["phase"] = status_update["phase"]

                        # Handle completion
                        if (
                            status_update.get("status") == "Completed"
                            and "output_path" in status_update
                        ):
                            output_path = status_update["output_path"]
                            try:
                                # Load the results
                                results_df = pd.read_csv(output_path)
                                job_data["results"] = results_df
                                job_data["output_path"] = output_path
                                job_data["end_time"] = time.time()
                            except Exception as e:
                                error_msg = (
                                    f"Error loading results for job {job_id}: {e}"
                                )
                                job_data["log_messages"].append(error_msg)
                                job_data["error_message"] = error_msg

                        # Handle error
                        if (
                            status_update.get("status") == "Error"
                            and "error" in status_update
                        ):
                            job_data["error_message"] = status_update["error"]
                            job_data["end_time"] = time.time()
            except Exception as e:
                error_msg = f"Error processing status queue for job {job_id}: {e}"
                if "log_messages" not in job_data:
                    job_data["log_messages"] = []
                job_data["log_messages"].append(error_msg)

    # Now process all jobs' log queues
    for job_id, job_data in st.session_state["active_jobs"].items():
        if "log_queue" in job_data and job_data["log_queue"] is not None:
            try:
                logs_updated = False
                while not job_data["log_queue"].empty():
                    record = job_data["log_queue"].get_nowait()
                    if record:
                        log_message = f"{record.asctime if hasattr(record, 'asctime') else ''} - {record.levelname if hasattr(record, 'levelname') else ''} - {record.getMessage() if hasattr(record, 'getMessage') else str(record)}"
                        # Add to job-specific logs
                        if "log_messages" not in job_data:
                            job_data["log_messages"] = []
                        job_data["log_messages"].append(log_message)
                        logs_updated = True

                # Check if the job process is still alive
                if "process" in job_data and job_data["process"] is not None:
                    process = job_data["process"]
                    if not process.is_alive() and job_data.get("status") == "Running":
                        # Process ended but status wasn't properly updated
                        job_data["status"] = "Completed"
                        job_data["phase"] = "Finished (Status not properly updated)"
                        job_data["end_time"] = time.time()

                if logs_updated:
                    any_logs_updated = True

            except Exception as e:
                error_msg = f"Error processing log queue for job {job_id}: {e}"
                if "log_messages" not in job_data:
                    job_data["log_messages"] = []
                job_data["log_messages"].append(error_msg)
                any_logs_updated = True

    # Set flag in session state to indicate new logs available
    if any_logs_updated:
        st.session_state["logs_updated"] = True


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

                st.rerun()

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
        st.session_state["company_list"] = None
        print("Switched to Manual Input, cleared file upload state.")


def process_data():
    """Processes the data from the selected input method."""
    st.toast("process_data called")
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
                logging.warning("No valid data in manual input.")
                return

            data_to_process = manual_df.to_dict("records")
            st.session_state["company_list"] = data_to_process  # Store the final list
            logging.info(f"Processing {len(data_to_process)} manually entered records.")
        else:
            st.warning("No manual data entered.")
            logging.warning("Start Processing clicked with no manual data.")
            return
    else:
        st.warning("No data provided. Please upload a file or enter data manually.")
        logging.warning(
            "Start Processing clicked with no data source selected or data provided."
        )
        return

    # --- Prepare Pipeline Configuration ---
    if data_to_process:
        st.info(f"Starting enrichment for {len(data_to_process)} companies...")
        logging.info(f"Data prepared for pipeline: {len(data_to_process)} records.")

        try:
            # Generate a unique job ID
            job_id = generate_job_id()

            # Create a job-specific output directory to prevent overwriting
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            job_output_dir = os.path.join(project_root, "outputs", f"job_{timestamp}")
            os.makedirs(job_output_dir, exist_ok=True)

            # Create a temporary CSV file for the pipeline
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                temp_csv_path = temp_file.name
                df = pd.DataFrame(data_to_process)
                df.to_csv(temp_csv_path, index=False)
                logging.info(
                    f"Temporary input CSV created at {temp_csv_path} for job {job_id}"
                )

            # Create output directory if it doesn't exist
            output_dir = job_output_dir
            os.makedirs(output_dir, exist_ok=True)

            # Prepare the configuration
            pipeline_config = {
                "input_csv": temp_csv_path,
                "output_dir": output_dir,
                "category": st.session_state["config"].get("category"),
                "log_level": "INFO",  # This can be configured via UI if needed
                "skip_llm_validation": True,  # Adjust as needed or make configurable via UI
                "job_id": job_id,  # Include job_id in the configuration
            }

            # Set up the queues for communication between processes
            manager = Manager()
            log_queue = manager.Queue()
            status_queue = manager.Queue()

            # Create job entry with initial state
            job_data = {
                "id": job_id,
                "status": "Initializing",
                "progress": 0,
                "phase": "Creating job",
                "start_time": time.time(),
                "end_time": None,
                "process": None,
                "log_queue": log_queue,
                "status_queue": status_queue,
                "config": pipeline_config,
                "log_messages": [],
                "results": None,
                "output_path": None,
                "error_message": None,
                "file_info": {
                    "type": st.session_state["input_method"],
                    "name": (
                        st.session_state["uploaded_file_data"].name
                        if st.session_state["input_method"] == "File Upload"
                        and st.session_state["uploaded_file_data"]
                        else "Manual Input"
                    ),
                    "record_count": len(data_to_process),
                },
            }

            # Store the job data in session state
            if "active_jobs" not in st.session_state:
                st.session_state["active_jobs"] = {}
            st.session_state["active_jobs"][job_id] = job_data

            # Set this as the selected job
            st.session_state["selected_job_id"] = job_id

            # Start the pipeline in a separate process
            p = Process(
                target=run_pipeline_in_process,
                args=(pipeline_config, log_queue, status_queue, job_id),
            )
            p.daemon = True  # Set as daemon so it terminates when the main process ends
            p.start()

            # Update the job record with the process
            job_data["process"] = p
            job_data["status"] = "Running"
            job_data["phase"] = "Starting Pipeline"
            job_data["progress"] = 5

            print(f"Pipeline process started with PID: {p.pid} for job {job_id}")
            logging.info(f"Pipeline process started with PID: {p.pid} for job {job_id}")

            # Add initial log entry to the job
            job_data["log_messages"].append(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Job {job_id} started with {len(data_to_process)} companies"
            )

        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")

            # Update job status if job was created
            if "job_id" in locals() and job_id in st.session_state.get(
                "active_jobs", {}
            ):
                st.session_state["active_jobs"][job_id]["status"] = "Error"
                st.session_state["active_jobs"][job_id]["phase"] = "Failed to start"
                st.session_state["active_jobs"][job_id]["error_message"] = str(e)
                st.session_state["active_jobs"][job_id]["end_time"] = time.time()

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
    st.write("Track the progress of the scraping and enrichment processes.")

    # Initial queue processing is still needed outside the fragment
    process_queue_messages()

    # Job selection and management section
    with st.container():  # TODO Use st.fragment to auto update this container
        st.subheader("Jobs")

        # Get active jobs
        active_jobs = st.session_state.get("active_jobs", {})

        # Ensure the most recent job is selected by default if none is selected
        selected_job_id = st.session_state.get("selected_job_id")
        if not selected_job_id and active_jobs:
            sorted_jobs = sorted(
                active_jobs.items(),
                key=lambda x: x[1].get("start_time", 0),
                reverse=True,
            )
            if sorted_jobs:
                selected_job_id = sorted_jobs[0][0]
                st.session_state["selected_job_id"] = selected_job_id

        if not active_jobs:
            st.info(
                "No jobs have been run yet. Start a new job from the Input section."
            )
        else:
            # Create a table of jobs with their status
            job_rows = []
            for job_id, job_data in active_jobs.items():
                # Format timestamp as human-readable
                start_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job_data.get("start_time", 0))
                )

                # Calculate duration
                if job_data.get("end_time"):
                    duration_sec = job_data["end_time"] - job_data["start_time"]
                    if duration_sec < 60:
                        duration = f"{int(duration_sec)}s"
                    else:
                        duration = (
                            f"{int(duration_sec / 60)}m {int(duration_sec % 60)}s"
                        )
                else:
                    # For running jobs, calculate current duration
                    duration_sec = time.time() - job_data["start_time"]
                    if duration_sec < 60:
                        duration = f"{int(duration_sec)}s (running)"
                    else:
                        duration = f"{int(duration_sec / 60)}m {int(duration_sec % 60)}s (running)"

                # Get file info
                file_info = job_data.get("file_info", {})
                file_type = file_info.get("type", "Unknown")
                file_name = file_info.get("name", "Unknown")
                record_count = file_info.get("record_count", 0)

                job_rows.append(
                    {
                        "ID": job_id,
                        "Status": job_data.get("status", "Unknown"),
                        "Start Time": start_time_str,
                        "Duration": duration,
                        "Progress": job_data.get("progress", 0),
                        "Source": f"{file_type}: {file_name} ({record_count} records)",
                    }
                )

            # Convert to DataFrame for display
            jobs_df = pd.DataFrame(job_rows)

            # Display jobs table
            st.dataframe(
                jobs_df,
                column_config={
                    "ID": st.column_config.TextColumn("Job ID"),
                    "Status": st.column_config.TextColumn("Status"),
                    "Start Time": st.column_config.TextColumn("Start Time"),
                    "Duration": st.column_config.TextColumn("Duration"),
                    "Progress": st.column_config.ProgressColumn(
                        "Progress", format="%d%%", min_value=0, max_value=100
                    ),
                    "Source": st.column_config.TextColumn("Source"),
                },
                hide_index=True,
                use_container_width=True,
            )

            # Add a selectbox for job selection
            job_ids = [job_id for job_id in active_jobs.keys()]
            job_labels = [
                f"{job_id} - {active_jobs[job_id].get('status', 'Unknown')}"
                for job_id in job_ids
            ]

            selected_job_index = 0
            if (
                "selected_job_id" in st.session_state
                and st.session_state["selected_job_id"] in job_ids
            ):
                selected_job_index = job_ids.index(st.session_state["selected_job_id"])

            selected_job_label = st.selectbox(
                "Select Job:",
                options=job_labels,
                index=selected_job_index,
                key="job_selector",
            )

            # Extract job_id from the selected label
            if selected_job_label:
                selected_job_id = job_ids[job_labels.index(selected_job_label)]
                st.session_state["selected_job_id"] = selected_job_id

            # Show only the Cancel button for the selected job
            if st.button("Cancel Selected Job", key="cancel_job_btn"):
                if (
                    "selected_job_id" in st.session_state
                    and st.session_state["selected_job_id"]
                ):
                    job_id = st.session_state["selected_job_id"]

                    # Only try to cancel if job is running
                    if active_jobs[job_id].get("status") == "Running":
                        if cancel_job(job_id):
                            st.success(f"Job {job_id} has been cancelled.")
                        else:
                            st.error(f"Failed to cancel job {job_id}.")
                    else:
                        st.warning(
                            f"Job {job_id} is not running (status: {active_jobs[job_id].get('status')}). Only running jobs can be cancelled."
                        )
                else:
                    st.warning("Please select a job from the dropdown first.")

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

    # Show details for selected job (if none selected, select the most recent one)
    selected_job_id = st.session_state.get("selected_job_id")
    active_jobs = st.session_state.get("active_jobs", {})

    # If no job is selected but jobs exist, select the most recent one
    if not selected_job_id and active_jobs:
        # Sort jobs by start_time (descending) and get the most recent
        sorted_jobs = sorted(
            active_jobs.items(), key=lambda x: x[1].get("start_time", 0), reverse=True
        )
        if sorted_jobs:
            selected_job_id = sorted_jobs[0][0]
            st.session_state["selected_job_id"] = selected_job_id

    if selected_job_id and selected_job_id in active_jobs:
        job_data = active_jobs[selected_job_id]

        # Display current job status and phase
        status_color = {
            "Idle": "blue",
            "Initializing": "blue",
            "Running": "orange",
            "Completed": "green",
            "Error": "red",
            "Cancelled": "gray",
        }.get(job_data.get("status", "Unknown"), "blue")

        st.markdown(f"### Job: {selected_job_id}")

        st.markdown(
            f"**Status:** <span style='color:{status_color}'>{job_data.get('status', 'Unknown')}</span>",
            unsafe_allow_html=True,
        )

        if job_data.get("phase"):
            st.markdown(f"**Current Phase:** {job_data.get('phase')}")

        # Display progress bar based on selected job
        if job_data.get("status") == "Running":
            st.progress(job_data.get("progress", 0) / 100)
        elif job_data.get("status") in ["Completed", "Error", "Cancelled"]:
            st.progress(1.0)  # Full progress bar

        # Display error message if there is one
        if job_data.get("error_message"):
            st.error(f"Error: {job_data['error_message']}")
    else:
        st.info(
            "No jobs have been started yet. Use the 'Input' section to start a new job."
        )

    # Call the fragment to display the initial status
    display_status_info()

    # Auto-refresh control
    with st.expander("Log Auto-Refresh Settings"):
        col1, col2 = st.columns([1, 3])

        with col1:
            auto_refresh = st.toggle(
                "Auto-refresh enabled",
                value=st.session_state.get("auto_refresh_enabled", True),
                key="auto_refresh_toggle",
            )
            st.session_state["auto_refresh_enabled"] = auto_refresh

        with col2:
            refresh_interval = st.slider(
                "Refresh interval (seconds)",
                min_value=1.0,
                max_value=10.0,
                value=st.session_state.get("refresh_interval", 3.0),
                step=0.5,
                key="refresh_slider",
            )
            st.session_state["refresh_interval"] = refresh_interval

            try:
                refresh_rate_display = float(refresh_interval)
                st.caption(
                    f"Logs will refresh every {refresh_rate_display:.1f} seconds"
                )
            except (TypeError, ValueError):
                st.caption("Logs will refresh automatically")

    # Log display with selection based on selected job
    st.subheader("Logs")

    # The selected_job_id is already set by the selectbox above
    selected_job_id = st.session_state.get("selected_job_id")

    if selected_job_id and selected_job_id in st.session_state.get("active_jobs", {}):
        # Show logs for the selected job
        job_data = st.session_state["active_jobs"][selected_job_id]

        # Notify user which job's logs they're viewing
        st.markdown(f"*Showing logs for job: {selected_job_id}*")
    else:
        # Fallback to global logs if no job is selected
        st.markdown("*Showing global logs (no job selected)*")

    # Display logs in a scrollable container
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def display_logs():
        # Process queue messages to ensure logs are up to date
        process_queue_messages()

        # Get logs based on currently selected job
        selected_job_id = st.session_state.get("selected_job_id")
        current_logs = []

        if selected_job_id and selected_job_id in st.session_state.get(
            "active_jobs", {}
        ):
            current_logs = st.session_state["active_jobs"][selected_job_id].get(
                "log_messages", []
            )
            if not current_logs:
                current_logs = []  # Ensure we have a valid list even if log_messages is None
        else:
            current_logs = []  # No fallback to global logs anymore

        # Display logs in a container
        log_container = st.container(height=400)
        with log_container:
            if not current_logs:
                st.info("No log messages yet.")
            else:
                # Display logs in reverse order (newest first)
                for msg in reversed(current_logs):
                    st.text(msg)

    # Call the logs fragment
    display_logs()


def display_output_section():
    """Displays the results and download options."""
    st.header("4. Output")

    # Process any pending messages to ensure we have the latest results
    process_queue_messages()

    # Final results tab
    st.write("View and download the enriched data, logs, and intermediate artifacts.")

    tab1, tab2 = st.tabs(["Final Results", "Pipeline Artifacts"])

    with tab1:
        # Display a job selector for selecting which job's results to view
        selected_job_id = st.session_state.get("selected_job_id")
        active_jobs = st.session_state.get("active_jobs", {})

        # Create a list of completed jobs for selection
        completed_jobs = []
        for job_id, job_data in active_jobs.items():
            if (
                job_data.get("status") == "Completed"
                and job_data.get("results") is not None
            ):
                job_info = job_data.get("file_info", {})
                timestamp = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job_data.get("start_time", 0))
                )
                completed_jobs.append(
                    {
                        "id": job_id,
                        "display": f"Job {job_id} ({timestamp}) - {job_info.get('name', 'Unknown')} ({job_info.get('record_count', 0)} records)",
                    }
                )

        if completed_jobs:
            job_options = [job["display"] for job in completed_jobs]
            job_ids = [job["id"] for job in completed_jobs]

            # Default selection to current selected job if it's completed, otherwise first completed job
            default_idx = 0
            if selected_job_id in job_ids:
                default_idx = job_ids.index(selected_job_id)

            selected_display = st.selectbox(
                "Select job results to view:",
                options=job_options,
                index=default_idx,
                key="output_job_selector",
            )

            # Get the job ID from the selection
            selected_output_job_id = job_ids[job_options.index(selected_display)]

            # Display results for the selected job
            job_data = active_jobs[selected_output_job_id]
            results_data = job_data.get("results")

            if results_data is not None and not results_data.empty:
                st.subheader(f"Final Enriched Data - Job {selected_output_job_id}")
                st.dataframe(results_data, use_container_width=True)

                # Get output path for download
                output_path = job_data.get("output_path")
                if output_path:
                    try:
                        # Prepare data for download
                        @st.cache_data  # Cache the conversion to avoid re-running on every interaction
                        def convert_df_to_csv(df):
                            # IMPORTANT: Cache the conversion to prevent computation on every rerun
                            return df.to_csv(index=False).encode("utf-8")

                        csv_data = convert_df_to_csv(results_data)

                        st.download_button(
                            label="Download Results as CSV",
                            data=csv_data,
                            file_name=f"job_{selected_output_job_id}_results.csv",
                            mime="text/csv",
                            key=f"download-csv-{selected_output_job_id}",
                        )
                    except Exception as e:
                        st.error(f"Error preparing download: {e}")
            else:
                st.warning("Selected job has no results data available.")

        # Handle case when no jobs are completed yet
        elif any(
            job_data.get("status") == "Running" for job_data in active_jobs.values()
        ):
            st.info("Jobs are still running. Results will appear here when complete.")
        elif active_jobs:
            st.warning(
                "No completed jobs with results available. Check for errors in the Monitoring section."
            )
        else:
            st.info(
                "No jobs have been run yet. Start a job from the Input section to see results here."
            )

    with tab2:
        st.subheader("Pipeline Artifacts")

        # Get active jobs with output directories
        job_dirs = {}
        for job_id, job_data in st.session_state.get("active_jobs", {}).items():
            if "config" in job_data and job_data["config"].get("output_dir"):
                output_dir = job_data["config"].get("output_dir")
                if Path(output_dir).exists():
                    timestamp = time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(job_data.get("start_time", 0)),
                    )
                    job_dirs[job_id] = {
                        "path": output_dir,
                        "display": f"Job {job_id} ({timestamp}) - {job_data.get('file_info', {}).get('name', 'Unknown')}",
                    }

        if job_dirs:
            # Create job selection dropdown
            job_ids = list(job_dirs.keys())
            job_displays = [job_dirs[job_id]["display"] for job_id in job_ids]

            # Default to currently selected job if it exists in the list
            default_idx = 0
            selected_job_id = st.session_state.get("selected_job_id")
            if selected_job_id in job_ids:
                default_idx = job_ids.index(selected_job_id)

            selected_job_display = st.selectbox(
                "Select job artifacts to view:",
                options=job_displays,
                index=default_idx,
                key="artifacts_job_selector",
            )

            # Get the job ID and output directory from selection
            selected_artifact_job_id = job_ids[job_displays.index(selected_job_display)]
            output_dir = job_dirs[selected_artifact_job_id]["path"]

            st.write(f"Pipeline output directory: `{output_dir}`")

            # List all directories in the output directory (these are the phase directories)
            output_path = Path(output_dir)
            phases = [d for d in output_path.glob("*") if d.is_dir()]

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
                        phase_files = [f for f in phase_files if f.is_file()]

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
                                key=f"select_{selected_artifact_job_id}_{phase_dir.name}",
                            )

                            if selected_file:
                                try:
                                    file_contents = selected_file.read_bytes()
                                    st.download_button(
                                        label=f"Download {selected_file.name}",
                                        data=file_contents,
                                        file_name=selected_file.name,
                                        mime="application/octet-stream",
                                        key=f"download_{selected_artifact_job_id}_{phase_dir.name}_{selected_file.name}",
                                    )
                                except Exception as e:
                                    st.error(f"Error reading file: {e}")
                        else:
                            st.info("No files found in this phase.")
            else:
                # Check if there are any files directly in the output directory
                direct_files = [f for f in output_path.glob("*") if f.is_file()]
                if direct_files:
                    st.write("### Files in Output Directory")

                    # Create a table of files
                    file_data = []
                    for file_path in direct_files:
                        size = file_path.stat().st_size
                        size_str = (
                            f"{size / 1024:.1f} KB" if size > 1024 else f"{size} bytes"
                        )
                        file_data.append(
                            {
                                "File": file_path.name,
                                "Size": size_str,
                                "Path": str(file_path),
                            }
                        )

                    # Display as a DataFrame
                    df_files = pd.DataFrame(file_data)
                    st.dataframe(df_files, use_container_width=True)

                    # Create download buttons for files
                    selected_file = st.selectbox(
                        "Select file to download:",
                        direct_files,
                        format_func=lambda p: p.name,
                        key=f"select_direct_{selected_artifact_job_id}",
                    )

                    if selected_file:
                        try:
                            file_contents = selected_file.read_bytes()
                            st.download_button(
                                label=f"Download {selected_file.name}",
                                data=file_contents,
                                file_name=selected_file.name,
                                mime="application/octet-stream",
                                key=f"download_direct_{selected_artifact_job_id}_{selected_file.name}",
                            )
                        except Exception as e:
                            st.error(f"Error reading file: {e}")
                else:
                    st.info(
                        "No pipeline runs or direct files found in the output directory."
                    )
        else:
            st.info(
                "No output directories found for any jobs. Start processing data first."
            )


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

    # Process any queue messages on each rerun
    process_queue_messages()

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
