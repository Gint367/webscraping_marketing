import io
import logging
import multiprocessing
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Manager, Process, Queue
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import from master_pipeline.py
from master_pipeline import (  # noqa: E402
    run_pipeline,
)
from streamlit_app.models.job_data_model import JobDataModel  # noqa: E402

# Import from input_section modules (moved to top-level imports)
from streamlit_app.section.input_section import display_input_section  # noqa: E402
from streamlit_app.section.output_section import (  # noqa: E402
    display_output_section,
)
from streamlit_app.utils import db_utils  # noqa: E402
from streamlit_app.utils.utils import check_process_details_by_pid  # noqa: E402

# FOR LLM: DO NOT CHANGE PRINTS TO LOGGING
# --- Page Configuration (Must be the first Streamlit command) ---
st.set_page_config(
    layout="wide", page_title="Company Enrichment Tool"
)  # Added page config

# --- Database Connection ---
conn = st.connection("jobs_db", type="sql")

# --- Constants for Column Validation ---
REQUIRED_COLUMNS_MAP = {
    "company name": ["company name", "firma1"],
    "location": ["location", "ort"],
    "url": ["url"],
}


# Configure basic logging for the root logger (e.g., for libraries)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Added %(name)s
    datefmt="%H:%M:%S",
)

# Create a dedicated logger for the Streamlit application
app_logger = logging.getLogger("streamlit_app_main")
app_logger.setLevel(logging.INFO)
app_logger.propagate = True  # This will pass the logs to the streamlit_app.log


# --- Helper Functions ---
def validate_columns(
    df_columns: list[str],
    required_columns_map: dict[
        str, list[str]
    ],  # this is because of dependency injection
) -> tuple[dict[str, tuple[bool, str | None]], bool]:
    """
    Checks if required columns (or their aliases) exist in the DataFrame columns.

    Args:
        df_columns (list[str]): A list of column names from the DataFrame.
        required_columns_map (dict[str, list[str]]):
            A dictionary where keys are canonical required column names and values are lists of aliases for each required column.

    Returns:
        tuple[dict[str, tuple[bool, str | None]], bool]:
            - A dictionary where keys are the canonical required column names
              and values are tuples: (found_status: bool, actual_name_found: str | None).
            - A boolean indicating if all required columns were found.
    """
    validation_results: dict[str, tuple[bool, str | None]] = {}
    all_found: bool = True
    normalized_df_columns: dict[str, str] = {
        col.lower().strip(): col for col in df_columns
    }  # Store original casing

    for canonical_name, aliases in required_columns_map.items():
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
def init_session_state() -> bool:
    """
    Initializes session state variables if they don't exist.
    Also handles specific state adjustments based on input method.
    Initializes the database and loads existing jobs.
    Marks interrupted jobs.

    Returns:
        bool: True if this was the first full initialization, False otherwise.
    """
    # Sentinel key to check if this is the first time defaults are being applied in this session.
    is_first_full_init = "_app_defaults_initialized" not in st.session_state

    # Initialize database and create tables if they don't exist
    # This should be done early, before loading jobs or setting defaults that might depend on DB state.
    if "_db_initialized" not in st.session_state:
        try:
            db_utils.init_db(conn)
            app_logger.info("Database connection established and schema initialized.")
            st.session_state["_db_initialized"] = True
        except Exception as e:
            app_logger.error(f"Failed to initialize database: {e}")
            st.error(f"Application critical error: Failed to initialize database: {e}")
            # Depending on the severity, you might want to halt further execution
            # or allow the app to run in a degraded state if possible.
            # For now, we'll let it continue so other UI elements can render,
            # but operations requiring the DB will likely fail.
            st.session_state["_db_initialized"] = False  # Mark as failed

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
        "log_file_positions": {},  # Stores last read position for job log files
    }

    # Apply defaults if keys don't exist
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if "_jobs_loaded_from_db" not in st.session_state:
        try:
            loaded_jobs = db_utils.load_jobs_from_db(conn)
            st.session_state["active_jobs"] = loaded_jobs
            app_logger.info(
                f"Successfully loaded {len(loaded_jobs)} jobs from the database."
            )

            st.session_state["_jobs_loaded_from_db"] = True

        except Exception as e:
            app_logger.error(f"Failed to load or update jobs from database: {e}")
            # Keep the default empty 'active_jobs' if DB load fails
            if "active_jobs" not in st.session_state:  # Ensure it exists
                st.session_state["active_jobs"] = {}

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
            app_logger.info(
                "Re-initialized 'manual_input_df' as it was not a DataFrame in Manual Input mode."
            )

    # No longer need to check global log_messages as we've moved to per-job logging

    return is_first_full_init


def clear_other_input(selected_method: str) -> None:
    """
    Clears the session state of the non-selected input method.

    Args:
        selected_method (str): The input method selected ("File Upload" or "Manual Input").

    Returns:
        None
    """
    if selected_method == "File Upload":
        # When switching to File Upload, clear manual input DataFrame
        st.session_state["manual_input_df"] = pd.DataFrame(
            columns=["company name", "location", "url"]
        )
        st.session_state["company_list"] = None  # Clear any processed list
        print("Switched to File Upload, cleared manual input state.")
        app_logger.info("Switched to File Upload, cleared manual input state.")
    elif selected_method == "Manual Input":
        # When switching to Manual Input, clear uploaded file data
        st.session_state["uploaded_file_data"] = None
        # Consider if file_uploader widget needs explicit reset (often handled by Streamlit's keying)
        st.session_state["company_list"] = None  # Clear any processed list
        print("Switched to Manual Input, cleared file upload state.")
        app_logger.info("Switched to Manual Input, cleared file upload state.")
    else:
        app_logger.warning(
            f"clear_other_input called with unknown method: {selected_method}"
        )


init_session_state()


def parse_progress_log_line(log_line: str) -> Optional[tuple[str, str, str]]:
    """
    Parses a log line to extract progress information.
    Expected format: "PROGRESS:main_phase:step:details"
                     For example: "PROGRESS:webcrawl:extract_llm:1/8:Extracting data from example.com"

    Args:
        log_line (str): A single line from the log file.

    Returns:
        Optional[Tuple[str, str, str]]: A tuple containing (main_phase, step, details_str)
                                         if the line is a valid progress line.
                                         details_str will be empty if no details are present.
                                         Returns None otherwise.
    """
    log_line = log_line.strip()
    if not log_line.startswith("PROGRESS:"):
        return None

    try:
        # Remove "PROGRESS:" part
        content = log_line[len("PROGRESS:") :].strip()

        # Split by ":" to get components
        components = content.split(
            ":", 2
        )  # Split into max 3 parts: main_phase, step, details

        if len(components) < 2:
            return None

        main_phase = components[0].strip()
        step = components[1].strip()

        # The rest is details (which might contain additional colons)
        details = components[2].strip() if len(components) > 2 else ""

        if not main_phase or not step:
            return None

        return main_phase, step, details
    except Exception as e:
        app_logger.error(f"Error parsing progress log line '{log_line}': {e}")
        return None


def update_selected_job_progress_from_log(
    job_model: JobDataModel,
    conn,
    PHASE_FORMATS: Dict[str, Any],
    PHASE_ORDER: list[str],
    calculate_progress_from_phase: Callable[..., float],
) -> bool:
    """
    Reads new lines from a job's log file, parses the latest progress information,
    updates the job model, and saves it to the database.

    Args:
        job_model (JobDataModel): The job data model to update.
        conn: The database connection object.
        PHASE_FORMATS (Dict[str, Any]): Configuration for phase descriptions.
        PHASE_ORDER (list[str]): Order of phases for progress calculation.
        calculate_progress_from_phase (Callable[..., float]): Function to calculate progress,
                                                                expected to return a float.


    Returns:
        bool: True if an update occurred, False otherwise.
    """
    if not job_model.pipeline_log_file_path or not os.path.exists(
        job_model.pipeline_log_file_path
    ):
        # app_logger.debug(f"Log file path not set or does not exist for job {job_model.id}")
        return False

    updated = False
    last_read_position = st.session_state.get("log_file_positions", {}).get(
        job_model.id, 0
    )

    try:
        with open(job_model.pipeline_log_file_path, "r", encoding="utf-8") as f:
            f.seek(last_read_position)
            new_lines = f.readlines()
            current_position = f.tell()
            st.session_state.setdefault("log_file_positions", {})[job_model.id] = (
                current_position
            )

        if not new_lines:
            return False

        latest_progress_info = None
        for line in reversed(new_lines):
            # We are interested in lines that contain "PROGRESS:" not necessarily start with it,
            # as the logger might add timestamps etc.
            if "PROGRESS:" in line:
                # Extract the part of the string that actually starts with PROGRESS:
                progress_segment = line[line.find("PROGRESS:") :]
                parsed_info = parse_progress_log_line(progress_segment)
                if parsed_info:
                    latest_progress_info = parsed_info
                    break  # Found the last valid progress line

        if latest_progress_info:
            main_phase_key, sub_phase_key, details = latest_progress_info
            descriptive_phase = PHASE_FORMATS.get(main_phase_key, {}).get(sub_phase_key)

            if descriptive_phase:
                # Ensure job_model.status is valid before passing
                current_status = (
                    job_model.status
                    if hasattr(job_model, "status") and job_model.status
                    else "Running"
                )

                new_progress_float = calculate_progress_from_phase(
                    descriptive_phase,
                    PHASE_FORMATS,
                    PHASE_ORDER,
                    current_status,  # Use current_status
                    base_progress=0.05,
                )
                new_progress_int = int(new_progress_float * 100)

                if (
                    job_model.phase != descriptive_phase
                    or job_model.progress != new_progress_int
                ):
                    job_model.phase = descriptive_phase
                    job_model.progress = new_progress_int
                    db_utils.add_or_update_job_in_db(conn, job_model)
                    app_logger.info(
                        f"Job {job_model.id} progress updated from log: Phase='{descriptive_phase}', Progress={new_progress_int}%"
                    )
                    updated = True
            else:
                app_logger.warning(
                    f"Could not find descriptive phase for {main_phase_key}.{sub_phase_key} in PHASE_FORMATS for job {job_model.id}"
                )

    except FileNotFoundError:
        app_logger.warning(
            f"Log file not found for job {job_model.id} at {job_model.pipeline_log_file_path} during progress update."
        )
    except Exception as e:
        app_logger.error(
            f"Error updating job progress from log for job {job_model.id}: {e}",
            exc_info=True,
        )

    return updated


# --- Logging Handler for Streamlit ---
class StreamlitLogHandler(logging.Handler):
    LOG_FILE_TTL_DAYS = 2  # If streamlit_app.log is older than these days, delete it

    def __init__(self):
        super().__init__()
        # Create log directory if it doesn't exist
        self.log_dir = os.path.join(project_root, "logfiles")
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file_path = os.path.join(self.log_dir, "streamlit_app.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.setFormatter(formatter)
        # Set the log level to INFO by default
        self.setLevel(logging.INFO)
        # TTL Checks
        try:
            if os.path.exists(self.log_file_path):
                file_mod_time = os.path.getmtime(self.log_file_path)
                current_time = time.time()
                age_seconds = current_time - file_mod_time
                age_days = age_seconds / (24 * 3600)

                if age_days > self.LOG_FILE_TTL_DAYS:
                    os.remove(self.log_file_path)
                    logging.getLogger("streamlit_app_main").info(
                        f"Old log file {self.log_file_path} exceeded TTL and was deleted."
                    )
        except Exception as e:
            # Log an error if there's an issue checking/deleting the old log file,
            # but don't prevent the app from starting.
            # Using a direct print here as logger might not be fully set up.
            print(
                f"Error managing log file TTL for {self.log_file_path}: {e}",
                file=sys.stderr,
            )

    def emit(self, record: logging.LogRecord):
        """
        Emit a log record ONLY to the main application log file (streamlit_app.log).
        Job-specific logs are read directly from their dedicated files in the UI.
        """

        try:
            msg = self.format(record)
            # Always write to the application log file
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(f"{msg}\n")
        except Exception:
            # Pass the record to handleError as expected by the logging framework
            self.handleError(record)


# Define streamlit_handler at module level, initialized to None
streamlit_handler: Optional[StreamlitLogHandler] = None
# Add the handler to the root logger AFTER initial state setup, but only if it doesn't exist yet
root_logger = logging.getLogger()

# Use session_state to ensure the handler is added only once per session
if "_streamlit_log_handler_added" not in st.session_state:
    # This print helps confirm when this block is entered
    app_logger.debug(
        f"'_streamlit_log_handler_added' flag not in session_state. Checking root_logger.handlers: {root_logger.handlers}"
    )

    # As a safeguard, also check if a handler with the same class name is already present.
    # This handles edge cases but the session_state flag is the primary guard.
    if not any(
        h.__class__.__name__ == "StreamlitLogHandler" for h in root_logger.handlers
    ):
        streamlit_handler = StreamlitLogHandler()
        root_logger.addHandler(streamlit_handler)
        app_logger.debug(
            f"StreamlitLogHandler added. New root_logger.handlers: {root_logger.handlers}"
        )
    else:
        app_logger.debug(
            "StreamlitLogHandler with the same class name already found in root_logger.handlers. Not re-adding, but setting session flag."
        )
        app_logger.debug(
            f"StreamlitLogHandler found by class name. Not re-adding. root_logger.handlers: {root_logger.handlers}"
        )

    st.session_state["_streamlit_log_handler_added"] = (
        True  # Mark as added for this session
    )
else:
    app_logger.debug(
        f"'_streamlit_log_handler_added' flag found in session_state. Handler not added again. root_logger.handlers: {root_logger.handlers}"
    )


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

    job_model = st.session_state["active_jobs"][job_id]

    process_object = job_model.process

    # Check if the process_object is an instance of multiprocessing.Process
    if not isinstance(process_object, Process):
        app_logger.error(
            f"Job {job_id} has an invalid process object. "
            f"Expected multiprocessing.Process, got {type(process_object)}"
        )
        return False

    # Only attempt to cancel if process exists and is running
    if process_object and process_object.is_alive():
        try:
            # Terminate the process
            process_object.terminate()
            process_object.join()
            job_model.status = "Cancelled"
            job_model.end_time = time.time()
            db_utils.add_or_update_job_in_db(conn, job_model)
            app_logger.info(f"Job {job_id} successfully cancelled.")
            return True
        except Exception as e:
            app_logger.error(f"Failed to cancel job {job_id}: {e}")

    return False


# --- Pipeline Processing in Separate Process ---


def run_pipeline_in_process(
    config: Dict[str, Any],
    status_queue: Queue,
    job_id: Optional[str] = None,
):
    """
    Run the pipeline in a separate process.

    Args:
        config: Configuration for the pipeline. Example structure:
            {
                "input_csv": "/path/to/input.csv",  # Path to the input CSV file
                "output_dir": "/path/to/output",  # Directory to save the output
                "category": "example_category",  # Category for processing (optional)
                "log_level": "INFO",  # Logging level (e.g., DEBUG, INFO, WARNING)
                "skip_llm_validation": True,  # Whether to skip LLM validation (optional)
                "job_id": "job_20231010_123456",  # Unique job ID (optional)
            }
        status_queue: Queue for sending status updates to the main process.
        job_id: The unique ID for this pipeline job.
    """
    # Configure logging to capture pipeline logs and send to file
    root_logger = logging.getLogger()

    # Create log directory if it doesn't exist
    log_dir = os.path.join(project_root, "logfiles")
    os.makedirs(log_dir, exist_ok=True)

    # Generate log filename with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"pipeline_{timestamp}.log")

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
    root_logger.setLevel(logging.INFO)

    # Log the file location
    app_logger.info(f"Pipeline logs are being saved to: {log_file_path}")

    # Send initial status, including the log_file_path
    status_data = {
        "status": "Running",
        "progress": 0,
        "phase": "Initializing",
        "pipeline_log_file_path": log_file_path,  # Add log file path here
    }
    if job_id:
        status_data["job_id"] = job_id
    status_queue.put(status_data)
    input_csv_path_to_delete = config.get("input_csv")  # Store path for cleanup
    try:
        app_logger.info(f"Pipeline process started with config: {config}")
        app_logger.info(
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

        # Send success status with final output file path
        status_data = {
            "status": "Completed",
            "progress": 100,
            "phase": "Finished",
            "output_final_file_path": final_output,
        }
        if job_id:
            status_data["job_id"] = job_id
        status_queue.put(status_data)

        print(f"Pipeline process completed successfully, output at: {final_output}")
        app_logger.info(
            f"Pipeline process completed successfully for job {job_id}, output at: {final_output}"
            if job_id
            else f"Pipeline completed successfully, output at: {final_output}"
        )

    except Exception as e:
        error_msg = str(e)
        app_logger.error(
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
    finally:
        if input_csv_path_to_delete and os.path.exists(input_csv_path_to_delete):
            try:
                os.unlink(input_csv_path_to_delete)
                app_logger.info(
                    f"Temporary input CSV file deleted by pipeline process: {input_csv_path_to_delete}"
                )
            except Exception as e_unlink:
                app_logger.warning(
                    f"Pipeline process failed to delete temporary input CSV file: {input_csv_path_to_delete}, reason: {e_unlink}"
                )


# Hardcoded phase parsing dictionary for known phases and formatting (move to global scope)
PHASE_FORMATS = {
    "extracting_machine": {
        "get_bundesanzeiger_html": "Extracting Machine: Fetch Bundesanzeiger HTML",
        "clean_html": "Extracting Machine: Clean HTML",
        "extract_sachanlagen": "Extracting Machine: Extract Sachanlagen",
        "generate_report": "Extracting Machine: Generate Report",
        "merge_data": "Extracting Machine: Merge Technische Anlagen and Sachanlagen",
    },
    "webcrawl": {
        "crawl_domain": "Webcrawl: Crawl Domain",
        "extract_llm": "Webcrawl: Extract Keywords (LLM)",
        "fill_process_type": "Webcrawl: Fill Process Type",
        "pluralize_llm_file": "Webcrawl: Pluralize Keywords in File",
        "pluralize_llm_entry": "Webcrawl: Pluralize Keywords for Entry",
        "process_files": "Webcrawl: Consolidate Data",
        "convert_to_csv": "Webcrawl: Convert to CSV",
    },
    "integration": {
        "merge_technische_anlagen": "Integration: Merge Technische Anlagen",
        "enrich_data": "Integration: Enrich Data",
    },
}

# Define the order of main phases for progress calculation
PHASE_ORDER = ["extracting_machine", "webcrawl", "integration"]


# Function to monitor queues and update session state
def calculate_progress_from_phase(
    current_phase_str: Optional[str],
    phase_formats: dict,
    phase_order: list,
    current_status: Optional[str] = None,
    initial_progress_value: float = 0.01,
    starting_progress_value: float = 0.05,
    base_progress: float = 0.0,
) -> float:
    """
    Calculates the progress percentage based on the current phase relative to PHASE_FORMATS.

    Args:
        current_phase_str: The string describing the current phase.
        phase_formats: The dictionary defining known phases and their formats.
        phase_order: The list defining the order of main phases.
        current_status: The current job status (e.g., "Running", "Completed").
        initial_progress_value: Progress value for "Initializing" or "Creating job" phase.
        starting_progress_value: Progress value for "Starting Pipeline" phase.

    Returns:
        A float between 0.0 and 1.0 representing the progress.
    """
    if not current_phase_str:
        return base_progress

    # Handle terminal statuses or specific end phases first
    if (
        current_status in ["Completed", "Failed", "Error", "Cancelled"]
        or "Finished" in current_phase_str
    ):
        return 1.0

    # Handle specific initial phases that might occur before those in PHASE_FORMATS
    if "Initializing" in current_phase_str or "Creating job" in current_phase_str:
        return max(base_progress, initial_progress_value)
    if "Starting Pipeline" in current_phase_str:
        return max(base_progress, starting_progress_value)

    flat_ordered_phases = []
    for main_phase_key in phase_order:
        if main_phase_key in phase_formats:
            for sub_phase_description in phase_formats[main_phase_key].values():
                flat_ordered_phases.append(sub_phase_description)

    if not flat_ordered_phases:
        app_logger.warning(
            "PHASE_FORMATS is empty or misconfigured; cannot calculate dynamic progress."
        )
        return base_progress  # Fallback if no phases are defined in PHASE_FORMATS

    total_steps = len(flat_ordered_phases)
    current_step_count = 0

    for i, fmt_phase_description in enumerate(flat_ordered_phases):
        if current_phase_str.startswith(fmt_phase_description):
            current_step_count = i + 1  # Current step is 1-indexed
            break  # Found the current phase

    # If current_step_count is 0, it means the phase wasn't found in PHASE_FORMATS.
    # The progress will be based on the last matched phase or 0 if none matched yet.
    # This aligns with basing progress on known phases in PHASE_FORMATS.

    if total_steps == 0:  # Should be caught earlier, but as a safeguard
        return base_progress

    # Progress is distributed from base_progress to 1.0
    # For example, if base_progress=0.05, then phase progress goes from 0.05 to 1.0
    # So, progress = base_progress + (1-base_progress) * (current_step_count/total_steps)
    progress = base_progress + (1.0 - base_progress) * (
        float(current_step_count) / total_steps
    )

    return min(progress, 1.0)  # Ensure progress doesn't exceed 1.0


def process_queue_messages():
    """
    Process messages from the status queues for all active jobs,
    updating the Streamlit session state.
    This should be called on each Streamlit rerun.
    """
    # Initialize active_jobs if it doesn't exist
    if "active_jobs" not in st.session_state:
        st.session_state["active_jobs"] = {}

    # Process all jobs' status queues
    for job_id, job_model in st.session_state["active_jobs"].items():
        if job_model.status_queue is not None:
            while not job_model.status_queue.empty():
                try:
                    status_update = job_model.status_queue.get_nowait()
                    if "status" in status_update:
                        job_model.status = status_update["status"]
                        app_logger.info(
                            f"Job {job_id} status updated to: {job_model.status}"
                        )

                    if "progress" in status_update:
                        job_model.progress = status_update["progress"]
                        app_logger.debug(
                            f"Job {job_id} progress updated to: {job_model.progress}%"
                        )

                    if "phase" in status_update:
                        job_model.phase = status_update["phase"]
                        app_logger.info(
                            f"Job {job_id} phase updated to: {job_model.phase}"
                        )

                    if "pipeline_log_file_path" in status_update:
                        job_model.pipeline_log_file_path = status_update[
                            "pipeline_log_file_path"
                        ]
                        app_logger.info(
                            f"Job {job_id} log file path set to: {job_model.pipeline_log_file_path}"
                        )

                    if "output_final_file_path" in status_update:
                        job_model.output_file_path = status_update[
                            "output_final_file_path"
                        ]
                        app_logger.info(
                            f"Job {job_id} output file path set to: {job_model.output_file_path}"
                        )

                    if "error" in status_update:
                        job_model.error_message = status_update["error"]
                        app_logger.error(
                            f"Job {job_id} encountered an error: {job_model.error_message}"
                        )

                    # Always update the database after processing a message
                    db_utils.add_or_update_job_in_db(conn, job_model)

                except Exception as e:
                    app_logger.error(
                        f"Error processing status update for job {job_id}: {e}",
                        exc_info=True,
                    )

        # Do not set job status to Error/Interrupted just because process is None after rerun.
        # Status updates for running jobs should be handled by PID check logic only.


def process_data():
    """Processes the data from the selected input method."""
    print("Processing started.")

    data_to_process = None
    temp_csv_path = None
    if (
        st.session_state["input_method"] == "File Upload"
        and st.session_state["uploaded_file_data"]
    ):
        uploaded_file = st.session_state["uploaded_file_data"]
        app_logger.info(f"Processing uploaded file: {uploaded_file.name}")
        try:
            temp_csv_path = os.path.join(tempfile.gettempdir(), uploaded_file.name)
            with open(temp_csv_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            data_to_process = pd.read_csv(temp_csv_path).to_dict(orient="records")
        except Exception as e:
            app_logger.error(f"Failed to process uploaded file: {e}")
            st.error(f"Error processing uploaded file: {e}")
            return

    elif st.session_state["input_method"] == "Manual Input":
        manual_df = st.session_state.get("manual_input_df")
        if manual_df is not None and not manual_df.empty:
            data_to_process = manual_df.to_dict(orient="records")
        else:
            st.warning("No data provided in manual input.")
            app_logger.warning("Manual input was selected but no data was provided.")
            return
    else:
        st.warning("No data provided. Please upload a file or enter data manually.")
        app_logger.warning(
            "Start Processing clicked with no data source selected or data provided."
        )
        return

    # --- Prepare Pipeline Configuration ---
    if data_to_process:
        app_logger.info(f"Data prepared for pipeline: {len(data_to_process)} records.")

        job_id = None
        try:
            # Generate a unique job ID
            job_id = generate_job_id()
            st.info(
                f"Starting enrichment for {len(data_to_process)} companies as job {job_id}"
            )
            # Create a job-specific output directory to prevent overwriting
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            job_output_dir = os.path.join(project_root, "outputs", f"job_{timestamp}")
            os.makedirs(job_output_dir, exist_ok=True)

            # Create a temporary CSV file for the pipeline
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                pd.DataFrame(data_to_process).to_csv(temp_file.name, index=False)
                temp_csv_path = temp_file.name
                app_logger.info(f"Temporary input data CSV created at {temp_csv_path}")

            # Create output directory if it doesn't exist
            output_dir = job_output_dir
            os.makedirs(output_dir, exist_ok=True)

            # Prepare the configuration
            pipeline_config = {
                "input_csv": temp_csv_path,
                "output_dir": output_dir,
                "category": st.session_state["config"].get("category"),
                "log_level": "INFO",
                "skip_llm_validation": True,
                "job_id": job_id,
            }

            # Set up the queues for communication between processes
            manager = Manager()
            status_queue = manager.Queue()

            # Create job entry with initial state using JobDataModel
            job_model = JobDataModel(
                id=job_id,
                status="Initializing",
                progress=0,
                phase="Creating job",
                start_time=time.time(),
                end_time=None,
                process=None,
                status_queue=status_queue,
                config=pipeline_config,
                output_final_file_path=None,
                error_message=None,
                pipeline_log_file_path=None,
                temp_input_csv_path=temp_csv_path,
                file_info={
                    "type": st.session_state["input_method"],
                    "name": (
                        st.session_state["uploaded_file_data"].name
                        if st.session_state["input_method"] == "File Upload"
                        and st.session_state["uploaded_file_data"]
                        else "Manual Input"
                    ),
                    "record_count": len(data_to_process),
                },
                log_messages=[
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Job {job_id} started with {len(data_to_process)} companies"
                ],
                max_progress=0.0,
            )

            # Store the job model in session state
            if "active_jobs" not in st.session_state:
                st.session_state["active_jobs"] = {}
            st.session_state["active_jobs"][job_id] = job_model

            # Set this as the selected job
            st.session_state["selected_job_id"] = job_id

            # Persist initial job data to DB
            try:
                db_utils.add_or_update_job_in_db(conn, job_model)
                app_logger.info(f"Initial data for job {job_id} saved to database.")
            except Exception as db_exc:
                app_logger.error(
                    f"Failed to save initial data for job {job_id} to database: {db_exc}"
                )

            # Start the pipeline in a separate process
            p = Process(
                target=run_pipeline_in_process,
                args=(pipeline_config, status_queue, job_id),
            )
            p.daemon = True  # Set as daemon so it terminates when the main process ends
            p.start()

            # Update the job model with the process and running state
            job_model.process = p
            job_model.status = "Running"
            job_model.phase = "Starting Pipeline"
            job_model.progress = 5

            print(f"Pipeline process started with PID: {p.pid} for job {job_id}")
            app_logger.info(
                f"Pipeline process started with PID: {p.pid} for job {job_id}"
            )

            # Persist job data after process start to DB
            try:
                db_utils.add_or_update_job_in_db(conn, job_model)
                app_logger.info(
                    f"Job {job_id} data after process start saved to database."
                )
            except Exception as db_exc:
                app_logger.error(
                    f"Failed to save job {job_id} data after process start to database: {db_exc}"
                )

        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")

            # Update job status if job was created
            if "job_id" in locals() and job_id in st.session_state.get(
                "active_jobs", {}
            ):
                job_model = st.session_state["active_jobs"][job_id]
                job_model.status = "Error"
                job_model.phase = "Failed to start"
                job_model.error_message = str(e)
                job_model.end_time = time.time()
                # Persist error status to DB
                try:
                    db_utils.add_or_update_job_in_db(conn, job_model)
                    app_logger.info(f"Error status for job {job_id} saved to database.")
                except Exception as db_exc:
                    app_logger.error(
                        f"Failed to save error status for job {job_id} to database: {db_exc}"
                    )

            app_logger.error(f"Failed to start pipeline: {e}", exc_info=True)

    else:
        st.warning("No data to process. Please check your input.")
        app_logger.warning("No data to process after preparation.")


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
        app_logger.info(
            f"Configuration updated: Depth={st.session_state['config'].get('depth')}, LLM={st.session_state['config'].get('llm_provider')}"
        )


def display_monitoring_section():
    """Displays the job monitoring and log output."""
    st.header("3. Monitoring")
    st.write("Track the progress of the scraping and enrichment processes.")

    # --- Helper: Non-blocking PID check for jobs ---

    def _update_job_statuses_with_pid_check(jobs_dict, min_interval=30.0):
        """
        Checks the aliveness of jobs with status 'Running' or 'Initializing' that have a PID but no attached process object.
        Uses a non-blocking, debounced approach to avoid redundant checks and UI blocking.
        If the process is alive, keeps the job status as 'Running'.
        If the process is not alive, sets the job status to 'Completed' and updates the end time.

        Args:
            jobs_dict (dict): Dictionary of job_id to job_data objects.
            min_interval (float): Minimum interval in seconds between PID checks. Default is 30.0 seconds.

        Returns:
            None
        """
        now = time.time()
        last_check = st.session_state.get("_last_pid_check_time", 0)
        if st.session_state.get("_pid_check_in_progress", False):
            return  # Already running, skip
        if now - last_check < min_interval:
            return  # Too soon since last check, skip

        st.session_state["_pid_check_in_progress"] = True
        try:
            jobs_to_check = [
                (job_id, job_data)
                for job_id, job_data in jobs_dict.items()
                if getattr(job_data, "status", None) in ("Running", "Initializing")
                and getattr(job_data, "process", None) is None
                and getattr(job_data, "pid", None) is not None
            ]
            # If process is not attached (e.g. after reload), check PID
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_job_id = {
                    executor.submit(check_process_details_by_pid, job_data.pid): job_id
                    for job_id, job_data in jobs_to_check
                }
                for future in as_completed(future_to_job_id):
                    job_id = future_to_job_id[future]
                    try:
                        is_alive, _ = future.result()
                        if is_alive:
                            jobs_dict[job_id].status = "Running"
                        else:
                            jobs_dict[job_id].status = "Completed"
                            jobs_dict[job_id].end_time = time.time()
                    except Exception as e:
                        app_logger.warning(f"PID check failed for job {job_id}: {e}")
            st.session_state["_last_pid_check_time"] = now
        finally:
            st.session_state["_pid_check_in_progress"] = False

    # Always reload jobs from DB to ensure up-to-date info
    try:
        loaded_jobs = db_utils.load_jobs_from_db(conn)
        # Run non-blocking PID check for jobs that are "Running" or "Initializing" and have a PID but no process
        _update_job_statuses_with_pid_check(loaded_jobs)
        st.session_state["active_jobs"] = loaded_jobs
    except Exception as e:
        app_logger.error(f"Failed to reload jobs from DB: {e}")
    # Initial queue processing is still needed outside the fragment
    process_queue_messages()

    # --- Jobs Table (auto-refreshing fragment) ---
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def jobs_table_fragment():
        st.subheader("Jobs")
        process_queue_messages()
        active_jobs = st.session_state.get("active_jobs", {})
        if not active_jobs:
            st.info(
                "No jobs have been run yet. Start a new job from the Input section."
            )
        else:
            job_rows = []
            for job_id, job_data in active_jobs.items():
                start_time = getattr(job_data, "start_time", 0)
                end_time = getattr(job_data, "end_time", None)
                # Defensive: handle None, NaN, or invalid values
                try:
                    if start_time is None or (
                        isinstance(start_time, float) and (start_time != start_time)
                    ):
                        start_time = 0
                    if end_time is not None and (
                        isinstance(end_time, float) and (end_time != end_time)
                    ):
                        end_time = None
                except Exception:
                    start_time = 0
                    end_time = None

                try:
                    start_time_str = time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(start_time),
                    )
                except Exception:
                    start_time_str = "N/A"

                if end_time is not None:
                    try:
                        duration_sec = end_time - start_time
                        if duration_sec != duration_sec or duration_sec < 0:
                            duration = "N/A"
                        elif duration_sec < 60:
                            duration = f"{int(duration_sec)}s"
                        else:
                            duration = (
                                f"{int(duration_sec // 60)}m {int(duration_sec % 60)}s"
                            )
                    except Exception:
                        duration = "N/A"
                else:
                    try:
                        duration_sec = time.time() - start_time
                        if duration_sec != duration_sec or duration_sec < 0:
                            duration = "N/A (running)"
                        elif duration_sec < 60:
                            duration = f"{int(duration_sec)}s (running)"
                        else:
                            duration = f"{int(duration_sec // 60)}m {int(duration_sec % 60)}s (running)"
                    except Exception:
                        duration = "N/A (running)"
                file_info = getattr(job_data, "file_info", {})
                file_type = file_info.get("type", "Unknown")
                file_name = file_info.get("name", "Unknown")
                record_count = file_info.get("record_count", 0)
                job_rows.append(
                    {
                        "ID": job_id,
                        "Status": getattr(job_data, "status", "Unknown"),
                        "Start Time": start_time_str,
                        "Duration": duration,
                        "Progress": getattr(job_data, "progress", 0),
                        "Source": f"{file_type}: {file_name} ({record_count} records)",
                    }
                )
            jobs_df = pd.DataFrame(job_rows)
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

    jobs_table_fragment()

    # --- Job Selection and Cancel Button (separate container, not auto-refreshing) ---
    with st.container():
        active_jobs = st.session_state.get("active_jobs", {})
        job_ids = list(active_jobs.keys())
        # --- Sort job_ids by start_time (latest first) ---
        sorted_jobs = sorted(
            active_jobs.items(),
            key=lambda x: getattr(x[1], "start_time", 0),
            reverse=True,
        )
        sorted_job_ids = [job_id for job_id, _ in sorted_jobs]
        # --- Create a selectbox for job selection ---
        job_id_to_label = {
            job_id: f"{job_id} - {getattr(active_jobs[job_id], 'status', 'Unknown')}"
            for job_id in sorted_job_ids
        }

        # Determine the initial value for selected_job_id if not already set or invalid.
        # This logic runs BEFORE the selectbox is instantiated.
        current_selection = st.session_state.get("selected_job_id")

        if (
            not current_selection or current_selection not in active_jobs
        ) and active_jobs:
            # If no valid job is selected and jobs exist, select the most recent one.
            sorted_jobs = sorted(
                active_jobs.items(),
                key=lambda x: getattr(x[1], "start_time", 0),
                reverse=True,
            )
            if sorted_jobs:
                st.session_state["selected_job_id"] = sorted_jobs[0][0]
        elif not active_jobs:
            # If there are no jobs, ensure selected_job_id is None
            st.session_state["selected_job_id"] = None
        # If current_selection is valid and in active_jobs, it remains unchanged.

        # The st.selectbox will now use the value from st.session_state.selected_job_id
        # as its current selection due to the `key`.
        if job_ids:
            st.selectbox(
                label="Select Job:",
                options=job_ids,
                format_func=lambda job_id: job_id_to_label.get(job_id, str(job_id)),
                key="selected_job_id",
            )
        # else: No selectbox if no jobs. st.session_state.selected_job_id would be None.

        # Read the selected job ID from session state for the cancel button logic
        selected_job_id_for_cancel = st.session_state.get("selected_job_id")

        if st.button("Cancel Selected Job", key="cancel_job_btn"):
            if selected_job_id_for_cancel:
                if cancel_job(selected_job_id_for_cancel):
                    st.toast(f"Job {selected_job_id_for_cancel} cancelled.")
                else:
                    st.error(f"Could not cancel job {selected_job_id_for_cancel}.")
            else:
                st.warning("No job selected to cancel.")

    # Create a fragment for status information that auto-refreshes and displays job status/phase
    @st.fragment(
        run_every=st.session_state.get("refresh_interval", 3.0)
        if st.session_state.get("auto_refresh_enabled", True)
        and not st.session_state.get("testing_mode", False)
        else None
    )
    def display_status_info():
        # Process messages from queues inside fragment to ensure fresh data
        process_queue_messages()

        # Show details for selected job (value is read from session state)
        selected_job_id = st.session_state.get("selected_job_id")
        active_jobs = st.session_state.get("active_jobs", {})

        if selected_job_id and selected_job_id in active_jobs:
            job_data = active_jobs[selected_job_id]

            # --- Task 2.1: Call update_selected_job_progress_from_log ---
            if (
                job_data
                and job_data.status in ["Running", "Initializing"]
                and job_data.pipeline_log_file_path
            ):
                update_selected_job_progress_from_log(
                    job_model=job_data,
                    conn=conn,
                    PHASE_FORMATS=PHASE_FORMATS,
                    PHASE_ORDER=PHASE_ORDER,
                    calculate_progress_from_phase=calculate_progress_from_phase,
                )
            # --- End Task 2.1 ---

            job_status = getattr(job_data, "status", None)
            current_phase = getattr(job_data, "phase", None)

            # Display current job status and phase
            status_color = {
                "Idle": "blue",
                "Initializing": "blue",
                "Running": "orange",
                "Completed": "green",
                "Error": "red",
                "Cancelled": "gray",
                "Failed": "red",
            }.get(job_status or "Unknown", "blue")

            st.markdown(f"### Job: {selected_job_id}")

            st.markdown(
                f"**Status:** <span style='color:{status_color}'>{job_status or 'Unknown'}</span>",
                unsafe_allow_html=True,
            )

            # --- Task 2.1: Displayed phase should directly use job_data.phase ---
            if current_phase:  # current_phase is already job_data.phase
                st.markdown(f"**Current Phase:** {current_phase}")
            # --- End Task 2.1 ---

            # --- Task 2.1: Adjust progress display ---
            # Primary progress source is job_data.progress (integer 0-100)
            current_progress_from_model = (
                float(getattr(job_data, "progress", 0.0)) / 100.0
            )

            # Use the existing max_progress field from JobDataModel instead of max_progress_displayed
            # max_progress is already defined in JobDataModel and is properly stored

            # Update logic for max_progress (stored as fraction 0.0-1.0)
            if current_progress_from_model > job_data.max_progress:
                job_data.max_progress = current_progress_from_model

            progress_to_display_on_bar = job_data.max_progress
            # --- End Task 2.1 ---

            # Display progress bar based on progress_to_display_on_bar
            if (
                job_status == "Running" or job_status == "Initializing"
            ):  # Combined condition
                st.progress(progress_to_display_on_bar)
            elif job_status in [
                "Completed",
                "Error",
                "Failed",
                "Cancelled",
            ]:  # Explicitly handle terminal states
                st.progress(1.0)
            else:  # Default for unknown or other states
                st.progress(
                    progress_to_display_on_bar
                )  # Or 0.0, depending on desired behavior for non-active states

            # Display error message if there is one
            if getattr(job_data, "error_message", None):
                st.error(f"Error: {job_data.error_message}")
        else:
            st.info(
                "No job selected. Please select a job from the dropdown above to view its status."
            )

    # Call the fragment to display the initial status and enable auto-refresh for these details
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
        # Notify user which job's logs they're viewing
        st.markdown(f"*Showing logs for job: {selected_job_id}*")

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
        log_lines = []  # Initialize as an empty list

        if selected_job_id and selected_job_id in st.session_state.get(
            "active_jobs", {}
        ):
            job_data = st.session_state["active_jobs"][selected_job_id]
            pipeline_log_file_path = job_data.pipeline_log_file_path

            if pipeline_log_file_path and os.path.exists(pipeline_log_file_path):
                try:
                    with open(pipeline_log_file_path, "r", encoding="utf-8") as f:
                        log_lines = f.readlines()
                except Exception as e:
                    log_lines = [f"Error reading log file: {e}"]
            elif pipeline_log_file_path:
                log_lines = [f"Log file not found: {pipeline_log_file_path}"]
            else:
                initial_logs = job_data.log_messages
                if initial_logs:
                    st.text_area(
                        "Initial Job Messages",
                        value="\n".join(initial_logs),
                        height=100,
                        key=f"initial_log_{job_data.id}",
                        disabled=True,
                    )
                else:
                    st.info(
                        f"Job status: {getattr(job_data, 'status', 'N/A')} - Phase: {getattr(job_data, 'phase', 'N/A')} - Waiting for log file path..."
                    )

        else:
            # This case handles when no job is selected.
            log_lines = ["No job selected or job data not found."]

        # Display logs in a container
        log_container = st.container(height=400)
        with log_container:
            if not log_lines:
                st.info("No log messages yet.")
            else:
                # Display logs in reverse order (newest first)
                for msg in reversed(log_lines):
                    st.text(msg.strip())  # Use strip() to remove trailing newlines

    # Call the logs fragment
    display_logs()


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
        display_input_section(
            process_data_func=process_data,
            validate_columns_func=validate_columns,
            req_cols_map=REQUIRED_COLUMNS_MAP,
            clear_other_input_func_from_app=clear_other_input,  # Pass the function
        )
    elif page == "Configuration":
        display_config_section()
    elif page == "Monitoring":
        display_monitoring_section()
    elif page == "Output":
        display_output_section(conn)

    print(f"Displayed page: {page}")
