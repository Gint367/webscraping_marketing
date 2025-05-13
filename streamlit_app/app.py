import io
import logging
import multiprocessing
import os
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue
from pathlib import Path
from typing import Any, Dict, Optional

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

# Import from input_section modules (moved to top-level imports)
from streamlit_app.section.input_section import display_input_section  # noqa: E402
from streamlit_app.section.output_section import (  # noqa: E402
    display_output_section,
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


# Configure basic logging for the root logger (e.g., for libraries)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Added %(name)s
    datefmt="%H:%M:%S",
)

# Create a dedicated logger for the Streamlit application
app_logger = logging.getLogger("streamlit_app_main")
app_logger.setLevel(logging.INFO)
app_logger.propagate = True  # Do not pass logs to the root logger


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
        Emit a log record ONLY to the application log file (streamlit_app.log).
        Job-specific logs are populated by reading from their respective queues
        in `process_queue_messages`.
        """

        try:
            msg = self.format(record)
            # Always write to the application log file
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(f"{msg}\n")
        except Exception:
            # Pass the record to handleError as expected by the logging framework
            self.handleError(record)


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

    job_data = st.session_state["active_jobs"][job_id]

    process_object = job_data.get("process")

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

            # After termination, check process details by PID and log
            pid = getattr(process_object, "pid", None)
            if pid is not None:
                try:
                    from streamlit_app.utils.utils import check_process_details_by_pid
                except ImportError:
                    # fallback for relative import if running as __main__
                    from utils.utils import check_process_details_by_pid
                is_alive, details = check_process_details_by_pid(pid)
                app_logger.info(
                    f"After cancelling job {job_id}, process PID {pid} alive: {is_alive}. Details: {details}"
                )
            else:
                app_logger.info(
                    f"After cancelling job {job_id}, process object has no PID attribute."
                )

            # Update job status
            job_data["status"] = "Cancelled"
            job_data["phase"] = "Terminated by user"
            job_data["end_time"] = time.time()

            app_logger.info(f"Job {job_id} was cancelled by user")
            return True
        except Exception as e:
            app_logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

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
        print(f"Pipeline process started with config: {config}")
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
    Process messages from the log and status queues for all active jobs,
    updating the Streamlit session state.
    This should be called on each Streamlit rerun.
    """
    # Initialize active_jobs if it doesn't exist
    if "active_jobs" not in st.session_state:
        st.session_state["active_jobs"] = {}

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

    for job_id, job_data in st.session_state["active_jobs"].items():
        if "log_queue" in job_data and job_data["log_queue"] is not None:
            try:
                while not job_data["log_queue"].empty():
                    record = job_data["log_queue"].get_nowait()
                    if record:
                        log_message = f"{record.asctime if hasattr(record, 'asctime') else ''} - {record.levelname if hasattr(record, 'levelname') else ''} - {record.getMessage() if hasattr(record, 'getMessage') else str(record)}"
                        # Add to job-specific logs
                        if "log_messages" not in job_data:
                            job_data["log_messages"] = []
                        job_data["log_messages"].append(log_message)

                        # --- PROGRESS log parsing for phase update ---
                        try:
                            log_message_text = (
                                record.getMessage()
                                if hasattr(record, "getMessage")
                                else str(record)
                            )
                            if log_message_text.startswith("PROGRESS:"):
                                progress_details = log_message_text.split(
                                    "PROGRESS:", 1
                                )[1].strip()
                                # Split into at most 4 parts: component, sub_component, steps, description
                                parts = progress_details.split(":", 3)
                                new_phase_description = None
                                if len(parts) == 4:
                                    component, sub_component, steps_str, description = (
                                        parts
                                    )
                                    comp_fmt = component.replace("_", " ").title()
                                    sub_fmt = sub_component.replace("_", " ").title()
                                    if "/" in steps_str:
                                        try:
                                            current_step, total_steps = steps_str.split(
                                                "/"
                                            )
                                            if (
                                                current_step.isdigit()
                                                and total_steps.isdigit()
                                            ):
                                                phase_fmt = PHASE_FORMATS.get(
                                                    component, {}
                                                ).get(sub_component)
                                                if phase_fmt:
                                                    new_phase_description = f"{phase_fmt} - {description} ({current_step}/{total_steps})"
                                                else:
                                                    new_phase_description = f"{comp_fmt}: {sub_fmt} - {description} ({current_step}/{total_steps})"
                                            else:
                                                new_phase_description = f"{comp_fmt}: {sub_fmt} - {description} ({steps_str})"
                                        except Exception as e_parse_steps:
                                            app_logger.debug(
                                                f"Error parsing steps in PROGRESS log: {e_parse_steps}"
                                            )
                                            new_phase_description = f"{comp_fmt}: {sub_fmt} - {description} ({steps_str})"
                                    else:
                                        new_phase_description = (
                                            f"{comp_fmt}: {sub_fmt} - {description}"
                                        )
                                elif len(parts) == 3:
                                    component, sub_component, description = parts
                                    comp_fmt = component.replace("_", " ").title()
                                    sub_fmt = sub_component.replace("_", " ").title()
                                    new_phase_description = (
                                        f"{comp_fmt}: {sub_fmt} - {description}"
                                    )
                                elif len(parts) == 2:
                                    component, description = parts
                                    comp_fmt = component.replace("_", " ").title()
                                    new_phase_description = (
                                        f"{comp_fmt} - {description}"
                                    )
                                else:
                                    new_phase_description = (
                                        progress_details
                                        if len(progress_details) < 80
                                        else progress_details[:77] + "..."
                                    )

                                if new_phase_description:
                                    job_data["phase"] = new_phase_description
                                    app_logger.debug(
                                        f"Updated job {job_id} phase to: {new_phase_description}"
                                    )
                        except Exception as e_parse_progress:
                            app_logger.warning(
                                f"Error parsing PROGRESS log for job {job_id}: {e_parse_progress}"
                            )

                # Check if the job process is still alive
                if "process" in job_data and job_data["process"] is not None:
                    process = job_data["process"]
                    if not process.is_alive() and job_data.get("status") == "Running":
                        # Process ended but status wasn't properly updated
                        job_data["status"] = "Completed"
                        if (
                            not job_data.get("phase")
                            or "Finished" not in job_data["phase"]
                        ):
                            job_data["phase"] = "Finished (Status not properly updated)"
                        job_data["end_time"] = time.time()

            except Exception as e:
                error_msg = f"Error processing log queue for job {job_id}: {e}"
                if "log_messages" not in job_data:
                    job_data["log_messages"] = []
                job_data["log_messages"].append(error_msg)


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
            if uploaded_file.name.endswith(".csv"):
                # Use StringIO to treat the byte stream as a text file
                stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                df = pd.read_csv(stringio)
            elif uploaded_file.name.endswith((".xls", ".xlsx")):
                # Read excel directly from bytes
                df = pd.read_excel(uploaded_file)
            else:
                st.error("Unsupported file type.")
                app_logger.error("Unsupported file type uploaded.")
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
                app_logger.error(f"Uploaded file missing columns: {missing_cols}")
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
                subset=required_cols,
                inplace=True,  # Drop rows with missing required values
            )

            if df.empty:
                st.warning("No valid data found in the uploaded file after cleaning.")
                app_logger.warning("No valid data in uploaded file.")
                return

            data_to_process = df.to_dict("records")
            st.session_state["company_list"] = data_to_process  # Store the final list
            app_logger.info(
                f"Successfully parsed {len(data_to_process)} records from file."
            )

        except Exception as e:
            st.error(f"Error reading or processing file: {e}")
            st.session_state["job_status"] = "Error"
            app_logger.error(f"Error processing file {uploaded_file.name}: {e}")
            return

    elif st.session_state["input_method"] == "Manual Input":
        manual_df = st.session_state.get("manual_input_df")
        if manual_df is not None and not manual_df.empty:
            # Basic cleaning (optional, adapt as needed)
            manual_df = manual_df.map(lambda x: x.strip() if isinstance(x, str) else x)
            # Validate required columns (data editor should enforce this, but double-check)
            required_cols = ["company name", "location", "url"]
            manual_df.dropna(
                subset=required_cols,
                inplace=True,  # Drop rows missing required values
            )

            if manual_df.empty:
                st.warning("No valid data entered manually after cleaning.")
                app_logger.warning("No valid data in manual input.")
                return

            data_to_process = manual_df.to_dict("records")
            st.session_state["company_list"] = data_to_process  # Store the final list
            app_logger.info(
                f"Processing {len(data_to_process)} manually entered records."
            )
        else:
            st.warning("No manual data entered.")
            app_logger.warning("Start Processing clicked with no manual data.")
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
                temp_csv_path = temp_file.name
                df = pd.DataFrame(data_to_process)
                df.to_csv(temp_csv_path, index=False)
                app_logger.info(
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
                "status_queue": status_queue,
                "config": pipeline_config,
                "results": None,
                "output_path": None,
                "error_message": None,
                "pipeline_log_file_path": None,  # Initialize with None, will be updated
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
                args=(pipeline_config, status_queue, job_id),
            )
            p.daemon = True  # Set as daemon so it terminates when the main process ends
            p.start()

            # Update the job record with the process
            job_data["process"] = p
            job_data["status"] = "Running"
            job_data["phase"] = "Starting Pipeline"
            job_data["progress"] = 5

            print(f"Pipeline process started with PID: {p.pid} for job {job_id}")
            app_logger.info(
                f"Pipeline process started with PID: {p.pid} for job {job_id}"
            )

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

            app_logger.error(f"Failed to start pipeline: {e}", exc_info=True)

    else:
        # This case should ideally be caught earlier, but as a fallback:
        st.warning("No data available to process.")
        app_logger.warning("process_data called but data_to_process was empty.")


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
                start_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(job_data.get("start_time", 0))
                )
                if job_data.get("end_time"):
                    duration_sec = job_data["end_time"] - job_data["start_time"]
                    if duration_sec < 60:
                        duration = f"{int(duration_sec)}s"
                    else:
                        duration = (
                            f"{int(duration_sec / 60)}m {int(duration_sec % 60)}s"
                        )
                else:
                    duration_sec = time.time() - job_data["start_time"]
                    if duration_sec < 60:
                        duration = f"{int(duration_sec)}s (running)"
                    else:
                        duration = f"{int(duration_sec / 60)}m {int(duration_sec % 60)}s (running)"
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
            index=selected_job_index if job_labels else 0,
            key="job_selector",
        )
        # Extract job_id from the selected label
        if selected_job_label:
            st.session_state["selected_job_id"] = selected_job_label.split(" - ")[0]

        # Show only the Cancel button for the selected job
        if st.button("Cancel Selected Job", key="cancel_job_btn"):
            if (
                "selected_job_id" in st.session_state
                and st.session_state["selected_job_id"]
            ):
                if cancel_job(st.session_state["selected_job_id"]):
                    st.toast(f"Job {st.session_state['selected_job_id']} cancelled.")
                    # No need to rerun here, fragment will update
                else:
                    st.error(
                        f"Could not cancel job {st.session_state['selected_job_id']}."
                    )
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

        # Show details for selected job (if none selected, select the most recent one)
        selected_job_id = st.session_state.get("selected_job_id")
        active_jobs = st.session_state.get("active_jobs", {})

        # If no job is selected but jobs exist, select the most recent one
        if not selected_job_id and active_jobs:
            # Sort jobs by start_time (descending) and get the most recent
            sorted_jobs = sorted(
                active_jobs.items(),
                key=lambda x: x[1].get("start_time", 0),
                reverse=True,
            )
            if sorted_jobs:
                selected_job_id = sorted_jobs[0][0]
                st.session_state["selected_job_id"] = selected_job_id

        if selected_job_id and selected_job_id in active_jobs:
            job_data = active_jobs[selected_job_id]
            job_status = job_data.get("status")
            current_phase = job_data.get("phase")

            # Display current job status and phase
            status_color = {
                "Idle": "blue",
                "Initializing": "blue",
                "Running": "orange",
                "Completed": "green",
                "Error": "red",
                "Cancelled": "gray",
                "Failed": "red",
            }.get(job_status, "blue")

            st.markdown(f"### Job: {selected_job_id}")

            st.markdown(
                f"**Status:** <span style='color:{status_color}'>{job_status or 'Unknown'}</span>",
                unsafe_allow_html=True,
            )

            if current_phase:
                st.markdown(f"**Current Phase:** {current_phase}")

            # Determine base_progress from job_data["progress"] if available and valid
            base_progress = 0.0
            try:
                if "progress" in job_data and isinstance(
                    job_data["progress"], (int, float)
                ):
                    base_progress = float(job_data["progress"]) / 100.0
                    if base_progress >= 1.0:
                        base_progress = 0.99
                    if base_progress < 0.0:
                        base_progress = 0.0
            except Exception:
                base_progress = 0.0

            # Calculate progress dynamically, starting from base_progress
            calculated_progress = calculate_progress_from_phase(
                current_phase_str=current_phase,
                phase_formats=PHASE_FORMATS,
                phase_order=PHASE_ORDER,
                current_status=job_status,
                base_progress=base_progress,
            )

            # --- Prevent progress from going backwards ---
            if "max_progress" not in job_data:
                job_data["max_progress"] = 0.0
            if calculated_progress > job_data["max_progress"]:
                job_data["max_progress"] = calculated_progress
            progress_to_display = job_data["max_progress"]

            # Display progress bar based on progress_to_display
            if job_status == "Running":
                st.progress(progress_to_display)
            elif job_status == "Initializing":
                st.progress(progress_to_display)
            elif job_status in ["Completed", "Error", "Cancelled", "Failed"]:
                st.progress(1.0)
            # else: no progress bar for other states

            # Display error message if there is one
            if job_data.get("error_message"):
                st.error(f"Error: {job_data['error_message']}")
        else:
            st.info(
                "No jobs have been started yet. Use the 'Input' section to start a new job."
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
        display_output_section()

    print(f"Displayed page: {page}")
