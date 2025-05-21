"""
Brief explanation on how the different way job run being managed:
1. In-Memory Jobs:
   - These jobs are managed in the Streamlit session state.
   - They are created when a user starts a job through the UI.
   - Each job is associated with a process and a status queue.
   - The status of these jobs is updated in real-time as the pipeline runs.
   - The Job is only managed as long as user didnt close the browser and the session is active.
   - The jobs are persisted to the database up to the last save. and upon reload, it would be treated as Database Jobs.
2. Database Jobs:
   - These jobs are loaded from the database.
   - They are loaded into the session state when the app starts and merged with the current jobs in the active_jobs.
   - The jobs does not have process and status queue attached.
   - The jobs can be accessed and managed even after the session ends, as they are persisted in the database.
   - The management of the status and progress phase is based on reading the latest log lines in monitoring_section.
   - The job status management also heavily relies on PID checks.
"""
import io
import logging
import logging.handlers  # Added
import multiprocessing
import os
import signal
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

# Add project root to Python path, if you delete this you need to specify python -m streamlit... when running the app
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from master_pipeline import (  # noqa: E402
    run_pipeline,
)
from streamlit_app.models.job_data_model import JobDataModel  # noqa: E402
from streamlit_app.section.config_section import display_config_section  # noqa: E402
from streamlit_app.section.input_section import display_input_section  # noqa: E402
from streamlit_app.section.monitoring_section import (  # noqa: E402
    display_monitoring_section,
)
from streamlit_app.section.output_section import (  # noqa: E402
    display_output_section,
)
from streamlit_app.utils import db_utils  # noqa: E402
from streamlit_app.utils.job_utils import (  # noqa: E402
    merge_active_jobs_with_db,
)
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

# disable some module logging verboseness
logging.getLogger("httpx._client").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
# Set all streamlit loggers to ERROR level to suppress the ScriptRunContext warnings
logging.getLogger("streamlit").setLevel(logging.ERROR)  # Root logger for all streamlit modules
logging.getLogger("streamlit.runtime.scriptrunner").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
# app_logger.debug("Loggers at startup:", list(logging.Logger.manager.loggerDict.keys()))


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

    # Use column names derived from REQUIRED_COLUMNS_MAP for manual input DataFrame
    manual_input_columns = list(REQUIRED_COLUMNS_MAP.keys())

    defaults = {
        "page": "Input",
        "company_list": None,  # Will store list of dicts for processing
        "uploaded_file_data": None,  # Stores the uploaded file object
        "manual_input_df": pd.DataFrame(
            columns=manual_input_columns
        ),  # For data editor, using derived column names
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
        # Job selection and deletion
        "job_ids_selected_for_deletion": {},  # Dictionary of job_id -> bool for multi-select in jobs table
        "show_confirm_delete_expander": False,  # Controls visibility of deletion confirmation UI
        # Optional but recommended - to be added if needed during implementation of task 2
        # "jobs_df_for_display": None,  # Store DataFrame used for job display to map selections later
    }

    # Apply defaults if keys don't exist
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if "_jobs_loaded_from_db" not in st.session_state:
        try:
            loaded_jobs = db_utils.load_jobs_from_db(conn)
            st.session_state["active_jobs"] = merge_active_jobs_with_db(
                st.session_state.get("active_jobs", {}), loaded_jobs
            )
            app_logger.info(
                f"Successfully loaded {len(st.session_state['active_jobs'])} jobs from the database."
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
        app_logger.info("Default session state values initialized for the new user session.")

    # Specific state adjustments that might need to occur on reruns
    if st.session_state.get("input_method") == "Manual Input":
        if not isinstance(st.session_state.get("manual_input_df"), pd.DataFrame):
            # Get column names from REQUIRED_COLUMNS_MAP for consistency
            manual_input_columns = list(REQUIRED_COLUMNS_MAP.keys())
            st.session_state["manual_input_df"] = pd.DataFrame(
                columns=manual_input_columns
            )
            app_logger.info(
                "Re-initialized 'manual_input_df' as it was not a DataFrame in Manual Input mode."
            )

    return is_first_full_init


def clear_other_input(selected_method: str) -> None:
    """
    Clears the session state of the non-selected input method.

    Args:
        selected_method (str): The input method selected ("File Upload" or "Manual Input").

    Returns:
        None
    """
    # Get column names from REQUIRED_COLUMNS_MAP for consistency
    manual_input_columns = list(REQUIRED_COLUMNS_MAP.keys())

    if selected_method == "File Upload":
        # When switching to File Upload, clear manual input DataFrame
        st.session_state["manual_input_df"] = pd.DataFrame(columns=manual_input_columns)
        st.session_state["company_list"] = None  # Clear any processed list
        app_logger.info("Switched to File Upload, cleared manual input state.")
    elif selected_method == "Manual Input":
        # When switching to Manual Input, clear uploaded file data
        st.session_state["uploaded_file_data"] = None
        # Consider if file_uploader widget needs explicit reset (often handled by Streamlit's keying)
        st.session_state["company_list"] = None  # Clear any processed list
        app_logger.info("Switched to Manual Input, cleared file upload state.")
    else:
        app_logger.warning(
            f"clear_other_input called with unknown method: {selected_method}"
        )


init_session_state()


# --- Logging Handler for Streamlit ---
# The StreamlitLogHandler class has been removed and replaced by a TimedRotatingFileHandler setup below.


# Define streamlit_handler at module level, initialized to None
# This variable can hold the handler instance if needed elsewhere, though its primary role is being added to the root_logger.
streamlit_handler: Optional[logging.Handler] = None

# Add the handler to the root logger AFTER initial state setup, but only if it doesn't exist yet
root_logger = logging.getLogger()

# Use session_state to ensure the handler is added only once per session
if "_streamlit_log_handler_added" not in st.session_state:
    app_logger.debug(
        f"'_streamlit_log_handler_added' flag not in session_state. Attempting to add TimedRotatingFileHandler for streamlit_app.log. Current root_logger.handlers: {root_logger.handlers}"
    )

    # Check if a TimedRotatingFileHandler for streamlit_app.log is already present.
    # This is crucial to prevent duplicate handlers because of how streamlit reruns the entire page on click
    handler_exists = any(
        isinstance(h, logging.handlers.TimedRotatingFileHandler) and \
        getattr(h, 'baseFilename', '').endswith('streamlit_app.log')
        for h in root_logger.handlers
    )

    if not handler_exists:
        try:
            log_dir = os.path.join(project_root, "logfiles")
            os.makedirs(log_dir, exist_ok=True)
            log_file_path = os.path.join(log_dir, "streamlit_app.log")

            # Create TimedRotatingFileHandler
            rotating_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_file_path,
                when='D',  # Rotate daily
                interval=2, # Daily interval
                backupCount=1, # Number of backup files to keep. With 1+, the main log file is cleared on rotation.
                encoding='utf-8',
                delay=False # Open file immediately,
            )
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            rotating_handler.setFormatter(formatter)
            rotating_handler.setLevel(logging.INFO) # Set level for this handler

            root_logger.addHandler(rotating_handler)
            streamlit_handler = rotating_handler # Assign if needed elsewhere
            app_logger.info(
                f"TimedRotatingFileHandler for streamlit_app.log added. Log path: {log_file_path}. New root_logger.handlers: {root_logger.handlers}"
            )
        except Exception as e:
            # Log an error if there's an issue setting up the handler,
            # but don't prevent the app from starting.
            # Using a direct print here as logger might not be fully set up.
            print(
                f"Error setting up TimedRotatingFileHandler for streamlit_app.log: {e}",
                file=sys.stderr,
            )
            app_logger.error(f"Error setting up TimedRotatingFileHandler for streamlit_app.log: {e}")
    else:
        app_logger.debug(
            "TimedRotatingFileHandler for streamlit_app.log already found in root_logger.handlers. Not re-adding."
        )

    st.session_state["_streamlit_log_handler_added"] = True
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
    Cancel a running job by its ID using its PID.

    Args:
        job_id: The ID of the job to cancel

    Returns:
        bool: True if job was signalled for cancellation, False otherwise.
    """
    if job_id not in st.session_state.get("active_jobs", {}):
        app_logger.warning(f"Cancel_job: Job ID {job_id} not found in active_jobs.")
        return False

    job_model = st.session_state["active_jobs"][job_id]

    if job_model.status in ["Completed", "Error", "Failed", "Cancelled"]:
        app_logger.info(f"Cancel_job: Job {job_id} is already in terminal state ({job_model.status}). No action taken.")
        return False
    
    if job_model.pid is None:
        app_logger.warning(f"Cancel_job: Job {job_id} has no PID to cancel.")
        # If no PID, but status is running/initializing, perhaps it failed before PID was set
        if job_model.status in ["Running", "Initializing"]:
            job_model.status = "Error"
            job_model.error_message = "Cancellation attempted but PID was missing."
            job_model.end_time = time.time()
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
        return False

    # Check if process is alive using PID before attempting to kill
    is_alive, keyword = check_process_details_by_pid(job_model.pid)
    app_logger.debug(
        f"Cancel_job: Process with PID {job_model.pid} is alive: {is_alive}, keyword: {keyword}"
    )
    if is_alive:
        try:
            if "defunct" in keyword:  # terminate zombie processes
                app_logger.info(f"Cancel_job: Job {job_id} is a zombie process, force kill")
                os.kill(job_model.pid, signal.SIGKILL)  # Force kill zombie
            else:
                os.kill(job_model.pid, signal.SIGTERM)  # Send SIGTERM
                app_logger.info(
                    f"Sent SIGTERM to PID {job_model.pid} for job {job_id}."
                )
            # Give it a moment, then check again or rely on PID check to update status
            # For immediate UI feedback:
            job_model.status = "Cancelled"
            job_model.phase = "Terminated"
            job_model.end_time = time.time()
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
            app_logger.info(f"Job {job_id} marked as Cancelled in DB.")
            return True
        except ProcessLookupError:
            app_logger.warning(
                f"Cancel_job: Process with PID {job_model.pid} not found for job {job_id}. Already terminated?"
            )
            # Process already gone, update status if it wasn't already terminal
            if job_model.status not in ["Completed", "Error", "Failed", "Cancelled"]:
                job_model.status = "Completed"  # Or "Error" if appropriate
                job_model.phase = "Process not found"
                job_model.end_time = time.time()
                job_model.touch()
                db_utils.add_or_update_job_in_db(conn, job_model)
            return False
        except PermissionError:
            app_logger.error(
                f"Cancel_job: No permission to send SIGTERM to PID {job_model.pid} for job {job_id}."
            )
            job_model.error_message = "Cancellation failed: Permission denied."
            job_model.status = "Error"  # Or keep as Running if preferred
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
            return False
        except Exception as e:
            app_logger.error(
                f"Failed to cancel job {job_id} (PID {job_model.pid}): {e}"
            )
            job_model.error_message = f"Cancellation failed: {e}"
            # Optionally update status to Error
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
            return False
    else:
        app_logger.info(
            f"Cancel_job: PID {job_model.pid} for job {job_id} is not alive. Updating status if necessary."
        )
        if job_model.status not in ["Completed", "Error", "Failed", "Cancelled"]:
            job_model.status = "Completed"  # Or "Error"
            job_model.phase = "Process already ended"
            job_model.end_time = time.time()
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
        return False  # Job was not actively cancelled now, but was already not running


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

    # Helper function to safely put data in the queue
    def safe_queue_put(data):
        try:
            status_queue.put(
                data,
                timeout=5.0,  # Add a timeout to prevent indefinite blocking
            )
            return True
        except (
            BrokenPipeError,
            EOFError,
            ConnectionRefusedError,
            ConnectionResetError,
        ) as e:
            # Connection error - parent process likely terminated
            app_logger.warning(
                f"Failed to send status update: {e}. Parent process may have terminated."
            )
            return False
        except Exception as e:
            app_logger.warning(f"Unexpected error while sending status update: {e}")
            return False

    # Send initial status, including the log_file_path
    status_data = {
        "status": "Running",
        "progress": 0,
        "phase": "Initializing",
        "pipeline_log_file_path": log_file_path,  # Add log file path here
    }
    if job_id:
        status_data["job_id"] = job_id
    safe_queue_put(status_data)
    input_csv_path_to_delete = config.get("input_csv")  # Store path for cleanup
    try:
        app_logger.info(f"Pipeline process started with config: {config}")
        root_logger.info(
            f"PIPELINE_PROCESS_STARTED: Pipeline process started for job {job_id}"
        )

        status_data = {
            "status": "Running",
            "progress": 10,
            "phase": "Starting Pipeline",
        }
        if job_id:
            status_data["job_id"] = job_id
        safe_queue_put(status_data)

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
        safe_queue_put(status_data)

        root_logger.info(
            f"PIPELINE_PROCESS_COMPLETED: Pipeline process completed successfully for job {job_id}, output at: {final_output}"
        )

    except Exception as e:
        error_msg = str(e)
        root_logger.error(
            f"PIPELINE_PROCESS_ERROR: Pipeline process failed for job {job_id}: {error_msg}"
        )
        
        status_data = {
            "status": "Error",
            "progress": 0,
            "phase": "Failed",
            "error": error_msg,
        }
        if job_id:
            status_data["job_id"] = job_id
        safe_queue_put(status_data)
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
        # Log final state to file to ensure it's captured even if queue communication fails
        app_logger.info(
            f"PIPELINE_PROCESS_EXITING: Pipeline process for job {job_id} exiting"
        )


def process_queue_messages():
    """
    Process messages from the status queues for all active jobs,
    updating the Streamlit session state.
    This should be called on each Streamlit rerun.
    This is used to manage in-memory jobs.
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
                        # Set end_time when job reaches terminal state
                        # Check for terminal status and ensure end_time is valid (not None, nan, or invalid)
                        if job_model.status in ["Completed", "Error", "Failed", "Cancelled"] and (job_model.end_time is None or pd.isna(job_model.end_time) or job_model.end_time <= 0):
                            job_model.end_time = time.time()
                            app_logger.info(f"Job {job_id} end_time set to: {job_model.end_time}")

                    if "progress" in status_update:
                        job_model.progress = status_update["progress"]
                        app_logger.debug(
                            f"Job {job_id} progress updated to: {job_model.progress}%"
                        )

                    if "phase" in status_update:
                        job_model.phase = status_update["phase"]
                        app_logger.debug(
                            f"Job {job_id} phase updated to: {job_model.phase}"
                        )

                    if "pipeline_log_file_path" in status_update:
                        job_model.pipeline_log_file_path = status_update[
                            "pipeline_log_file_path"
                        ]
                        app_logger.debug(
                            f"Job {job_id} log file path set to: {job_model.pipeline_log_file_path}"
                        )

                    if "output_final_file_path" in status_update:
                        job_model.output_final_file_path = status_update[
                            "output_final_file_path"
                        ]
                        app_logger.debug(
                            f"Job {job_id} output file path set to: {job_model.output_final_file_path}"
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


def process_data():
    """Processes the data from the selected input method."""
    data_to_process = None
    temp_csv_path = None

    # Get data from the selected input method
    if (
        st.session_state["input_method"] == "File Upload"
        and st.session_state["uploaded_file_data"]
    ):
        uploaded_file = st.session_state["uploaded_file_data"]
        app_logger.info(f"Processing uploaded file: {uploaded_file.name}")
        try:
            # Read the uploaded file into data_to_process
            # Explicitly use io.BytesIO for robustness with pd.read_csv
            file_content_bytes = uploaded_file.getvalue()
            bytes_io_object = io.BytesIO(file_content_bytes)
            data_to_process = pd.read_csv(bytes_io_object).to_dict(orient="records")
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
        temp_csv_path = None

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

            # Create a single temporary CSV file for the pipeline
            # regardless of input method
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                pd.DataFrame(data_to_process).to_csv(temp_file.name, index=False)
                temp_csv_path = temp_file.name
                app_logger.info(
                    f"Consolidated temporary input data CSV created at {temp_csv_path}"
                )

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
                pid=None,
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
            job_model.pid = p.pid  # Store the PID in the job model
            job_model.status = "Running"
            job_model.phase = "Starting Pipeline"
            job_model.progress = 5
            job_model.touch()

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
            # Clean up temporary file if job fails to start
            if temp_csv_path and os.path.exists(temp_csv_path):
                try:
                    os.unlink(temp_csv_path)
                    app_logger.info(
                        f"Cleaned up temporary CSV file after error: {temp_csv_path}"
                    )
                except Exception as cleanup_error:
                    app_logger.warning(
                        f"Failed to clean up temporary CSV after error: {cleanup_error}"
                    )

            st.error(f"Failed to start pipeline: {e}")
            app_logger.error(f"Failed to start pipeline: {e}", exc_info=True)

    else:
        st.warning("No data to process. Please check your input.")
        app_logger.warning("No data to process after preparation.")


# --- Sidebar Navigation ---
def handle_navigation():
    """Callback function to update the page state."""
    st.session_state["page"] = st.session_state["navigation_choice"]

st.sidebar.title("Navigation")
page_options = [
    "Input", 
    # "Configuration", 
    "Monitoring", "Output"]

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
        st.header("Input Data")
        display_input_section(
            process_data_func=process_data,
            validate_columns_func=validate_columns,
            req_cols_map=REQUIRED_COLUMNS_MAP,
            clear_other_input_func_from_app=clear_other_input,  # Pass the function
        )
    elif page == "Configuration":
        display_config_section()
    elif page == "Monitoring":
        st.header("Monitoring")
        display_monitoring_section(
            db_connection=conn,
            cancel_job_callback=cancel_job,  # Pass the existing function
            process_queue_messages_callback=process_queue_messages,  # Pass the existing function
            check_pid_callback=check_process_details_by_pid,  # Pass the imported function
        )
    elif page == "Output":
        st.header("Pipeline Artifacts")
        display_output_section(conn)

    app_logger.info(f"Displayed page: {page}")
