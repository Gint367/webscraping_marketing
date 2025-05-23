import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import streamlit as st
from streamlit.connections import SQLConnection

import streamlit_app.utils.db_utils as db_utils
from streamlit_app.models.job_data_model import JobDataModel
from streamlit_app.utils.job_utils import (
    delete_job_and_artifacts,
    merge_active_jobs_with_db,
)

# Get a logger that is a child of the main app logger configured in app.py
monitoring_logger = logging.getLogger("streamlit_app_main.monitoring")

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
        monitoring_logger.warning(
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

    return min(progress, 1.0)


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
        monitoring_logger.error(f"Error parsing progress log line '{log_line}': {e}")
        return None


def update_selected_job_progress_from_log(
    job_model: JobDataModel,
    conn,
    PHASE_FORMATS: Dict[str, Any],
    PHASE_ORDER: list[str],
    calculate_progress_from_phase: Callable[..., float],
) -> bool:
    """
    Reads new lines from a job's log file, parses progress, phase, errors,
    and completion status, updates the job model, and saves it to the database.

    Args:
        job_model (JobDataModel): The job data model to update.
        conn: The database connection object.
        PHASE_FORMATS (Dict[str, Any]): Configuration for phase descriptions.
        PHASE_ORDER (list[str]): Order of phases for progress calculation.
        calculate_progress_from_phase (Callable[..., float]): Function to calculate progress.

    Returns:
        bool: True if an update occurred, False otherwise.
    """
    if not job_model.pipeline_log_file_path or not os.path.exists(
        job_model.pipeline_log_file_path
    ):
        return False

    updated = False
    log_changed_job_state = False  # Flag if log parsing changes terminal state
    last_read_position = st.session_state.get("log_file_positions", {}).get(
        job_model.id, 0
    )

    try:
        with open(job_model.pipeline_log_file_path, "r", encoding="utf-8") as f:
            f.seek(last_read_position)
            new_lines = f.readlines()
            current_position = f.tell()
            if new_lines:  # Only update position if new lines were read
                st.session_state.setdefault("log_file_positions", {})[job_model.id] = (
                    current_position
                )

        if not new_lines:
            return False

        latest_progress_info = None
        # Iterate through new lines to find specific markers
        for line in new_lines:  # Process in chronological order
            line_strip = line.strip()
            if "PROGRESS:" in line_strip:
                progress_segment = line_strip[line_strip.find("PROGRESS:") :]
                parsed_info = parse_progress_log_line(progress_segment)
                if parsed_info:
                    latest_progress_info = (
                        parsed_info  # Keep the latest one found in this batch
                    )

            final_output_marker = "FINAL_OUTPUT_PATH:"
            if final_output_marker in line_strip:
                try:
                    path = line_strip.split(final_output_marker, 1)[1].strip()
                    if job_model.output_final_file_path != path:
                        job_model.output_final_file_path = path
                        monitoring_logger.info(
                            f"Job {job_model.id} output file path set from log: {path}"
                        )
                        updated = True
                except IndexError:
                    monitoring_logger.warning(
                        f"Could not parse payload for {final_output_marker} in line: {line_strip}"
                    )

            error_marker = "PIPELINE_PROCESS_ERROR:"
            if error_marker in line_strip:
                try:
                    error_msg = line_strip.split(error_marker, 1)[1].strip()
                    if (
                        job_model.error_message != error_msg
                        or job_model.status != "Error"
                    ):
                        job_model.error_message = error_msg
                        job_model.status = "Error"
                        job_model.phase = "Failed (from log)"
                        # Correctly set end_time, handling None, NaN, or already set cases
                        if isinstance(job_model.end_time, float) and math.isnan(job_model.end_time):
                            job_model.end_time = time.time() # Update if NaN
                        else:
                            job_model.end_time = job_model.end_time or time.time()
                        monitoring_logger.error(
                            f"Job {job_model.id} error set from log: {error_msg}"
                        )
                        updated = True
                        log_changed_job_state = True
                except IndexError:
                    monitoring_logger.warning(
                        f"Could not parse payload for {error_marker} in line: {line_strip}"
                    )

            completed_marker = "PIPELINE_PROCESS_COMPLETED"
            if completed_marker in line_strip:
                if job_model.status != "Completed":
                    job_model.status = "Completed"
                    job_model.phase = "Finished (from log)"
                    job_model.progress = 100  # Ensure progress is 100%
                    # Correctly set end_time, handling None, NaN, or already set cases
                    if isinstance(job_model.end_time, float) and math.isnan(job_model.end_time):
                        job_model.end_time = time.time() # Update if NaN
                    else:
                        job_model.end_time = job_model.end_time or time.time() # Original logic for None, 0.0, or already valid
                    monitoring_logger.info(
                        f"Job {job_model.id} status COMPLETED from log."
                    )
                    updated = True
                    log_changed_job_state = True

            # Marker for pipeline exiting (Note: this log might go to a different file)
            exiting_marker = "PIPELINE_PROCESS_EXITING:"
            if exiting_marker in line_strip:
                if job_model.status not in [
                    "Completed",
                    "Error",
                    "Failed",
                    "Cancelled",
                ]:
                    monitoring_logger.info(
                        f"Job {job_model.id} log indicates process exiting. PID check will confirm."
                    )

        if (
            latest_progress_info and not log_changed_job_state
        ):  # Don't update progress if job already marked completed/error by logs
            main_phase_key, sub_phase_key, details = latest_progress_info
            descriptive_phase = PHASE_FORMATS.get(main_phase_key, {}).get(sub_phase_key)

            if descriptive_phase:
                current_status = (
                    job_model.status
                    if hasattr(job_model, "status") and job_model.status
                    else "Running"
                )
                new_progress_float = calculate_progress_from_phase(
                    descriptive_phase,
                    PHASE_FORMATS,
                    PHASE_ORDER,
                    current_status,
                    base_progress=0.05,
                )
                new_progress_int = int(new_progress_float * 100)

                if (
                    job_model.phase != descriptive_phase
                    or job_model.progress != new_progress_int
                ):
                    job_model.phase = descriptive_phase
                    job_model.progress = new_progress_int
                    monitoring_logger.info(
                        f"Job {job_model.id} progress updated from log: Phase='{descriptive_phase}', Progress={new_progress_int}%"
                    )
                    updated = True
            else:
                monitoring_logger.warning(
                    f"Could not find descriptive phase for {main_phase_key}.{sub_phase_key} in PHASE_FORMATS for job {job_model.id}"
                )

        if updated:
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)

    except FileNotFoundError:
        monitoring_logger.warning(
            f"Log file not found for job {job_model.id} at {job_model.pipeline_log_file_path} during progress update."
        )
        if job_model.status in [
            "Running",
            "Initializing",
        ]:  # If log file disappears for a running job
            job_model.status = "Error"
            job_model.error_message = "Log file disappeared."
            job_model.phase = "Log file missing"
            job_model.end_time = time.time()
            job_model.touch()
            db_utils.add_or_update_job_in_db(conn, job_model)
            updated = False
    except Exception as e:
        monitoring_logger.error(
            f"Error updating job progress from log for job {job_model.id}: {e}",
            exc_info=True,
        )
    return updated


def display_monitoring_section(
    db_connection: SQLConnection,
    cancel_job_callback: Callable[[str], bool],
    process_queue_messages_callback: Callable[[], None],
    check_pid_callback: Callable[[int], Tuple[bool, str]],
):
    """Displays the job monitoring and log output."""
    st.write("Track the progress of the scraping and enrichment processes.")

    # Determine the actual run_every interval for fragments on this page
    # This relies on your main app setting st.session_state.page
    is_monitoring_page_active = st.session_state.get('page') == 'Monitoring' # Adjust 'Monitoring' if your page name is different
    
    # Check if there are any active jobs
    has_active_jobs = bool(st.session_state.get("active_jobs", {}))
    
    actual_run_every_interval = None
    if st.session_state.get("auto_refresh_enabled", True) and \
       is_monitoring_page_active and \
       has_active_jobs:  # Only auto-refresh if there are active jobs
        actual_run_every_interval = st.session_state.get("refresh_interval", 3.0)

    # --- Helper: Non-blocking PID check for jobs ---

    def _update_job_statuses_with_pid_check(jobs_dict, min_interval=1.0):
        """
        Checks the aliveness of jobs with status 'Running' or 'Initializing' that have a PID but no attached process object.
        Uses a non-blocking, debounced approach to avoid redundant checks and UI blocking.
        If the process is alive, keeps the job status as 'Running'.
        If the process is not alive, sets the job status to 'Completed' and updates the end time.

        Args:
            jobs_dict (dict): Dictionary of job_id to job_data objects.
            min_interval (float): Minimum interval in seconds between PID checks.

        Returns:
            None
        """
        monitoring_logger.info("checking job PIDs for aliveness...")
        now = time.time()
        last_check = st.session_state.get("_last_pid_check_time", 0)
        if st.session_state.get("_pid_check_in_progress", False):
            monitoring_logger.info("PID check skipped: already in progress")
            return  # Already running, skip
        if now - last_check < min_interval:
            monitoring_logger.info(
                f"PID check skipped: only {now - last_check:.2f}s since last check (min_interval={min_interval})"
            )
            return  # Too soon since last check, skip

        st.session_state["_pid_check_in_progress"] = True
        try:
            # Find running/initializing jobs that need PID checks
            jobs_to_check = []
            for job_id, job_data in jobs_dict.items():
                # Only check jobs that are still in active states
                if getattr(job_data, "status", None) not in ("Running", "Initializing"):
                    continue

                current_pid = getattr(job_data, "pid", None)
                if current_pid is not None:
                    jobs_to_check.append((job_id, job_data, current_pid))
                    monitoring_logger.debug(
                        f"Job {job_id}: Queued for PID check (PID {current_pid})"
                    )
                else:
                    # If job is Running/Initializing but has no PID, it's an anomaly.
                    # Could be a very early error before PID was set, or data inconsistency.
                    monitoring_logger.warning(
                        f"Job {job_id} is {job_data.status} but has no PID. Marking as Error."
                    )
                    job_data.status = "Error"
                    job_data.phase = "Missing PID"
                    job_data.error_message = "Job was in an active state without a PID."
                    job_data.end_time = time.time()
                    job_data.touch()
                    # Consider db_utils.add_or_update_job_in_db(conn, job_data) here if this function modifies db directly
                    # For now, it modifies the jobs_dict which is then set to session_state

            monitoring_logger.info(f"Found {len(jobs_to_check)} jobs to check PIDs")

            if not jobs_to_check:  # No jobs to check, release lock and update time
                st.session_state["_last_pid_check_time"] = now
                st.session_state["_pid_check_in_progress"] = (
                    False  # Explicitly release if no jobs
                )
                return

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_job_id = {
                    executor.submit(check_pid_callback, pid_to_check): j_id
                    for j_id, _, pid_to_check in jobs_to_check  # Iterate through the prepared list
                }
                for future in as_completed(future_to_job_id):
                    job_id_from_future = future_to_job_id[future]
                    try:
                        is_alive, details = future.result()
                        monitoring_logger.info(
                            f"PID check for job {job_id_from_future}: is_alive={is_alive}, details='{details}'"
                        )
                        # Ensure job_id_from_future is valid in jobs_dict
                        if job_id_from_future in jobs_dict:
                            target_job_model = jobs_dict[job_id_from_future]
                            if is_alive:
                                # Check if the process is a zombie (defunct)
                                if details and "defunct" in details.lower():
                                    monitoring_logger.warning(
                                        f"Job {job_id_from_future} (PID {target_job_model.pid}) is a defunct (zombie) process. Marking as Completed."
                                    )
                                    target_job_model.status = "Completed"
                                    target_job_model.phase = "Process finished"
                                    target_job_model.end_time = time.time()
                                    target_job_model.touch()
                                    db_utils.add_or_update_job_in_db(
                                        db_connection, target_job_model
                                    )  # If updating DB directly
                                elif (
                                    target_job_model.status != "Running"
                                ):  # If it was Initializing
                                    target_job_model.status = "Running"
                                    target_job_model.touch()
                                    # db_utils.add_or_update_job_in_db(conn, target_job_model) # If updating DB directly
                            else:
                                # If process is not alive, and status is still Running/Initializing,
                                # it means it completed/failed/was killed without explicit update through logs yet.
                                # Log parsing should ideally set the final state.
                                # This PID check acts as a fallback.
                                if target_job_model.status in [
                                    "Running",
                                    "Initializing",
                                ]:
                                    target_job_model.status = (
                                        "Completed"  # Default to Completed
                                    )
                                    target_job_model.phase = (
                                        "Process ended (detected by PID check)"
                                    )
                                    target_job_model.end_time = time.time()
                                    target_job_model.touch()
                                    db_utils.add_or_update_job_in_db(
                                        db_connection, target_job_model
                                    )  # If updating DB directly
                                    monitoring_logger.info(
                                        f"Job {job_id_from_future} (PID {target_job_model.pid}) detected as not alive. Marked Completed."
                                    )
                        else:
                            monitoring_logger.warning(
                                f"Job ID {job_id_from_future} from future not found in jobs_dict during PID check."
                            )
                    except Exception as e:
                        # Ensure job_id_from_future is valid in jobs_dict before trying to update
                        if job_id_from_future in jobs_dict:
                            jobs_dict[job_id_from_future].status = "Error"
                            jobs_dict[job_id_from_future].phase = "PID check failed"
                            jobs_dict[
                                job_id_from_future
                            ].error_message = f"PID check error: {e}"
                            jobs_dict[job_id_from_future].touch()
                            # db_utils.add_or_update_job_in_db(conn, jobs_dict[job_id_from_future]) # If updating DB directly
                        monitoring_logger.warning(
                            f"PID check failed for job {job_id_from_future}: {e}"
                        )
            st.session_state["_last_pid_check_time"] = now
        finally:
            st.session_state["_pid_check_in_progress"] = False

    # Always reload jobs from DB to ensure up-to-date info
    try:
        loaded_jobs = db_utils.load_jobs_from_db(db_connection)
        merged_jobs = merge_active_jobs_with_db(
            st.session_state.get("active_jobs", {}), loaded_jobs
        )
        # Run non-blocking PID check for jobs that are "Running" or "Initializing" and have a PID but no process
        _update_job_statuses_with_pid_check(merged_jobs)
        st.session_state["active_jobs"] = merged_jobs
    except Exception as e:
        monitoring_logger.error(f"Failed to reload jobs from DB: {e}")
    # Initial queue processing is still needed outside the fragment
    process_queue_messages_callback()

    # --- Jobs Table (auto-refreshing fragment) ---
    @st.fragment(run_every=actual_run_every_interval)
    def jobs_table_fragment():
        """
        Renders a table displaying all active jobs in the application.
        This function:
        1. Displays a "Jobs" subheader
        2. Processes any pending queue messages
        3. Shows an informational message if no jobs exist
        4. For existing jobs, creates a table with the following information:
           - Job ID
           - Status
           - Progress (as a progress bar)
           - Current processing phase
           - Input File (name from file_info)
           - Start time (formatted as YYYY-MM-DD HH:MM:SS)
           - End time (shows "Running" for active jobs)
        The table supports multi-row selection for job deletion and includes
        search functionality. Jobs data is stored in session state for reference
        when handling selection events.
        Returns:
            None: This function renders UI components directly using Streamlit
        """
        st.subheader("Jobs")
        process_queue_messages_callback()
        active_jobs = st.session_state.get("active_jobs", {})
        if not active_jobs:
            st.info(
                "No jobs have been run yet. Start a new job from the Input section."
            )
        else:
            job_rows = []
            for job_id, job_data in active_jobs.items():
                # Corrected start_time logic
                start_val = job_data.start_time
                if start_val is not None and not math.isnan(start_val):
                    start_time_str = time.strftime(
                        "%Y-%m-%d %H:%M:%S", 
                        time.localtime(start_val)
                    )
                else:
                    start_time_str = "N/A"
                
                end_val = job_data.end_time
                if end_val is not None and not math.isnan(end_val):
                    end_time_str = time.strftime(
                        "%Y-%m-%d %H:%M:%S", 
                        time.localtime(end_val)
                    )
                else:
                    end_time_str = "Running"
                
                # Get input file name from file_info
                file_info = getattr(job_data, "file_info", {})
                input_file = file_info.get("name", "N/A")
                
                job_rows.append({
                    "job_id": job_id,  # Include job_id in the DataFrame
                    "Status": job_data.status,
                    "Progress": job_data.progress if job_data.progress is not None else 0,  # Store as integer without % sign
                    "Phase": job_data.phase or "N/A",
                    "Input File": input_file,
                    "Start Time": start_time_str,
                    "End Time": end_time_str,
                })
                
            # Create DataFrame with job data
            jobs_df = pd.DataFrame(job_rows)
            
            # Store the DataFrame in session state for later reference when processing selections
            st.session_state["jobs_df_for_display"] = jobs_df
            
            # Add a note about selection behavior
            st.caption("Select rows to delete. Note: Sorting will reset selections. Use search to filter before selecting.")
            
            # Configure columns for better readability
            column_config = {
                "job_id": st.column_config.TextColumn(
                    "Job ID",
                    help="Unique identifier for the job",
                    width="small"
                ),
                "Status": st.column_config.TextColumn(
                    "Status",
                    help="Current status of the job",
                    width="small"
                ),
                "Progress": st.column_config.ProgressColumn(
                    "Progress",
                    help="Job completion percentage",
                    format="%s%%",
                    min_value=0,
                    max_value=100
                ),
                "Phase": st.column_config.TextColumn(
                    "Current Phase",
                    help="Current processing phase",
                    width="medium"
                ),
                "Input File": st.column_config.TextColumn(
                    "Input File",
                    help="Name of the input file or data source",
                    width="medium"
                ),
                "Start Time": st.column_config.DatetimeColumn(
                    "Started At",
                    help="When the job was started",
                    format="YYYY-MM-DD HH:mm:ss",
                    width="small"
                ),
                "End Time": st.column_config.DatetimeColumn(
                    "Completed At", 
                    help="When the job was completed",
                    format="YYYY-MM-DD HH:mm:ss",
                    width="small"
                ),
            }
            
            # Display the DataFrame with selection capabilities
            st.dataframe(
                jobs_df,
                key="jobs_dataframe_selector",  # Unique key for accessing selection state
                on_select="rerun",              # Re-render app when selection changes
                selection_mode=["multi-row"],   # Allow selecting multiple rows
                hide_index=True,                # Hide index for cleaner display
                column_config=column_config     # Apply column configuration for better readability
            )

    jobs_table_fragment()

    # --- Delete Jobs Button ---
    # This is placed outside the fragment so it doesn't refresh with the table
    deletion_status_container = st.container()  # Container for persistent deletion status messages

    # Only show delete button if there are jobs to delete
    if st.session_state.get("active_jobs", {}):
        if st.button("Delete Selected Jobs", key="delete_selected_jobs_button", icon=":material/delete:"):
            # Access the current selection state from session state
            if "jobs_dataframe_selector" in st.session_state and hasattr(st.session_state["jobs_dataframe_selector"], "selection") and st.session_state["jobs_dataframe_selector"].selection.get("rows", []):
                # Get the list of integer indices representing selected rows
                selected_row_indices = st.session_state["jobs_dataframe_selector"].selection["rows"]

                # Retrieve the DataFrame that was originally supplied to st.dataframe
                original_jobs_df = st.session_state.get("jobs_df_for_display")
                
                if original_jobs_df is not None and not original_jobs_df.empty and len(selected_row_indices) > 0:
                    try:
                        # Extract job_ids from selected rows using row indices
                        selected_job_ids = original_jobs_df.iloc[selected_row_indices]["job_id"].tolist()
                        
                        # Store selected job_ids in session state for confirmation/deletion logic
                        st.session_state["job_ids_selected_for_deletion"] = selected_job_ids
                        
                        # Show confirmation dialog
                        st.session_state["show_confirm_delete_expander"] = True
                        
                        # Log the operation for monitoring
                        monitoring_logger.info(f"Selected {len(selected_job_ids)} jobs for deletion: {selected_job_ids}")
                    except Exception as e:
                        st.error(f"Error processing selected rows: {e}")
                        monitoring_logger.error(f"Failed to process selected rows for deletion: {e}", exc_info=True)
                else:
                    st.warning("No jobs selected or job data not available.")
            else:
                st.warning("No jobs selected. Please select one or more rows from the table.")
    
    # Display persistent deletion status messages if they exist
    if "deletion_success_count" in st.session_state and st.session_state["deletion_success_count"] > 0:
        deletion_status_container.success(
            f"Successfully deleted {st.session_state['deletion_success_count']} job"
            f"{'s' if st.session_state['deletion_success_count'] != 1 else ''}."
        )
        # Clear the message after displaying once
        st.session_state.pop("deletion_success_count", None)
        
    if "deletion_error_count" in st.session_state and st.session_state["deletion_error_count"] > 0:
        deletion_status_container.error(
            f"Failed to delete {st.session_state['deletion_error_count']} job"
            f"{'s' if st.session_state['deletion_error_count'] != 1 else ''}. See logs for details."
        )
        # Clear the message after displaying once
        st.session_state.pop("deletion_error_count", None)
    
    # --- Delete Confirmation UI ---
    if st.session_state.get("show_confirm_delete_expander", False):
        with st.expander("Confirm Deletion", expanded=True):
            selected_jobs = st.session_state.get("job_ids_selected_for_deletion", [])
            num_selected = len(selected_jobs)
            
            st.warning(f"You are about to delete {num_selected} job{'s' if num_selected != 1 else ''} and all associated artifacts. This action cannot be undone.")
            st.write(f"Selected job IDs: {', '.join(selected_jobs)}")
            
            col1, col2 = st.columns(2, gap="small")
            
            with col1:
                if st.button("Yes, Delete These Jobs", key="confirm_delete_button", type="primary"):
                    success_count = 0
                    error_count = 0
                    for job_id in selected_jobs:
                        try:
                            # Call the delete_job_and_artifacts function to perform the deletion
                            result = delete_job_and_artifacts(db_connection, job_id, st.session_state.get("active_jobs", {}))
                            if result:
                                success_count += 1
                                
                            else:
                                error_count += 1
                                
                        except Exception as e:
                            error_count += 1
                            monitoring_logger.error(f"Error while deleting job {job_id}: {e}")
                    
                    # Store results in session state for display outside the expander
                    if success_count > 0:
                        st.session_state["deletion_success_count"] = success_count
                    if error_count > 0:
                        st.session_state["deletion_error_count"] = error_count
                    
                    # Clean up session state
                    st.session_state["job_ids_selected_for_deletion"] = []
                    st.session_state["show_confirm_delete_expander"] = False
                    
                    # Force UI refresh
                    st.rerun()
            
            with col2:
                if st.button("Cancel", key="cancel_delete_button"):
                    st.session_state["job_ids_selected_for_deletion"] = []
                    st.session_state["show_confirm_delete_expander"] = False
                    st.rerun()
    
    # --- Job Selection and Cancel Button (separate container, not auto-refreshing) ---
    with st.container():
        active_jobs = st.session_state.get("active_jobs", {})
        
        # --- Sort jobs by start_time (latest first) ---
        # Sort once and reuse
        sorted_jobs_list = sorted(
            active_jobs.items(),
            key=lambda item: item[1].start_time, # Direct access to start_time
            reverse=True,
        )
        
        sorted_job_ids = [job_id for job_id, _ in sorted_jobs_list]

        # --- Create a selectbox for job selection ---
        job_id_to_label = {
            job_id: f"{job_id} - {active_jobs[job_id].status}" # Direct access to status
            for job_id in sorted_job_ids
        }

        # Determine the initial value for selected_job_id if not already set or invalid.
        # This logic runs BEFORE the selectbox is instantiated.
        current_selection = st.session_state.get("selected_job_id")

        if not active_jobs:
            # If there are no jobs, ensure selected_job_id is None
            st.session_state["selected_job_id"] = None
        elif (
            not current_selection or current_selection not in active_jobs
        ):
            # If no valid job is selected and jobs exist, select the most recent one.
            # Use the already sorted list
            if sorted_job_ids: # Check if sorted_job_ids is not empty
                st.session_state["selected_job_id"] = sorted_job_ids[0]
            else: # Should not happen if active_jobs is not empty, but as a safeguard
                st.session_state["selected_job_id"] = None
        # If current_selection is valid and in active_jobs, it remains unchanged.

        # Determine selectbox state and options
        is_disabled = not bool(sorted_job_ids)

        def format_placeholder(val: str) -> str:
            """Returns the placeholder string itself."""
            return val

        def format_job_id_with_status(job_id: str) -> str:
            """Formats the job ID with its status for display in the selectbox."""
            return job_id_to_label.get(job_id, str(job_id))
        
        if is_disabled:
            # When no jobs, selected_job_id is None (set by logic above).
            # The selectbox will show this placeholder, disabled.
            # st.selectbox requires options not to be empty.
            options_for_selectbox = ["No jobs available"]
            # format_func for the placeholder string.
            format_func_selectbox = format_placeholder
        else:
            options_for_selectbox = sorted_job_ids
            format_func_selectbox = format_job_id_with_status

        # The st.selectbox will now use the value from st.session_state.selected_job_id
        # as its current selection due to the `key`.
        st.selectbox(
            label="Select Job:",
            options=options_for_selectbox,
            format_func=format_func_selectbox,
            key="selected_job_id", # This key drives the selection and persistence
            disabled=is_disabled,
            # If selected_job_id (key's value) is None and options are ["No jobs available"],
            # Streamlit will default to selecting the first option (index 0).
            # If selected_job_id is a valid job_id, it will be selected from sorted_job_ids.
        )

        # Read the selected job ID from session state for the cancel button logic
        selected_job_id_for_cancel = st.session_state.get("selected_job_id")

        if st.button("Cancel Selected Job", key="cancel_job_btn", icon=":material/stop_circle:"):
            if selected_job_id_for_cancel:
                if cancel_job_callback(selected_job_id_for_cancel):
                    st.toast(f"Job {selected_job_id_for_cancel} cancelled.")
                else:
                    st.error(f"Could not cancel job {selected_job_id_for_cancel}.")
            else:
                st.warning("No job selected to cancel.")

    @st.fragment(run_every=actual_run_every_interval)
    def display_status_info():
        """
        Displays detailed information about the currently selected job's status in the Streamlit UI.
        
        This function:
        1. Processes queue messages to ensure data is fresh
        2. Retrieves the selected job ID from session state
        3. Updates job progress by parsing log files if job is running
        4. Displays job status with color coding (running, completed, error, etc.)
        5. Shows the current execution phase
        6. Renders a progress bar indicating job completion percentage
        7. Displays any error messages if present
        
        The function handles different job states appropriately:
        - Running/Initializing: Shows current progress
        - Completed/Error/Failed/Cancelled: Shows full progress (100%)
        - Other states: Shows current tracked progress
        
        If no job is selected, displays an informational message prompting user selection.
        """
        # Process messages from queues inside fragment to ensure fresh data
        process_queue_messages_callback()

        # Show details for selected job (value is read from session state)
        selected_job_id = st.session_state.get("selected_job_id")
        active_jobs = st.session_state.get("active_jobs", {})

        if selected_job_id and selected_job_id in active_jobs:
            job_data = active_jobs[selected_job_id]

            # Update the selected job's progress by parsing its log file
            if (
                job_data
                and job_data.status in ["Running", "Initializing"]
                and job_data.pipeline_log_file_path
            ):
                update_selected_job_progress_from_log(
                    job_model=job_data,
                    conn=db_connection,
                    PHASE_FORMATS=PHASE_FORMATS,
                    PHASE_ORDER=PHASE_ORDER,
                    calculate_progress_from_phase=calculate_progress_from_phase,
                )

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

            if current_phase:  # current_phase is already job_data.phase
                st.markdown(f"**Latest Phase:** {current_phase}")

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
    @st.fragment(run_every=actual_run_every_interval)
    def display_logs():
        # Process queue messages to ensure logs are up to date
        process_queue_messages_callback()

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
