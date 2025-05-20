Okay, here's a breakdown of the tasks to implement reading job status (phase and progress) from log files:

# **Phase 1: Core Logic for Log Parsing and State Update**

**Task 1.1: Initialize Session State and Create Helper Function for Parsing Log Lines**

1.  **Modify `init_session_state`**:
    *   Add `log_file_positions`: `dict` to `st.session_state` to store the last read byte offset for each job's log file. Initialize it as an empty dictionary.
2.  **Create `parse_progress_log_line` function**:
    *   **Input**: `log_line: str`
    *   **Output**: `Optional[Tuple[str, str, str]]` (e.g., `(main_phase, step, details_str)` or `None` if not a progress line or malformed).
    *   **Logic**:
        *   Check if the line starts with `"PROGRESS:"`.
        *   If yes, parse the line. A potential format could be `"PROGRESS: main_phase.step - Optional details"`. For example, `"PROGRESS: webcrawl.extract_llm - Processing file X"`.
        *   Extract `main_phase`, `step`, and any optional details.
        *   Return the extracted parts or `None`.
    *   Add comprehensive docstrings and type hints.
    *   Include basic error handling for parsing.

```python
// ...existing code...
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
        "log_file_positions": {}, # Stores last read position for job log files
    }

    # Apply defaults if keys don't exist
// ...existing code...
def parse_progress_log_line(log_line: str) -> Optional[tuple[str, str, str]]:
    """
    Parses a log line to extract progress information.
    Expected format: "PROGRESS: main_phase.step - Optional details"
                     or "PROGRESS: main_phase.step"

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
        # Remove "PROGRESS: " part
        content = log_line[len("PROGRESS:") :].strip()

        # Split by " - " to separate phase.step from details
        parts = content.split(" - ", 1)
        phase_step_part = parts[0]
        details = parts[1] if len(parts) > 1 else ""

        # Split phase_step_part by "."
        phase_step_split = phase_step_part.split(".", 1)
        if len(phase_step_split) != 2:
            app_logger.warning(
                f"Malformed PROGRESS line (phase.step): {log_line}"
            )
            return None

        main_phase = phase_step_split[0].strip()
        step = phase_step_split[1].strip()

        if not main_phase or not step:
            app_logger.warning(
                f"Malformed PROGRESS line (empty phase or step): {log_line}"
            )
            return None

        return main_phase, step, details.strip()
    except Exception as e:
        app_logger.error(f"Error parsing progress log line '{log_line}': {e}")
        return None


# Function to monitor queues and update session state
// ...existing code...
```
*   **Task 1.3: Implement `update_selected_job_progress_from_log` Function**
    *   **Objective**: Read new lines from a job's log file, parse the latest progress information, update the job model, and save to the database.
    *   **Signature**: `def update_selected_job_progress_from_log(job_model: JobDataModel, conn) -> bool:` (returns `True` if an update occurred, `False` otherwise).
    *   **Steps**:
        1.  Check if `job_model.pipeline_log_file_path` is valid and the file exists.
        2.  Retrieve the last read position for `job_model.id` from `st.session_state["log_file_positions"]` (default to `0` if not found).
        3.  Open the log file, seek to the last read position.
        4.  Read any new lines from the file.
        5.  Update `st.session_state["log_file_positions"][job_model.id]` with the new file pointer position (current end of the file).
        6.  Iterate through the *newly read lines in reverse* to find the *last* line that starts with `"PROGRESS:"`.
        7.  If a "PROGRESS:" line is found:
            *   Parse it using `parse_progress_log_line`.
            *   If parsing is successful (returns `main_phase_key`, `sub_phase_key`, `details`):
                *   Attempt to get the descriptive phase string: `descriptive_phase = PHASE_FORMATS.get(main_phase_key, {}).get(sub_phase_key)`.
                *   If `descriptive_phase` is found:
                    *   Calculate `new_progress_float` using `calculate_progress_from_phase(descriptive_phase, PHASE_FORMATS, PHASE_ORDER, job_model.status, base_progress=0.05)`. (The `base_progress` of 0.05 assumes some initial progress before detailed log phases begin).
                    *   Update `job_model.phase = descriptive_phase`.
                    *   Update `job_model.progress = int(new_progress_float * 100)`.
                    *   Call `db_utils.add_or_update_job_in_db(conn, job_model)`.
                    *   Log the update and return `True`.
        8.  If no relevant progress line is found or no update is made, return `False`.
        9.  Include `try-except` blocks for file operations and parsing, logging any errors.

# **Phase 2: Integration with UI and Existing Logic**

*   **Task 2.1: Modify `display_monitoring_section` (specifically `display_status_info` fragment)**
    *   **Objective**: Update the selected job's status from its log file just before displaying it.
    *   **Changes**:
        1.  Inside the `display_status_info` fragment, before displaying job details:
            *   Get the `selected_job_id` and corresponding `job_data`.
            *   If `job_data` exists, its status is "Running" or "Initializing", and `job_data.pipeline_log_file_path` is set, call `update_selected_job_progress_from_log(job_data, conn)`.
        2.  Adjust progress display in `display_status_info`:
            *   Remove the existing direct call to `calculate_progress_from_phase` that populates `calculated_progress`.
            *   The primary source for progress is now `job_data.progress` (which should be an integer 0-100).
            *   Rename `job_data.max_progress` to `job_data.max_progress_displayed` (this will store a float 0.0-1.0 for UI smoothing).
            *   Update the logic for `max_progress_displayed`:
                ```python
                # Inside display_status_info, after potentially calling update_selected_job_progress_from_log
                current_progress_from_model = float(getattr(job_data, "progress", 0.0)) / 100.0
                if not hasattr(job_data, "max_progress_displayed"):
                    job_data.max_progress_displayed = 0.0 # Initialize if not present
                if current_progress_from_model > job_data.max_progress_displayed:
                    job_data.max_progress_displayed = current_progress_from_model
                progress_to_display_on_bar = job_data.max_progress_displayed
                ```
            *   The `st.progress()` bar should use `progress_to_display_on_bar`.
        3.  The displayed phase should directly use `job_data.phase`, which is now updated by both queue messages and the new log reading mechanism.

*   **Task 2.2: Review and Ensure Consistency in `process_queue_messages`**
    *   **Objective**: Verify that queue messages still correctly handle overarching status changes, initial/final progress, and file paths.
    *   **Checks**:
        1.  Ensure that when `status_update` from the queue contains "progress", `job_model.progress` is updated (as an integer 0-100).
        2.  Confirm that "phase" updates from the queue (e.g., "Initializing", "Starting Pipeline", "Finished", "Failed") are correctly applied to `job_model.phase`.
        3.  This function primarily handles coarse-grained updates, while log reading provides fine-grained progress for the *selected, running* job.

# **Phase 3: Modifying Pipeline to Emit Progress Logs**

*   **Task 3.1: Update `run_pipeline_in_process` (and underlying pipeline scripts)**
    *   **Objective**: Ensure the pipeline process logs progress messages in the expected format.
    *   **Changes**:
        1.  Within `run_pipeline_in_process`, and any functions it calls in master_pipeline.py or other modules that represent distinct steps:
            *   At appropriate points, log messages like:
                `app_logger.info(f"PROGRESS: <main_phase_key>.<step_key> - Optional detailed message")`
                *   `<main_phase_key>` must match a key in `PHASE_FORMATS` (e.g., "webcrawl").
                *   `<step_key>` must match a key within `PHASE_FORMATS[main_phase_key]` (e.g., "extract_llm").
            *   Example: `app_logger.info("PROGRESS: webcrawl.extract_llm - Processing company X")`
        2.  These logs must be written to the job-specific `log_file_path` that `run_pipeline_in_process` sets up.

# **Phase 4: Testing**

*   **Task 4.1: Unit Tests**
    *   Write unit tests for `parse_progress_log_line` covering valid and invalid inputs.
    *   Write unit tests for `update_selected_job_progress_from_log`. This will involve mocking:
        *   File system operations (`open`, `read`, `seek`).
        *   `st.session_state`.
        *   `db_utils.add_or_update_job_in_db`.
        *   `calculate_progress_from_phase`.
        *   `JobDataModel` instances.
*   **Task 4.2: Integration and Manual Testing**
    *   Start a new job through the Streamlit UI.
    *   Monitor the "Monitoring" section:
        *   Verify that the phase updates according to the `PROGRESS:` lines in the log.
        *   Verify that the progress bar updates smoothly.
        *   Check that `max_progress_displayed` prevents the UI progress bar from flickering backward.
    *   Inspect the job's log file directly to confirm `PROGRESS:` messages are correctly formatted.
    *   Check the database to ensure `phase` and `progress` fields are updated.
    *   Test with multiple concurrent jobs if possible (though log reading is for the *selected* job).
    *   Test edge cases: job errors, cancellation, quick completion.

This list should guide you through the implementation incrementally.