## Requirement Document: `delete_job_and_artifacts` Function

**1. Introduction and Purpose**

The `delete_job_and_artifacts` function is responsible for the complete removal of a specified job and all its associated data. This includes deleting the job's record from the database, as well as removing related files and directories from the filesystem, such as log files, output directories, and temporary files. It also ensures that any cached information about the job in the application's session state is cleared.

**2. Inputs**

The function requires the following inputs:

*   **`conn: Any`**:
    *   Description: The active database connection object. This is used to interact with the database for deleting the job record.
    *   Type: Database connection object (e.g., SQLAlchemy connection or similar).
*   **`job_id: str`**:
    *   Description: The unique identifier (string) of the job to be deleted.
    *   Type: `str`
*   **`active_jobs_from_state: Dict[str, JobDataModel]`**:
    *   Description: A dictionary representing the currently active jobs, typically sourced from `st.session_state.active_jobs`. This is used as an initial source to retrieve job metadata.
    *   Type: `Dict[str, JobDataModel]`

**3. Outputs**

The function returns a boolean value indicating the success or failure of the core deletion operation:

*   **`return: bool`**:
    *   Description:
        *   Returns `True` if the job record was successfully deleted from the database OR if the job was determined to not exist in the database (i.e., already considered deleted from a DB perspective).
        *   Returns `False` if a critical error occurred, primarily during the database deletion step or if the job data could not be loaded from the database when it was expected to be there.
    *   Type: `bool`

**4. Process and Steps**

The function executes the following steps in sequence:

1.  **Log Deletion Attempt**: Logs an informational message indicating the start of the deletion process for the given `job_id`.
2.  **Retrieve Job Data**:
    1.  Attempts to retrieve the `JobDataModel` instance for the `job_id` from the provided `active_jobs_from_state` dictionary.
    2.  If not found in `active_jobs_from_state`, it attempts to load the job data directly from the database using `db_utils.load_jobs_from_db(conn)`.
    3.  If the job is not found in either the session state or the database, a warning is logged, and the function returns `True` (as the job is effectively already gone from a data persistence perspective).
    4.  If an error occurs while trying to load the job from the database (and it wasn't in session state), an error is logged, and the function returns `False`.
3.  **Delete Filesystem Artifacts**: If the `JobDataModel` is successfully retrieved, the function proceeds to delete associated filesystem artifacts. Errors during these steps are logged but are generally considered non-critical for the function's overall success status (i.e., they won't cause a `False` return if the DB deletion is successful).
    1.  **Delete Pipeline Log File**:
        *   Retrieves `job_data.pipeline_log_file_path`.
        *   If the path exists, attempts to delete the file using `os.remove()`.
        *   Logs success or any `OSError`.
    2.  **Delete Job-Specific Output Directory**:
        *   Constructs the path to the job's output directory (e.g., `output_base_dir/job_id`).
        *   If the directory exists, attempts to recursively delete it using `shutil.rmtree()`.
        *   Logs success or any `OSError`.
    3.  **Delete Temporary Input CSV File**:
        *   Retrieves `job_data.temp_input_csv_path`.
        *   If the path exists, attempts to delete the file using `os.remove()`.
        *   Logs success or any `OSError`.
4.  **Delete from Database**:
    1.  Calls `db_utils.delete_job_from_db(conn, job_id)` to remove the job's record from the database.
    2.  The success status of this operation is crucial and directly influences the function's return value.
    3.  Logs the outcome of the database deletion attempt.
5.  **Clean Up Session State**: Removes references to the deleted `job_id` from various `st.session_state` caches to prevent stale data issues:
    *   `st.session_state.active_jobs`
    *   `st.session_state.log_file_positions`
    *   `st.session_state.job_selections`
    *   `st.session_state.selected_for_deletion_cache`
6.  **Log Deletion Completion**: Logs an informational message indicating the end of the deletion process for the `job_id`.
7.  **Return Status**: Returns the boolean status primarily based on the success of the database deletion operation.

**5. Error Handling**

*   **Filesystem Errors**: Errors encountered during the deletion of files or directories (e.g., permission issues, file not found if expected) are logged. These errors do not typically halt the entire deletion process or cause the function to return `False` if the database deletion is successful.
*   **Database Errors**:
    *   Errors during the loading of job data from the database (if not found in session state) are considered critical and will lead to a `False` return.
    *   Errors during the deletion of the job record from the database are considered critical and will lead to a `False` return.
*   **Logging**: All significant actions, errors, and outcomes are logged using the `monitoring_logger`.

**6. Assumptions and Preconditions**

*   The `db_utils.load_jobs_from_db` and `db_utils.delete_job_from_db` functions are implemented correctly and are accessible.
*   The `monitoring_logger` object is properly configured and available.
*   The `JobDataModel` instances contain the necessary attributes for identifying artifacts (e.g., `pipeline_log_file_path`, `config['output_base_dir']`, `id`, `temp_input_csv_path`).
*   The Python `os` and `shutil` modules are available.
*   The database connection `conn` is valid and active.

**7. Success Criteria**

A successful deletion operation is characterized by:

*   The job record corresponding to `job_id` is no longer present in the database.
*   Associated filesystem artifacts (log file, output directory, temp file), if they existed, are removed from the filesystem.
*   The `job_id` is cleared from relevant `st.session_state` entries.
*   The function returns `True`.

**8. Failure Conditions**

The function may be considered to have failed (and return `False`) if:

*   A critical error occurs while attempting to delete the job record from the database.
*   A critical error occurs while attempting to load job data from the database when it was expected to be present.

Minor failures, such as an inability to delete a specific file from the filesystem (while the DB deletion succeeds), will be logged but might not result in a `False` return, depending on the implementation of `db_utils.delete_job_from_db` and the overall desired behavior. The primary indicator of success/failure is the database operation.]



**Key changes and considerations:**
*   **`delete_job_and_artifacts`**: This new helper function encapsulates the logic to remove log files, output directories, temporary files, and the database record. It also cleans up relevant `st.session_state` entries.
*   **Button and Confirmation**:
    *   A "Delete Checked Jobs" button is added.
    *   Clicking it sets `st.session_state.show_confirm_delete_expander = True` cache the jobs id in `job_ids_selected_for_deletion`
    *   An expander then appears for final confirmation.
    *   `st.rerun()` is used to refresh the UI state after actions.
*   **State Management**: Careful use of `st.session_state` for `job_ids_selected_for_deletion`, `show_confirm_delete_expander`, is crucial for the multi-step interaction to work correctly with Streamlit's execution model.
*   **Error Handling**: The `delete_job_and_artifacts` function includes basic error handling for file operations.
*   **Clarity**: Button labels and help texts are updated for better user understanding.
*   **Unique Keys**: Ensure all Streamlit widget keys (`key="..."`) are unique within the app page. I've updated some for clarity.


Okay, here's a 10-task plan to implement the multi-select and delete jobs feature:

Okay, I've updated tasks 1, 2, 3, and 4 to incorporate `st.dataframe` and its `DataframeSelectionState` for selecting rows, instead of using `st.data_editor` with a separate "Select" column.

1.  **Initialize Session State for Deletion Flow:**
    *   In `app.py`, within the `init_session_state` function, ensure the following keys are initialized:
        *   `show_confirm_delete_expander`: Initialize to `False`. This controls the visibility of the deletion confirmation UI.
        *   `job_ids_selected_for_deletion`: Initialize as an empty dict. This will store the actual `job_id`s derived from the `st.dataframe` row selections when the user initiates the deletion process.
        *   (Optional but recommended) If the DataFrame used for displaying jobs is generated and might be needed for mapping selections later, consider storing it or a reference to it in session state (e.g., `st.session_state.jobs_df_for_display`).

2.  **Modify `jobs_table_fragment` to Use `st.dataframe` for Row Selection:**
    *   Locate the `jobs_table_fragment` function (likely in `monitoring_section.py`).
    *   Ensure the DataFrame containing job information (`jobs_df`) is prepared. This DataFrame must include a column containing the unique `job_id` for each job, or the DataFrame's index should reliably map to `job_id`s.
    *   Replace any existing job table display widget with `st.dataframe(jobs_df, ...)`.
    *   Configure the `st.dataframe` call:
        *   Assign a unique and persistent `key` (e.g., `key="jobs_dataframe_selector"`). This key is essential for Streamlit to manage and expose the selection state via `st.session_state`.
        *   Set `on_select="rerun"`. This ensures that when a user changes the selection, the Streamlit app reruns, allowing UI elements or logic dependent on the selection to update.
        *   Set `selection_mode=["multi-row"]` (or an equivalent iterable like `("multi-row",)`). This configures the dataframe to allow users to select one or more entire rows.

3.  **Configure `st.dataframe` Display and Provide User Guidance:**
    *   In `jobs_table_fragment`, when calling `st.dataframe`:
        *   Optionally, use the `column_config` parameter to customize the appearance of columns for better readability (e.g., formatting dates, setting user-friendly column names, adjusting widths). This does not directly affect selection but improves the user experience.
        *   Ensure the column containing `job_id` is present in the DataFrame passed to `st.dataframe`, as this ID will be crucial for identifying which jobs to delete based on row selections.
        *   Add a note or caption near the `st.dataframe` (e.g., using `st.caption` or `st.markdown`) to inform users that sorting the table by clicking on column headers will reset any active row selections. Advise them to use the built-in search functionality within the dataframe's toolbar if they need to filter or find specific jobs before making selections.

4.  **Retrieve Selected Job IDs for Deletion Process (Triggered by User Action):**
    *   This task's logic will be executed when the user initiates the deletion (e.g., by clicking a "Delete Selected Jobs" button, as planned in later tasks). It's not about continuously persisting selections after every click in the dataframe, as `st.dataframe` handles its own selection state persistence when a `key` is provided.
    *   When the action to proceed with deletion is triggered:
        1.  Access the current selection state from `st.session_state[your_dataframe_key].selection.rows`, where `your_dataframe_key` is the `key` you assigned to `st.dataframe` in Task 2. This will provide a list of integer indices representing the selected rows based on their original order in the DataFrame.
        2.  Retrieve the DataFrame that was originally supplied to `st.dataframe` (let's call this `original_jobs_df`). It's critical that this DataFrame is the exact one whose row indices correspond to the selection state (it might be stored in `st.session_state` or re-fetched/re-generated consistently).
        3.  Using the list of selected row indices, extract the corresponding `job_id`s from `original_jobs_df`. For example, if `job_id` is a column in the DataFrame: `selected_job_ids = original_jobs_df.iloc[selected_row_indices]['job_id'].tolist()`.
        4.  Store this list of `selected_job_ids` into `st.session_state.job_ids_selected_for_deletion`. This list will then be used by the subsequent confirmation and deletion logic (Tasks 8-10).

5.  **Create `delete_job_and_artifacts` Function (Part 1: Core Logic & DB Deletion):**
    *   In db_utils.py, define the `delete_job_and_artifacts` function with the specified signature: `(conn: Any, job_id: str, active_jobs_from_state: Dict[str, JobDataModel]) -> bool`.
    *   Implement the initial logging for the deletion attempt.
    *   Add logic to retrieve the `JobDataModel`: first from `active_jobs_from_state`, then by loading from the database if not found. Handle cases where the job doesn't exist in either, logging a warning and returning `True`. If DB loading fails, log an error and return `False`.
    *   Call `db_utils.delete_job_from_db(conn, job_id)` to remove the job record. The success of this operation will be the primary determinant of the function's return value. Log the outcome.

6.  **Extend `delete_job_and_artifacts` (Part 2: Filesystem Artifact Deletion):**
    *   Continue implementing `delete_job_and_artifacts`.
    *   If the `JobDataModel` is retrieved, add sections to delete:
        *   The pipeline log file (using `job_data.pipeline_log_file_path` and `os.remove()`).
        *   The job-specific output directory (constructing the path, e.g., from `job_data.config['output_dir']`, and using `shutil.rmtree()`).
        *   The temporary input CSV file (using `job_data.temp_input_csv_path` and `os.remove()`).
    *   For each deletion attempt, check if the path exists and log success or any `OSError`. These errors should be logged but generally not cause the function to return `False` if the database deletion part was successful.

7.  **Extend `delete_job_and_artifacts` (Part 3: Session State Cleanup):**
    *   Finalize the `delete_job_and_artifacts` function.
    *   After successful database deletion and artifact removal attempts, add logic to clean up `st.session_state` for the processed `job_id`:
        *   `st.session_state.active_jobs.pop(job_id, None)`
        *   `st.session_state.log_file_positions.pop(job_id, None)`
    *   Note: The function should not manage the list of job IDs pending deletion; that's handled by the UI logic in Task 10.

8.  **Add "Delete Selected Jobs" Button and Confirmation UI in Monitoring Section:**
    *   In monitoring_section.py, outside the `jobs_table_fragment` (so it doesn't refresh with the table), add an `st.button("Delete Selected Jobs", key="delete_selected_jobs_button")`.
    *   Below this button, create an `st.expander("Confirm Deletion")` whose visibility is controlled by `st.session_state.show_confirm_delete_expander`.
    *   Inside the expander, add:
        *   A message indicating how many jobs are selected for deletion.
        *   An `st.button("Yes, Delete These Jobs", key="confirm_delete_button")`.
        *   An `st.button("Cancel", key="cancel_delete_button")`.

9.  **Implement "Delete Selected Jobs" Button Logic:**
    *   In `monitoring_section.py`, if the "Delete Selected Jobs" button is clicked:
        *   Access the selected row indices from `st.session_state[your_dataframe_key].selection.rows` (where `your_dataframe_key` is from Task 2, e.g., "jobs_dataframe_selector").
        *   Retrieve the `original_jobs_df` (e.g., from `st.session_state.jobs_df_for_display` as recommended in Task 1).
        *   Map these row indices to a list of `job_id`s using `original_jobs_df.iloc[selected_row_indices]['job_id'].tolist()` (as detailed in Task 4).
        *   Store this list of `job_id`s in `st.session_state.job_ids_selected_for_deletion`.
        *   If `st.session_state.job_ids_selected_for_deletion` is not empty, set `st.session_state.show_confirm_delete_expander = True`. Otherwise, show a warning that no jobs are selected.
        *   Call `st.rerun()` to update the UI and show the expander.

10. **Implement Confirmation and Final Deletion Logic:**
    *   In `monitoring_section.py`, within the logic for the confirmation expander:
        *   If the "Yes, Delete These Jobs" button is clicked:
            *   Iterate through each `job_id` in `st.session_state.job_ids_selected_for_deletion`.
            *   Call `delete_job_and_artifacts(conn, job_id, st.session_state.active_jobs)`.
            *   Collect results and display appropriate success/error messages (e.g., `st.success` or `st.error`) for the batch operation.
            *   Clear `st.session_state.job_ids_selected_for_deletion` (e.g., `st.session_state.job_ids_selected_for_deletion = []`).
            *   Set `st.session_state.show_confirm_delete_expander = False`.
            *   Call `st.rerun()` to refresh the job list.
        *   If the "Cancel" button is clicked:
            *   Clear `st.session_state.job_ids_selected_for_deletion`.
            *   Set `st.session_state.show_confirm_delete_expander = False`.
            *   Call `st.rerun()`.