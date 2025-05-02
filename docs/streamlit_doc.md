# Streamlit Component Usage Guide (Based on PRD)

This document outlines the recommended usage of Streamlit components for the Scraper UI, based on the project requirements document (PRD) and Streamlit best practices.

## Core Concepts

*   **State Management (`st.session_state`):** This is fundamental for managing user inputs, configurations, job statuses, and ensuring responsiveness across interactions and multiple runs.
    *   Use `st.session_state` to store selected phases, advanced configurations, uploaded file info, manually entered data, and the status/details of each submitted run (Run ID, status, log path, output path).
    *   Link widgets directly to session state using the `key` argument (e.g., `st.checkbox("Phase 1", key="phase1_selected")`).
    *   Use callbacks (`on_click`, `on_change`) on buttons and inputs to trigger functions that update `st.session_state` and manage application logic (e.g., starting a run, updating job status).
    *   Initialize session state variables at the beginning of the script to avoid errors (e.g., `st.session_state.setdefault('runs', {})`).

```python
# Example: Initializing and using session state for a checkbox
st.session_state.setdefault('phase1_selected', True) # Default to selected
st.checkbox("Run Phase 1: Machine Assets", key="phase1_selected")

# Example: Storing run information
if 'runs' not in st.session_state:
    st.session_state.runs = {}

def start_new_run(config):
    run_id = f"run_{int(time.time())}"
    st.session_state.runs[run_id] = {"status": "queued", "config": config, "logs": [], "output": None}
    # Trigger backend job submission here...

# Accessing state
# st.write(st.session_state.phase1_selected)
# st.write(st.session_state.runs)
```

## Layout

*   **Sidebar (`st.sidebar`):** Use for primary configuration options like phase selection and the "Start Run" button.
*   **Main Panel:** Use for data input (file upload/manual entry), displaying the list of runs, showing status/progress, viewing logs, and previewing/downloading results.
*   **Columns (`st.columns`):** Useful for organizing elements side-by-side within the sidebar or main panel.
*   **Containers (`st.container`):** Can help control the rendering order of elements if needed.
*   **Expanders (`st.expander`):** Ideal for the "Advanced Configuration" section, keeping the main UI clean by default.

```python
# Example: Basic Layout
with st.sidebar:
    st.header("Configuration")
    st.checkbox("Phase 1", key="phase1")
    # ... other phases ...
    with st.expander("Advanced Configuration"):
        st.number_input("Phase 1 Param", key="p1_param")
        # ... other params ...
    if st.button("Start Run"):
        # Call function to gather config from session state and start run
        pass

st.header("Job Status & Output")
# ... display list of runs from st.session_state.runs ...
# ... display logs/output for selected run ...
```

## Input Widgets

*   **File Uploader (`st.file_uploader`):** For uploading the input CSV file.
*   **Text Input (`st.text_input`):** For manual entry of company names or other parameters within the Advanced Configuration.
*   **Checkbox (`st.checkbox`):** For selecting pipeline phases and toggling the "Skip LLM Validation" option.

## Output & Display Widgets

*   **Download Button (`st.download_button`):** Provide buttons to download the final CSV and potentially zipped intermediate artifacts for each run. Data for the button should be prepared in memory.
*   **Dataframe/Data Editor (`st.dataframe` / `st.data_editor`):** Display a preview of the final output data table. `st.dataframe` is suitable for read-only display.
*   **Text Area/Code (`st.text_area` / `st.code`):** Use within the main panel to display logs for a selected run. Update dynamically based on backend log streaming or file reading.
*   **Progress Indicators (`st.progress`, `st.spinner`, `st.status`):**
    *   Use `st.progress` to show the overall progress of an active run.
    *   Use `st.spinner` to indicate short processing times (e.g., loading data).
    *   Use `st.status` to show discrete steps within a phase.

```python
# Example: Download Button
@st.cache_data # Cache the data generation if possible
def convert_df_to_csv(df):
   return df.to_csv(index=False).encode('utf-8')

# Assuming 'final_df' is your final pandas DataFrame
csv_data = convert_df_to_csv(final_df)
st.download_button(
   label="Download Final CSV",
   data=csv_data,
   file_name=f"final_export_{category}_{timestamp}.csv",
   mime='text/csv',
)

# Example: Log Display (simplified)
selected_run_id = st.selectbox("Select Run to View Logs", options=st.session_state.runs.keys())
if selected_run_id:
    # In a real app, fetch/stream logs based on run_id
    log_content = "\n".join(st.session_state.runs[selected_run_id].get("logs", ["No logs yet."]))
    st.text_area("Logs", value=log_content, height=300)
```

## Real-time Updates & Concurrency

*   The UI needs to reflect the status of multiple concurrent runs managed by the backend job runner.
*   Periodically refresh the status display or use techniques like Streamlit Fragments (if applicable and stable) or potentially a background thread that updates `st.session_state` to trigger UI updates showing job progress and log streaming. *Care must be taken with threading directly modifying session state; often, a queue mechanism or checking backend status on rerun is safer.*
*   Ensure the UI remains responsive during backend processing. Long-running tasks should be handled by the backend runner, not blocking the Streamlit script execution.
