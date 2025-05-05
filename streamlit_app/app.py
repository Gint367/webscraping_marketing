import io
import logging
import os
import sys

import pandas as pd
import streamlit as st

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Page Configuration (Must be the first Streamlit command) ---
st.set_page_config(layout="wide", page_title="Company Enrichment Tool") # Added page config

# --- Constants for Column Validation ---
REQUIRED_COLUMNS_MAP = {
    "company name": ["company name", "firma1"],
    "location": ["location", "ort"],
    "url": ["url"]
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Function for Validation ---
def validate_columns(df_columns: list[str]) -> tuple[dict[str, tuple[bool, str | None]], bool]:
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
    normalized_df_columns = {col.lower().strip(): col for col in df_columns} # Store original casing

    for canonical_name, aliases in REQUIRED_COLUMNS_MAP.items():
        found = False
        actual_name = None
        for alias in aliases:
            if alias.lower() in normalized_df_columns:
                found = True
                actual_name = normalized_df_columns[alias.lower()] # Get original casing
                break
        validation_results[canonical_name] = (found, actual_name)
        if not found:
            all_found = False
    return validation_results, all_found


# --- Session State Initialization ---
def init_session_state():
    """Initializes session state variables if they don't exist."""
    defaults = {
        "page": "Input",
        "company_list": None, # Will store list of dicts for processing
        "uploaded_file_data": None, # Stores the uploaded file object
        "manual_input_df": pd.DataFrame(columns=["company name", "location", "url"]), # For data editor
        "input_method": "File Upload", # Default input method
        "config": {},
        "job_status": "Idle",
        "results": None,
        "log_messages": [],
        "testing_mode": False, # Flag to disable st.rerun() calls during tests
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    # Ensure company_list is initialized correctly for data_editor if needed
    if st.session_state['input_method'] == 'Manual Input' and st.session_state['company_list'] is None:
         st.session_state['company_list'] = pd.DataFrame(columns=["company name", "location", "url"])

    logging.info("Session state initialized.")

init_session_state()

# --- Logging Handler for Streamlit ---
class StreamlitLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            # Use key access for log_messages
            st.session_state['log_messages'].append(msg)
            # Keep only the last N messages if needed
            # max_log_entries = 100
            # st.session_state['log_messages'] = st.session_state['log_messages'][-max_log_entries:]
        except Exception:
            self.handleError(record)

# Add the handler to the root logger AFTER initial state setup
streamlit_handler = StreamlitLogHandler()
logging.getLogger().addHandler(streamlit_handler)

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
        label_visibility="collapsed"
    )
    
    # Check if we need to update the input method
    if 'input_method' not in st.session_state or st.session_state['input_method'] != input_method:
        # For real app, but not for tests
        if hasattr(st.session_state, "running_test") and st.session_state.running_test:
            # In test mode, just update state without rerun
            st.session_state['input_method'] = input_method
        else:
            # Normal app flow
            old_method = st.session_state.get('input_method')
            clear_other_input(input_method)
            st.session_state['input_method'] = input_method
            
            # Only rerun if input method actually changed (not on first load)
            # Skip rerun if in testing mode
            if old_method and old_method != input_method and not st.session_state.get("testing_mode", False):
                st.rerun()

    # Flag to track validation status for uploaded file
    validation_passed_all = True # Assume true unless file upload fails validation

    if input_method == "File Upload":
        st.subheader("Upload Company List")
        current_file_in_state = st.session_state.get('uploaded_file_data')

        if current_file_in_state is None:
            # --- Show File Uploader ---
            uploaded_file = st.file_uploader(
                "Upload a CSV or Excel file", # Standard label
                type=["csv", "xlsx"],
                key="file_uploader_widget", # Use a distinct key for the widget
                accept_multiple_files=False
            )

            if uploaded_file is not None:
                # File has just been uploaded
                st.session_state['uploaded_file_data'] = uploaded_file
                # Clear other input method's state
                st.session_state['manual_input_df'] = pd.DataFrame(columns=["company name", "location", "url"])
                st.session_state['company_list'] = None # Clear processed list as input changed
                logging.info(f"File selected: {uploaded_file.name}")
                st.success(f"File '{uploaded_file.name}' selected.") # Use standard quotes
                
                # Only rerun if not in testing mode
                if not st.session_state.get("testing_mode", False):
                    st.rerun() # Rerun immediately to show the 'Change File' button state

        else:
            # --- Show File Info ---
            st.success(f"Selected file: **{current_file_in_state.name}**")

            # --- Display Preview and Validation ---
            st.write("Preview & Column Validation:")
            preview_df = None
            header_columns = [] # Store header columns found
            # validation_passed_all = True # Reset here for this specific file check

            try:
                # Read header and first 5 rows together
                current_file_in_state.seek(0) # Ensure we start from the beginning

                if current_file_in_state.name.endswith('.csv'):
                    bytesio = io.BytesIO(current_file_in_state.getvalue())
                    try:
                        # Try auto-detect separator, let pandas handle encoding from bytes
                        temp_df = pd.read_csv(bytesio, nrows=6, sep=None, engine='python')
                        # Check if columns were read correctly, sometimes sep=None gives one wrong col
                        if len(temp_df.columns) <= 1 and ',' in temp_df.columns[0]:
                             logging.warning("Auto-detected separator might be wrong, trying comma explicitly.")
                             bytesio.seek(0)
                             temp_df = pd.read_csv(bytesio, nrows=6, sep=',')
                        elif len(temp_df.columns) <= 1 and ';' in temp_df.columns[0]:
                             logging.warning("Auto-detected separator might be wrong, trying semicolon explicitly.")
                             bytesio.seek(0)
                             temp_df = pd.read_csv(bytesio, nrows=6, sep=';')

                    except Exception as e_read:
                        logging.warning(f"CSV read failed with auto/comma separator: {e_read}. Trying semicolon.")
                        # Reset and try semicolon if default failed
                        bytesio.seek(0)
                        try:
                             temp_df = pd.read_csv(bytesio, nrows=6, sep=';')
                        except Exception as e_read_semi:
                             logging.error(f"CSV read failed with common separators: {e_read_semi}", exc_info=True)
                             raise ValueError("Could not parse CSV file. Check format, encoding, and separator.") from e_read_semi

                    if not temp_df.empty:
                        header_columns = temp_df.columns.tolist()
                        preview_df = temp_df.head(5) # Take the first 5 rows for preview
                    else:
                        # Check if file has content but pandas couldn't parse columns/rows
                        bytesio.seek(0)
                        file_content_sample = bytesio.read(200).decode(errors='ignore')
                        if file_content_sample.strip():
                            logging.warning(f"Pandas read CSV resulted in empty DataFrame, but file has content. Sample: {file_content_sample[:100]}...")
                            st.warning("Could not parse rows/columns correctly. Please check CSV format (separator, quotes, encoding).")
                        else:
                            logging.warning("Pandas read CSV resulted in empty DataFrame, file appears empty.")
                            st.warning("File appears to be empty.")
                        validation_passed_all = False


                elif current_file_in_state.name.endswith(('.xls', '.xlsx')):
                    bytesio = io.BytesIO(current_file_in_state.getvalue())
                    # Read header and first 5 rows
                    temp_df = pd.read_excel(bytesio, nrows=6) # Reads header + 5 data rows
                    if not temp_df.empty:
                        header_columns = temp_df.columns.tolist()
                        preview_df = temp_df.head(5)
                    else:
                         logging.warning("Pandas read Excel resulted in empty DataFrame.")
                         st.warning("File appears empty or the first sheet has no data.")
                         validation_passed_all = False


                else:
                     st.warning("Cannot preview this file type.")
                     validation_passed_all = False # Cannot validate if cannot preview

                # --- Perform Validation ---
                if header_columns: # Check if we actually got columns
                    column_validation, validation_passed_all_cols = validate_columns(header_columns)
                    #st.markdown("---") # Separator
                    #st.write("**Required Column Status:**")
                    for canonical_name, (found, actual_name) in column_validation.items():
                        aliases_str = "/".join(REQUIRED_COLUMNS_MAP[canonical_name])
                        if found:
                            st.success(f"✔️ Found: **{canonical_name}** (as '{actual_name}')")
                        else:
                            st.error(f"❌ Missing: **{canonical_name}** (expected one of: {aliases_str})")
                    #st.markdown("---") # Separator
                    # Update overall validation status based on columns
                    if not validation_passed_all_cols:
                         validation_passed_all = False
                elif validation_passed_all: # Only show error if no other warning/error was raised during read
                     # No columns found during read attempt
                     st.error("Could not detect columns in the file. Please ensure the file is correctly formatted.")
                     validation_passed_all = False


                # --- Display Preview DataFrame ---
                if preview_df is not None and not preview_df.empty: # Check if preview has rows
                    st.dataframe(preview_df, use_container_width=True)
                elif header_columns: # Header found, but no data rows in the first 5
                     st.info("File has columns, but no data rows found in the preview (first 5 rows).")
                # If header_columns is empty, the error message above was already shown

                # Reset pointer for the actual processing function later
                current_file_in_state.seek(0)

            except Exception as e:
                st.error(f"Could not read or preview the file: {e}")
                logging.error(f"Error previewing file {current_file_in_state.name}: {e}", exc_info=True) # Add traceback
                validation_passed_all = False # Error means validation fails

            # --- Change File Button ---
            if st.button("Change File"):
                st.session_state['uploaded_file_data'] = None
                st.session_state['company_list'] = None # Clear processed list
                logging.info("User clicked 'Change File'. Clearing uploaded file.")
                
                # Only rerun if not in testing mode
                if not st.session_state.get("testing_mode", False):
                    st.rerun()

    elif input_method == "Manual Input":
        st.subheader("Enter Data Manually")
        st.write("Add or edit company details below:")

        # Initialize DataFrame in session state if it doesn't exist or is None
        if 'manual_input_df' not in st.session_state or st.session_state['manual_input_df'] is None:
             st.session_state['manual_input_df'] = pd.DataFrame(columns=["company name", "location", "url"])

        # Use st.data_editor for manual input
        edited_df = st.data_editor(
            st.session_state['manual_input_df'],
            num_rows="dynamic",
            key="manual_data_editor",
            column_config={ # Optional: Add specific configurations if needed
                "company name": st.column_config.TextColumn("Company Name", required=True),
                "location": st.column_config.TextColumn("Location", required=True),
                "url": st.column_config.LinkColumn("URL", required=True, validate="^https?://"),
            },
            hide_index=True,
            use_container_width=True
        )

        # Update session state with the edited data
        st.session_state['manual_input_df'] = edited_df

        # Clear uploaded file data when manual input is used
        st.session_state['uploaded_file_data'] = None
        st.session_state['company_list'] = None # Clear processed list until "Start" is clicked

        if not edited_df.empty:
            logging.info(f"Manual input data updated. Rows: {len(edited_df)}")
            # Convert DataFrame to list of dicts for potential downstream use
            # This conversion can happen here or just before processing
            # st.session_state['company_list'] = edited_df.to_dict('records')


    # --- Processing Trigger ---
    st.divider()
    # Disable button if validation failed for uploaded file OR if no file/manual data
    processing_disabled = False
    if st.session_state['input_method'] == "File Upload":
        if st.session_state.get('uploaded_file_data') is None:
            processing_disabled = True # No file uploaded
        elif not validation_passed_all:
            processing_disabled = True # File uploaded but failed validation
            st.warning("Cannot start processing. Please upload a file with all required columns (or fix the current one).")
    elif st.session_state['input_method'] == "Manual Input":
        manual_df = st.session_state.get('manual_input_df')
        if manual_df is None or manual_df.empty:
            processing_disabled = True # No manual data entered


    if st.button("Start Processing", type="primary", disabled=processing_disabled):
        # No need for the inner check if button is correctly disabled
        process_data()

def clear_other_input(selected_method):
    """Clears the session state of the non-selected input method."""
    if selected_method == "File Upload":
        st.session_state['manual_input_df'] = pd.DataFrame(columns=["company name", "location", "url"])
        st.session_state['company_list'] = None
        logging.info("Switched to File Upload, cleared manual input state.")
    elif selected_method == "Manual Input":
        st.session_state['uploaded_file_data'] = None
        # Reset file uploader widget state if possible (Streamlit might handle this)
        # st.session_state['file_uploader'] = None # May cause issues, test carefully
        st.session_state['company_list'] = None
        logging.info("Switched to Manual Input, cleared file upload state.")

def process_data():
    """Processes the data from the selected input method."""
    st.session_state['job_status'] = "Processing"
    st.session_state['results'] = None # Clear previous results
    st.session_state['log_messages'] = ["Processing started..."] # Reset logs
    logging.info("Processing started.")
    if not st.session_state.get("testing_mode", False):
        st.rerun() # Rerun to update status immediately

    data_to_process = None

    if st.session_state['input_method'] == 'file' and st.session_state['uploaded_file_data']:
        uploaded_file = st.session_state['uploaded_file_data']
        logging.info(f"Processing uploaded file: {uploaded_file.name}")
        try:
            if uploaded_file.name.endswith('.csv'):
                # Use StringIO to treat the byte stream as a text file
                stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                df = pd.read_csv(stringio)
            elif uploaded_file.name.endswith(('.xls', '.xlsx')):
                # Read excel directly from bytes
                df = pd.read_excel(uploaded_file)
            else:
                 st.error("Unsupported file type.")
                 st.session_state['job_status'] = "Error"
                 logging.error("Unsupported file type uploaded.")
                 return

            # --- Data Validation and Formatting ---
            # Ensure required columns exist (case-insensitive check)
            required_cols = ["company name", "location", "url"]
            df.columns = df.columns.str.lower().str.strip() # Normalize column names
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.error(f"Uploaded file is missing required columns: {', '.join(missing_cols)}")
                st.session_state['job_status'] = "Error"
                logging.error(f"Uploaded file missing columns: {missing_cols}")
                return

            # Select and rename columns to ensure consistency
            df = df[required_cols]
            df.rename(columns={ # Ensure exact column names if needed downstream
                "company name": "company name",
                "location": "location",
                "url": "url"
            }, inplace=True)

            # Basic cleaning (optional, adapt as needed)
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df.dropna(subset=required_cols, inplace=True) # Drop rows with missing required values

            if df.empty:
                st.warning("No valid data found in the uploaded file after cleaning.")
                st.session_state['job_status'] = "Completed (No Data)"
                logging.warning("No valid data in uploaded file.")
                return

            data_to_process = df.to_dict('records')
            st.session_state['company_list'] = data_to_process # Store the final list
            logging.info(f"Successfully parsed {len(data_to_process)} records from file.")

        except Exception as e:
            st.error(f"Error reading or processing file: {e}")
            st.session_state['job_status'] = "Error"
            logging.error(f"Error processing file {uploaded_file.name}: {e}")
            return

    elif st.session_state['input_method'] == 'manual':
        manual_df = st.session_state.get('manual_input_df')
        if manual_df is not None and not manual_df.empty:
             # Basic cleaning (optional, adapt as needed)
            manual_df = manual_df.map(lambda x: x.strip() if isinstance(x, str) else x)
            # Validate required columns (data editor should enforce this, but double-check)
            required_cols = ["company name", "location", "url"]
            manual_df.dropna(subset=required_cols, inplace=True) # Drop rows missing required values

            if manual_df.empty:
                st.warning("No valid data entered manually after cleaning.")
                st.session_state['job_status'] = "Completed (No Data)"
                logging.warning("No valid data in manual input.")
                return

            data_to_process = manual_df.to_dict('records')
            st.session_state['company_list'] = data_to_process # Store the final list
            logging.info(f"Processing {len(data_to_process)} manually entered records.")
        else:
            st.warning("No manual data entered.")
            st.session_state['job_status'] = "Idle" # Or "Completed (No Data)"
            logging.warning("Start Processing clicked with no manual data.")
            return
    else:
        st.warning("No data provided. Please upload a file or enter data manually.")
        st.session_state['job_status'] = "Idle"
        logging.warning("Start Processing clicked with no data source selected or data provided.")
        return

    # --- Placeholder for Actual Pipeline Execution ---
    if data_to_process:
        st.info(f"Starting enrichment for {len(data_to_process)} companies...")
        logging.info(f"Data prepared for pipeline: {len(data_to_process)} records.")
        # try:
        #     # **** Replace with your actual pipeline call ****
        #     # results_df = run_extracting_machine_pipeline(data_to_process, st.session_state['config'])
        #     # Mock results for now
        #     time.sleep(2) # Simulate work
        #     mock_results = [{"company name": d["company name"], "location": d["location"], "url": d["url"], "enriched_data": "Processed"} for d in data_to_process]
        #     results_df = pd.DataFrame(mock_results)
        #     # **** End Replace ****

        #     st.session_state['results'] = results_df
        #     st.session_state['job_status'] = "Completed"
        #     logging.info("Processing completed successfully.")
        #     st.success("Processing finished!")
        #     # Switch to output page?
        #     # st.session_state['page'] = "Output"
        #     # st.rerun()

        # except Exception as e:
        #     st.error(f"Pipeline execution failed: {e}")
        #     st.session_state['job_status'] = "Error"
        #     logging.error(f"Pipeline execution failed: {e}")

        # --- Mock Implementation ---
        import time
        time.sleep(1) # Simulate work
        mock_results = [{"company name": d["company name"], "location": d["location"], "url": d["url"], "enriched_data": f"Processed_{i+1}"} for i, d in enumerate(data_to_process)]
        results_df = pd.DataFrame(mock_results)
        st.session_state['results'] = results_df
        st.session_state['job_status'] = "Completed"
        logging.info("Mock processing completed successfully.")
        st.success("Processing finished!")
        # --- End Mock ---

    else:
         # This case should ideally be caught earlier, but as a fallback:
         st.warning("No data available to process.")
         st.session_state['job_status'] = "Idle"
         logging.warning("process_data called but data_to_process was empty.")

    if not st.session_state.get("testing_mode", False):
        st.rerun() # Rerun to update UI with results/status


def display_config_section():
    """Displays the UI for configuration settings."""
    st.header("2. Configuration")
    st.write("Configure scraping and enrichment parameters.")
    # Use key access for config dictionary
    st.session_state['config']['depth'] = st.slider("Crawling Depth", 1, 5, 2)
    st.session_state['config']['llm_provider'] = st.selectbox("LLM Provider", ["OpenAI", "Anthropic", "Gemini", "Mock"])
    st.session_state['config']['api_key'] = st.text_input("API Key", type="password")
    logging.info(f"Configuration updated: Depth={st.session_state['config'].get('depth')}, LLM={st.session_state['config'].get('llm_provider')}")


def display_monitoring_section():
    """Displays the job monitoring and log output."""
    st.header("3. Monitoring")
    st.write("Track the progress of the scraping and enrichment process.")

    # Use key access for job_status
    st.info(f"Current Status: **{st.session_state['job_status']}**")
    if st.session_state['job_status'] == "Running":
        st.progress(50) # Example progress

    st.subheader("Logs")
    log_container = st.container(height=300)
    with log_container:
        # Use key access for log_messages
        for msg in reversed(st.session_state['log_messages']):
            st.text(msg)


def display_output_section():
    """Displays the results and download options."""
    st.header("4. Output")
    st.write("View and download the enriched data.")
    # Use key access for results
    results_data = st.session_state.get('results') # Use .get for safer access

    if results_data is not None and not results_data.empty:
        st.dataframe(results_data, use_container_width=True)

        # Prepare data for download
        @st.cache_data # Cache the conversion to avoid re-running on every interaction
        def convert_df_to_csv(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv(index=False).encode('utf-8')

        csv_data = convert_df_to_csv(results_data)

        st.download_button(
            label="Download Results as CSV",
            data=csv_data,
            file_name="enriched_company_data.csv",
            mime="text/csv",
            key='download-csv'
        )
    elif st.session_state['job_status'] == "Processing":
         st.info("Processing is ongoing. Results will appear here when complete.")
    elif st.session_state['job_status'] in ["Error", "Completed (No Data)"]:
         st.warning("No results to display. Check the Monitoring section for status and logs.")
    else:
        st.info("No results yet. Input data and start processing.")


# --- Sidebar Navigation ---

def handle_navigation():
    """Callback function to update the page state."""
    st.session_state['page'] = st.session_state['navigation_choice']
    logging.info(f"Navigation handled, page set to: {st.session_state['page']}")

st.sidebar.title("Navigation")
page_options = ["Input", "Configuration", "Monitoring", "Output"]

# Use a separate key for the radio widget and an on_change callback
st.sidebar.radio(
    "Go to",
    options=page_options,
    key='navigation_choice', # Key for the widget's state
    on_change=handle_navigation, # Function to call when the value changes
    # The index is now set based on the main 'page' state, ensuring consistency
    index=page_options.index(st.session_state['page'])
)

# --- Main App Logic ---
if __name__ == "__main__":
    # Display the selected page
    page = st.session_state['page']
    if page == "Input":
        display_input_section()
    elif page == "Configuration":
        display_config_section()
    elif page == "Monitoring":
        display_monitoring_section()
    elif page == "Output":
        display_output_section()

    logging.info(f"Displayed page: {page}")

