"""
Input section module for the Streamlit app.
Contains all functions related to the data input UI and processing.
"""

import io
import logging

import pandas as pd
import streamlit as st


# --- Module-level placeholders for dependencies ---
_process_data_func = None
_validate_columns_func = None
_req_cols_map = None
_clear_other_input_func = None


def _init_dependencies(
    process_data_func, validate_columns_func, req_cols_map, clear_other_input_func
):
    """Initializes module-level dependencies."""
    global \
        _process_data_func, \
        _validate_columns_func, \
        _req_cols_map, \
        _clear_other_input_func
    _process_data_func = process_data_func
    _validate_columns_func = validate_columns_func
    _req_cols_map = req_cols_map
    _clear_other_input_func = clear_other_input_func


def _display_input_method_selector() -> str:
    """
    Displays the radio button for selecting the input method.

    Returns:
        str: The selected input method value.
    """
    return st.radio(
        "Select Input Method:",
        ("File Upload", "Manual Input"),
        key="input_method_choice",
        horizontal=True,
        label_visibility="collapsed",
    )


def _handle_input_method_change(selected_method: str):
    """
    Handles the logic when the input method changes.

    Args:
        selected_method: The newly selected input method.
    """
    if (
        "input_method" not in st.session_state
        or st.session_state["input_method"] != selected_method
    ):
        # For real app, but not for tests
        if hasattr(st.session_state, "running_test") and st.session_state.running_test:
            st.session_state["input_method"] = selected_method
        else:
            # Normal app flow
            old_method = st.session_state.get("input_method")
            if _clear_other_input_func:  # Check if the function is initialized
                _clear_other_input_func(selected_method)
            st.session_state["input_method"] = selected_method

            # Only rerun if input method actually changed (not on first load)
            # Skip rerun if in testing mode
            if (
                old_method
                and old_method != selected_method
                and not st.session_state.get("testing_mode", False)
            ):
                st.rerun()


def _display_input_format_help():
    """Displays help text about required columns in an expandable section."""
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


def _handle_new_file_upload():
    """
    Handles the UI and logic for a new file upload.
    """
    uploaded_file = st.file_uploader(
        "Upload a CSV or Excel file",
        type=["csv", "xlsx"],
        key="file_uploader_widget",
        accept_multiple_files=False,
    )

    if uploaded_file is not None:
        # File has just been uploaded, store in session state
        st.session_state["uploaded_file_data"] = uploaded_file

        # Clear other input method's state
        st.session_state["manual_input_df"] = pd.DataFrame(
            columns=["company name", "location", "url"]
        )
        st.session_state["company_list"] = None  # Clear processed list as input changed

        # Log and notify user of successful upload
        print(f"File selected: {uploaded_file.name}")
        st.success(f"File '{uploaded_file.name}' selected.")

        # Force UI refresh to show file preview
        st.rerun()


def _read_csv_with_error_handling(bytesio) -> tuple[pd.DataFrame, list, bool]:
    """
    Reads a CSV file with multiple attempts to handle different separators.

    Args:
        bytesio: BytesIO object containing the file data

    Returns:
        tuple: (DataFrame, list of column names, validation success flag)
    """
    header_columns = []
    preview_df = None
    validation_passed = True

    try:
        # First attempt: auto-detect separator
        temp_df = pd.read_csv(bytesio, nrows=6, sep=None, engine="python")

        # Check if columns were parsed correctly - sometimes sep=None gives only one column
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
        # Second attempt: try semicolon separator if auto-detect failed
        logging.warning(
            f"CSV read failed with auto/comma separator: {e_read}. Trying semicolon."
        )
        bytesio.seek(0)
        try:
            temp_df = pd.read_csv(bytesio, nrows=6, sep=";")
        except Exception as e_read_semi:
            logging.error(
                f"CSV read failed with common separators: {e_read_semi}", exc_info=True
            )
            raise ValueError(
                "Could not parse CSV file. Check format, encoding, and separator."
            ) from e_read_semi

    # Process the read results
    if not temp_df.empty:
        header_columns = temp_df.columns.tolist()
        preview_df = temp_df.head(5)  # Take the first 5 rows for preview
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
        validation_passed = False

    return preview_df, header_columns, validation_passed


def _display_file_preview_and_validate(file_data) -> bool:
    """
    Displays file preview and performs column validation.

    Args:
        file_data: The uploaded file object from session state.

    Returns:
        bool: True if validation passes, False otherwise.
    """
    st.write("Preview & Column Validation:")
    preview_df = None
    header_columns = []
    validation_passed = True

    try:
        # Ensure we start from the beginning of the file
        file_data.seek(0)

        # Handle different file types
        if file_data.name.endswith(".csv"):
            # Create a BytesIO object from the file's contents
            bytesio = io.BytesIO(file_data.getvalue())
            preview_df, header_columns, validation_passed = (
                _read_csv_with_error_handling(bytesio)
            )

        elif file_data.name.endswith((".xls", ".xlsx")):
            bytesio = io.BytesIO(file_data.getvalue())
            # Read header and first 5 rows
            temp_df = pd.read_excel(bytesio, nrows=6)  # Reads header + 5 data rows
            if not temp_df.empty:
                header_columns = temp_df.columns.tolist()
                preview_df = temp_df.head(5)
            else:
                logging.warning("Pandas read Excel resulted in empty DataFrame.")
                st.warning("File appears empty or the first sheet has no data.")
                validation_passed = False
        else:
            # Unsupported file type
            st.warning("Cannot preview this file type.")
            validation_passed = False  # Cannot validate if cannot preview

        # --- Perform Column Validation ---
        if header_columns:  # Check if we actually got columns
            column_validation, validation_passed_all_cols = _validate_columns_func(
                header_columns
            )

            # Display validation results for each required column
            for canonical_name, (found, actual_name) in column_validation.items():
                aliases_str = "/".join(_req_cols_map[canonical_name])
                if found:
                    st.success(f"✔️ Found: **{canonical_name}** (as '{actual_name}')")
                else:
                    st.error(
                        f"❌ Missing: **{canonical_name}** (expected one of: {aliases_str})"
                    )

            # Update overall validation status based on columns
            if not validation_passed_all_cols:
                validation_passed = False

        elif validation_passed:
            # Only show error if no other warning/error was raised during read
            # No columns found during read attempt
            st.error(
                "Could not detect columns in the file. Please ensure the file is correctly formatted."
            )
            validation_passed = False

        # --- Display Preview DataFrame ---
        if preview_df is not None and not preview_df.empty:  # Check if preview has rows
            st.dataframe(preview_df, use_container_width=True)
        elif header_columns:  # Header found, but no data rows in the first 5
            st.info(
                "File has columns, but no data rows found in the preview (first 5 rows)."
            )

        # Reset pointer for the actual processing function later
        file_data.seek(0)

    except Exception as e:
        # Handle any errors during preview and validation
        st.error(f"Could not read or preview the file: {e}")
        logging.error(f"Error previewing file {file_data.name}: {e}", exc_info=True)
        validation_passed = False  # Error means validation fails

    return validation_passed


def _handle_existing_file(current_file_in_state) -> bool:
    """
    Handles the UI for an existing uploaded file, including preview and validation.

    Args:
        current_file_in_state: The file object stored in session state

    Returns:
        bool: Validation status of the file
    """
    st.success(f"Selected file: **{current_file_in_state.name}**")

    # Display file preview and run validation
    validation_status = _display_file_preview_and_validate(current_file_in_state)

    # "Change File" button to clear the current file
    if st.button("Change File"):
        st.session_state["uploaded_file_data"] = None
        st.session_state["company_list"] = None  # Clear processed list
        print("User clicked 'Change File'. Clearing uploaded file.")
        st.rerun()

    return validation_status


def _display_file_upload_ui() -> bool:
    """
    Manages the UI for the file upload section.

    Returns:
        bool: True if file validation passes, False otherwise
    """
    st.subheader("Upload Company List")
    _display_input_format_help()

    # Get current file state
    current_file_in_state = st.session_state.get("uploaded_file_data")
    file_validation_passed = True  # Default to true unless validation fails

    if current_file_in_state is None:
        # No file uploaded yet, show uploader
        _handle_new_file_upload()
    else:
        # File already uploaded, show preview and validation
        file_validation_passed = _handle_existing_file(current_file_in_state)

    return file_validation_passed


def _display_manual_input_ui():
    """
    Manages the UI for the manual input section.
    """
    st.subheader("Enter Data Manually")
    st.write("Add or edit company details below:")

    # Use st.data_editor for manual input
    edited_df = st.data_editor(
        st.session_state["manual_input_df"],
        num_rows="dynamic",
        key="manual_data_editor",
        column_config={  # Configure column properties
            "company name": st.column_config.TextColumn("Company Name", required=True),
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

    # Log updates if data is present
    if not edited_df.empty:
        logging.info(f"Manual input data updated. Rows: {len(edited_df)}")


def _display_start_processing_button(validation_passed: bool):
    """
    Displays the 'Start Processing' button and manages its state.

    Args:
        validation_passed: Boolean indicating if file validation has passed
    """
    st.divider()

    # Determine if processing button should be disabled
    processing_disabled = False

    if st.session_state["input_method"] == "File Upload":
        if st.session_state.get("uploaded_file_data") is None:
            processing_disabled = True  # No file uploaded
        elif not validation_passed:
            processing_disabled = True  # File uploaded but failed validation
            st.warning(
                "Cannot start processing. Please upload a file with all required columns (or fix the current one)."
            )
    elif st.session_state["input_method"] == "Manual Input":
        manual_df = st.session_state.get("manual_input_df")
        if manual_df is None or manual_df.empty:
            processing_disabled = True  # No manual data entered

    # Display the button
    if st.button("Start Processing", type="primary", disabled=processing_disabled):
        if _process_data_func:  # Check if the function is initialized
            _process_data_func()  # Call the passed function


def display_input_section(
    process_data_func,
    validate_columns_func,
    req_cols_map,
    clear_other_input_func_from_app,
):
    """
    Displays the UI for data input using radio buttons and data editor.

    This is the main entry point for the input section of the app, which calls
    various helper functions to handle different aspects of the UI and logic.
    """
    # Initialize dependencies passed from the main app
    _init_dependencies(
        process_data_func,
        validate_columns_func,
        req_cols_map,
        clear_other_input_func_from_app,
    )

    st.header("1. Input Data")
    st.write("Choose your input method:")

    # Get the selected input method
    selected_method = _display_input_method_selector()

    # Handle any input method changes
    _handle_input_method_change(selected_method)

    # Default validation to True (will be updated if file upload has issues)
    validation_status = True

    # Display the appropriate input UI based on selected method
    if st.session_state["input_method"] == "File Upload":
        validation_status = _display_file_upload_ui()
    elif st.session_state["input_method"] == "Manual Input":
        _display_manual_input_ui()
        # For manual input, validation is handled by the data_editor constraints

    # Display the Start Processing button
    _display_start_processing_button(validation_status)
