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
    preview_df = pd.DataFrame()
    encodings_to_try = ["utf-8", "cp1252", "latin1", "iso-8859-1"]
    error_messages = []

    for encoding in encodings_to_try:
        try:
            bytesio.seek(0)  # Reset stream position for each attempt
            # First attempt: auto-detect separator with current encoding
            temp_df = pd.read_csv(
                bytesio,
                nrows=6,
                sep=None,
                engine="python",
                encoding=encoding,
                dtype=str,
            )

            # Check if columns were parsed correctly
            if (
                len(temp_df.columns) <= 1 and temp_df.shape[0] > 0
            ):  # Check if there's data
                if "," in temp_df.columns[0]:
                    logging.warning(
                        f"Auto-detected separator might be wrong with {encoding}, trying comma explicitly."
                    )
                    bytesio.seek(0)
                    temp_df = pd.read_csv(
                        bytesio, nrows=6, sep=",", encoding=encoding, dtype=str
                    )
                elif ";" in temp_df.columns[0]:
                    logging.warning(
                        f"Auto-detected separator might be wrong with {encoding}, trying semicolon explicitly."
                    )
                    bytesio.seek(0)
                    temp_df = pd.read_csv(
                        bytesio, nrows=6, sep=";", encoding=encoding, dtype=str
                    )

            # If successfully read, prepare to return
            header_columns = temp_df.columns.tolist()
            preview_df = temp_df.head(5)  # Ensure we only pass 5 rows for preview
            successful_read_params = {
                "encoding": encoding,
                "separator": "auto-detected",
            }
            try:
                logging.info(
                    f"Successfully read CSV preview with encoding: {encoding}, separator: {successful_read_params['separator']}"
                )
            except Exception as ui_log_ex:
                logging.warning(
                    f"Error during logging/UI success message for preview: {ui_log_ex}"
                )
            return preview_df, header_columns, True

        except UnicodeDecodeError as e_unicode:
            error_messages.append(f"Encoding {encoding} failed: {e_unicode}")
            logging.warning(f"CSV read failed with encoding {encoding}: {e_unicode}")
            if encoding == encodings_to_try[-1]:  # If this was the last encoding
                logging.error(
                    f"All attempted encodings failed. Last error: {e_unicode}",
                    exc_info=True,
                )
                raise ValueError(
                    "Could not parse CSV file. Tried encodings: "
                    f"{', '.join(encodings_to_try)}. Check format, encoding, and separator."
                ) from e_unicode
        except Exception as e_read:
            logging.warning(
                f"CSV read failed with auto/comma separator using {encoding}: {e_read}. Trying semicolon."
            )
            try:
                bytesio.seek(0)
                temp_df = pd.read_csv(
                    bytesio, nrows=6, sep=";", encoding=encoding, dtype=str
                )
                header_columns = temp_df.columns.tolist()
                preview_df = temp_df.head(5)  # Ensure we only pass 5 rows for preview
                successful_read_params = {
                    "encoding": encoding,
                    "separator": ";",
                }
                try:
                    logging.info(
                        f"Successfully read CSV preview with encoding: {encoding}, separator: {successful_read_params['separator']}"
                    )
                except Exception as ui_log_ex:
                    logging.warning(
                        f"Error during logging/UI success message for preview: {ui_log_ex}"
                    )
                return preview_df, header_columns, True

            except Exception as e_read_semi:
                error_messages.append(
                    f"Encoding {encoding} with ';' separator failed: {e_read_semi}"
                )
                logging.warning(
                    f"CSV read failed with semicolon separator using {encoding}: {e_read_semi}"
                )
                if encoding == encodings_to_try[-1]:  # If this was the last encoding
                    logging.error(
                        f"All attempted encodings and common separators failed. Last error: {e_read_semi}",
                        exc_info=True,
                    )
                    raise ValueError(
                        "Could not parse CSV file. Tried encodings: "
                        f"{', '.join(encodings_to_try)} and common separators. "
                        "Check format, encoding, and separator."
                    ) from e_read_semi

    logging.error("CSV parsing failed after trying all encodings and separators.")
    return pd.DataFrame(), [], False


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
        bytesio = io.BytesIO(file_data.getvalue())
        preview_df, header_columns, validation_passed = _read_csv_with_error_handling(
            bytesio
        )
    except Exception as e:
        st.error(f"Error previewing file {file_data.name}: {e}")
        logging.error(f"Error previewing file {file_data.name}: {e}", exc_info=True)
        validation_passed = False
        preview_df = pd.DataFrame()
        header_columns = []

    if preview_df is not None and not preview_df.empty:
        st.dataframe(preview_df)
        if _validate_columns_func:
            validation_results, all_found = _validate_columns_func(
                header_columns, _req_cols_map
            )
            validation_passed = all_found

            for canonical_name, (found, actual_name) in validation_results.items():
                col_display_name = f"'{canonical_name}'"
                if actual_name and actual_name != canonical_name:
                    col_display_name += f" (found as '{actual_name}')"
                if found:
                    st.success(f"Required column {col_display_name} found.")
                else:
                    st.error(f"Required column {col_display_name} NOT found.")
                    validation_passed = False
        else:
            st.warning("Column validation function not available.")
            validation_passed = False
    elif validation_passed:
        if not header_columns:
            st.info("File is empty or does not contain data rows for preview.")

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
