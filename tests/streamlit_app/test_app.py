# Correct import order: stdlib -> third-party -> local
import io
import logging
import os
import sys
import unittest
from operator import index
from unittest.mock import ANY, MagicMock, patch

import pandas as pd  # Add pandas import for DataFrame comparison

# Add the project root to the Python path to allow imports like 'from streamlit_app import app'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

# Import the components to be tested using the package structure
try:
    # Now import using the package path relative to the project root
    from streamlit_app.app import (
        StreamlitLogHandler,
        clear_other_input,  # Import the function to be patched if needed, or just patch the target string
        display_config_section,
        display_input_section,
        display_monitoring_section,
        init_session_state,
    )
except ImportError as e:
    # Handle cases where streamlit or app components might not be found
    print(f"Warning: Could not import Streamlit app components: {e}")

    # Define dummy versions if import fails, allowing test structure setup
    def init_session_state():
        pass

    class StreamlitLogHandler(logging.Handler):
        def emit(self, record):
            pass  # Dummy implementation

    def display_input_section():
        pass

    def display_config_section():
        pass

    def clear_other_input():
        pass

    # Mock streamlit itself if the import fails at the top level
    st_mock = MagicMock()
    sys.modules["streamlit"] = st_mock
    # Mock the app module itself to avoid errors if streamlit_app.app cannot be resolved
    # due to the initial ImportError
    if "streamlit_app.app" not in sys.modules:
        sys.modules["streamlit_app.app"] = MagicMock()


# --- Test Cases ---
# Use @patch('streamlit_app.app.st') to target streamlit within the app module


@patch(
    "streamlit_app.app.logging.info"
)  # Patch logging.info to prevent log side effects during init
@patch("streamlit_app.app.st")  # Patch st where it's used in the app module
class TestStreamlitAppSetup(unittest.TestCase):
    """Tests for the setup and initialization logic of the Streamlit app."""

    def test_init_session_state_initializes_defaults(self, mock_st, mock_logging_info):
        """
        Test that init_session_state sets default values in an empty session_state.
        """

        mock_st.session_state = {}  # Start with an empty session state

        init_session_state()

        # Expected defaults based on the updated app.py
        expected_defaults = {
            "_app_defaults_initialized": True,  # Flag to prevent re-initialization
            "page": "Input",
            "company_list": None,  # Will store list of dicts for processing
            "uploaded_file_data": None,  # Stores the uploaded file object
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),  # For data editor
            "input_method": "File Upload",  # Default input method
            "config": {},
            "job_status": "Idle",
            "progress": 0,
            "current_phase": "",
            "results": None,
            "log_messages": [],
            "log_queue": None,
            "status_queue": None,
            "pipeline_process": None,
            "pipeline_config": None,
            "error_message": None,
            "artifacts": None,
            "testing_mode": False,  # Flag to disable st.rerun() calls during tests
            "auto_refresh_enabled": True,
            "refresh_interval": 3.0,
        }
        # Compare DataFrames separately for robust comparison
        actual_manual_df = mock_st.session_state.pop("manual_input_df", None)
        expected_manual_df = expected_defaults.pop("manual_input_df", None)
        pd.testing.assert_frame_equal(actual_manual_df, expected_manual_df)

        # Compare the rest of the dictionary
        self.assertEqual(mock_st.session_state, expected_defaults)

    def test_init_session_state_does_not_overwrite_existing(
        self, mock_st, mock_logging_info
    ):
        """
        Test that init_session_state does not overwrite existing values.
        """
        mock_st.session_state = {
            "page": "Output",
            "job_status": "Running",
            "custom_key": "custom_value",
            "log_messages": ["Existing log"],
            "input_method": "Manual Input",  # Test existing value
            "manual_input_df": pd.DataFrame(
                [{"company name": "Test", "location": "Here", "url": "http://t.co"}]
            ),  # Test existing DF
            "_test_mode": True,  # Enable test mode to maintain old behavior
        }
        init_session_state()
        # Check that existing keys were not overwritten
        self.assertEqual(mock_st.session_state["page"], "Output")
        self.assertEqual(mock_st.session_state["job_status"], "Running")
        self.assertEqual(mock_st.session_state["custom_key"], "custom_value")
        self.assertEqual(
            mock_st.session_state["input_method"], "Manual Input"
        )  # Should remain Manual Input

        # Check existing DataFrame was not overwritten
        expected_existing_df = pd.DataFrame(
            [{"company name": "Test", "location": "Here", "url": "http://t.co"}]
        )
        pd.testing.assert_frame_equal(
            mock_st.session_state["manual_input_df"], expected_existing_df
        )

        # Check that missing default keys were added
        self.assertIn("company_list", mock_st.session_state)
        self.assertIsNone(mock_st.session_state["company_list"])

        self.assertIn("config", mock_st.session_state)
        self.assertEqual(mock_st.session_state["config"], {})
        self.assertIn("results", mock_st.session_state)
        self.assertIsNone(mock_st.session_state["results"])
        self.assertIn(
            "uploaded_file_data", mock_st.session_state
        )  # Check new key added
        self.assertIsNone(mock_st.session_state["uploaded_file_data"])

        # Check that log_messages was NOT modified by init_session_state logging
        self.assertIn("log_messages", mock_st.session_state)
        self.assertEqual(mock_st.session_state["log_messages"], ["Existing log"])


@patch("streamlit_app.app.st")  # Patch st where it's used in the app module
class TestStreamlitLogHandler(unittest.TestCase):
    """Tests for the custom StreamlitLogHandler."""

    def test_emit_appends_formatted_message_to_session_state(self, mock_st):
        """
        Test that the handler formats a log record and appends it to session_state.log_messages.
        """
        mock_st.session_state = {"log_messages": []}  # Initialize log_messages
        handler = StreamlitLogHandler()
        # Use a standard formatter for testing
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        record = logging.LogRecord(
            name="testlogger",
            level=logging.INFO,
            pathname="testpath",
            lineno=1,
            msg="Test log message",
            args=(),
            exc_info=None,
            func="test_func",
        )
        handler.emit(record)

        self.assertEqual(len(mock_st.session_state["log_messages"]), 1)
        self.assertEqual(
            mock_st.session_state["log_messages"][0], "INFO:Test log message"
        )


@patch("streamlit_app.app.st")  # Patch st where it's used in the app module
@patch("streamlit_app.app.clear_other_input")  # Patch the clear_other_input function
class TestUISections(unittest.TestCase):
    """Tests for the UI section display functions, focusing on session state updates."""

    # Add mock_clear_other_input to the arguments (order matters, patches applied bottom-up)
    def test_display_input_section_updates_state_on_upload(
        self, mock_clear_other_input, mock_st
    ):
        """
        Test that display_input_section updates session_state when a file is uploaded.
        """
        # Simulate initial state and radio button selection
        mock_st.session_state = {
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "File Upload",  # Assume user selected File Upload
            "input_method_choice": "File Upload",  # Mock the radio button's state key
            "testing_mode": True,  # Flag to disable st.rerun() calls
            "log_messages": [],  # Add log_messages to prevent KeyError
        }
        mock_st.radio.return_value = "File Upload"  # Mock radio button selection

        # Simulate file upload
        mock_file = MagicMock(spec=io.BytesIO)  # Use BytesIO spec for seek/getvalue
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = b"Company Name,Location,URL\nTestCo,TestCity,http://test.co"  # Add minimal content
        mock_st.file_uploader.return_value = mock_file

        # Mock pandas read_csv used for preview/validation
        mock_preview_df = pd.DataFrame(
            [
                {
                    "Company Name": "TestCo",
                    "Location": "TestCity",
                    "URL": "http://test.co",
                }
            ]
        )
        # Patch pd.read_csv within the app module where it's used. Remove 'as _' since the mock isn't used.
        with patch("streamlit_app.app.pd.read_csv", return_value=mock_preview_df):
            display_input_section()

            # Assert radio was called
            mock_st.radio.assert_called_once()
            # Assert file_uploader was called (since input_method is File Upload)
            mock_st.file_uploader.assert_called_once_with(
                "Upload a CSV or Excel file",
                type=["csv", "xlsx"],
                key="file_uploader_widget",
                accept_multiple_files=False,
            )
            # Assert success message for file selection
            mock_st.success.assert_any_call(
                "File 'test.csv' selected."
            )  # Check for selection message
            # Assert state update for uploaded file data
            self.assertEqual(mock_st.session_state["uploaded_file_data"], mock_file)
            # Assert other input method state was cleared
            self.assertTrue(mock_st.session_state["manual_input_df"].empty)
            # Assert clear_other_input was NOT called by the radio button's on_change
            mock_clear_other_input.assert_not_called()

    # Add mock_clear_other_input to the arguments
    def test_display_input_section_updates_state_on_manual_input(
        self, mock_clear_other_input, mock_st
    ):
        """
        Test that display_input_section updates session_state when manual input is provided.
        """
        # Simulate initial state and radio button selection
        mock_st.session_state = {
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "Manual Input",  # Assume user selected Manual Input
            "input_method_choice": "Manual Input",  # Mock the radio button's state key
            "testing_mode": True,  # Flag to disable st.rerun() calls
            "log_messages": [],  # Add log_messages to prevent KeyError
        }
        mock_st.radio.return_value = "Manual Input"  # Mock radio button selection

        # Simulate data editor returning data
        edited_df = pd.DataFrame(
            [
                {
                    "company name": "ManualCo",
                    "location": "ManualCity",
                    "url": "http://manual.co",
                }
            ]
        )
        mock_st.data_editor.return_value = edited_df

        display_input_section()

        # Assert radio was called
        mock_st.radio.assert_called_once()
        # Assert data_editor was called (since input_method is Manual Input)
        mock_st.data_editor.assert_called_once()
        # Assert state update for manual input data
        pd.testing.assert_frame_equal(
            mock_st.session_state["manual_input_df"], edited_df
        )
        # Assert other input method state was cleared
        self.assertIsNone(mock_st.session_state["uploaded_file_data"])
        # Assert clear_other_input was NOT called
        mock_clear_other_input.assert_not_called()

    # Add mock_clear_other_input to the arguments
    def test_display_config_section_updates_state(
        self, mock_clear_other_input, mock_st
    ):
        """
        Test that display_config_section updates session_state.config with widget values.
        """
        pass  # TODO after implementing the config section


from streamlit_app import app


class TestMonitoringSectionProgressBar(unittest.TestCase):
    """
    Test suite for the progress bar UI in the monitoring section.
    Focuses on the logic within the display_status_info function.
    """

    def setUp(self):
        """
        Set up mocks for Streamlit and other dependencies before each test.
        """
        # Mock streamlit and its components
        self.patcher_st = patch("streamlit_app.app.st", MagicMock())
        self.mock_st = self.patcher_st.start()

        # Configure st.fragment to be a pass-through decorator for testing
        # This means @st.fragment won't interfere with the function definition.
        self.mock_st.fragment = MagicMock(return_value=lambda func: func)

        # Mock st.columns to return two mock columns for unpacking
        self.mock_st.columns = MagicMock(return_value=(MagicMock(), MagicMock()))

        # Set up slider mock to return a float value instead of a MagicMock
        self.mock_st.slider = MagicMock(return_value=3.0)

        # Mock session_state as a dictionary that can be manipulated in tests
        # Initialize session_state with defaults as in init_session_state
        self.mock_st.session_state = {
            "page": "Input",
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "File Upload",
            "config": {},
            "job_status": "Idle",
            "progress": 0,
            "current_phase": "",
            "results": None,
            "log_messages": [],
            "log_queue": None,
            "status_queue": None,
            "pipeline_process": None,
            "pipeline_config": None,
            "error_message": None,
            "artifacts": None,
            "testing_mode": False,  # this need to be false to mimick the real app
            "auto_refresh_enabled": True,
            "refresh_interval": 3.0,  # Ensure this is a float, not a MagicMock
        }

        # Mock process_queue_messages as it's called within the tested functions
        self.patcher_pqm = patch(
            "streamlit_app.app.process_queue_messages", MagicMock()
        )
        self.mock_process_queue_messages = self.patcher_pqm.start()

        # Mock logging to avoid side effects and allow assertions if needed
        self.patcher_logging = patch("streamlit_app.app.logging", MagicMock())
        self.mock_logging = self.patcher_logging.start()

        # Default setup for pipeline_process to avoid errors in unrelated checks
        # Tests can override this if specific pipeline_process behavior is needed.
        self.mock_st.session_state["pipeline_process"] = None

    def tearDown(self):
        """
        Stop all patchers after each test.
        """
        self.patcher_st.stop()
        self.patcher_pqm.stop()
        self.patcher_logging.stop()
        self.mock_st.session_state.clear()  # Ensure clean state for next test

    def test_displayStatusInfo_JobRunningAndProgressSet_DisplaysProgressBar(self):
        """
        Tests that the progress bar is displayed with the correct value when
        job_status is "Running" and 'progress' is set.
        """
        self.mock_st.session_state["job_status"] = "Running"
        self.mock_st.session_state["progress"] = 50
        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        self.mock_st.session_state["pipeline_process"] = mock_process

        app.display_monitoring_section()

        self.mock_st.progress.assert_called_once_with(0.5)
        self.mock_st.markdown.assert_any_call(
            ANY, unsafe_allow_html=True
        )  # Status markdown

    def test_displayStatusInfo_JobRunningAndProgressNotSet_DoesNotDisplayProgressBar(
        self,
    ):
        """
        Tests that the progress bar is NOT displayed when job_status is "Running"
        but 'progress' key is missing from session_state.
        """
        # Create a new clean session_state instead of using the initialized one with defaults
        self.mock_st.session_state = {
            "job_status": "Running",
            # Explicitly remove 'progress' key
            "log_messages": [],  # Include this to avoid KeyError in other places
        }

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        self.mock_st.session_state["pipeline_process"] = mock_process

        app.display_monitoring_section()

        self.mock_st.progress.assert_not_called()

    def test_displayStatusInfo_JobCompletedAndProgressSet_DoesNotDisplayProgressBar(
        self,
    ):
        """
        Tests that the progress bar is NOT displayed when job_status is "Completed",
        even if 'progress' is set.
        """
        self.mock_st.session_state["job_status"] = "Completed"
        self.mock_st.session_state["progress"] = 75
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False  # Process not alive
        self.mock_st.session_state["pipeline_process"] = mock_process

        app.display_monitoring_section()

        self.mock_st.progress.assert_not_called()

    def test_displayStatusInfo_JobIdleAndProgressSet_DoesNotDisplayProgressBar(self):
        """
        Tests that the progress bar is NOT displayed when job_status is "Idle",
        even if 'progress' is set.
        """
        self.mock_st.session_state["job_status"] = "Idle"
        self.mock_st.session_state["progress"] = 20
        # 'pipeline_process' might be None or not alive for Idle status
        if "pipeline_process" in self.mock_st.session_state:
            del self.mock_st.session_state["pipeline_process"]

        app.display_monitoring_section()

        self.mock_st.progress.assert_not_called()

    def test_displayStatusInfo_JobErrorAndProgressSet_DoesNotDisplayProgressBarAndShowsError(
        self,
    ):
        """
        Tests that the progress bar is NOT displayed when job_status is "Error",
        even if 'progress' is set. Also checks that st.error is called.
        """
        self.mock_st.session_state["job_status"] = "Error"
        self.mock_st.session_state["progress"] = 90
        self.mock_st.session_state["error_message"] = "A test error occurred"
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        self.mock_st.session_state["pipeline_process"] = mock_process

        app.display_monitoring_section()

        self.mock_st.progress.assert_not_called()
        self.mock_st.error.assert_called_once_with(
            f"Error: {self.mock_st.session_state['error_message']}"
        )

    def test_displayStatusInfo_PipelineEndsAndJobWasRunning_ChangesStatusToCompletedAndHidesProgress(
        self,
    ):
        """
        Tests that if pipeline_process is not alive and job_status was "Running",
        the status updates to "Completed", and the progress bar is not shown.
        """
        self.mock_st.session_state["job_status"] = "Running"  # Initial status
        self.mock_st.session_state["progress"] = 99
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False  # Process has finished
        self.mock_st.session_state["pipeline_process"] = mock_process

        app.display_monitoring_section()

        self.assertEqual(self.mock_st.session_state["job_status"], "Completed")
        self.mock_st.progress.assert_not_called()  # Progress bar not shown for "Completed"
        self.mock_logging.info.assert_any_call("Pipeline process ended")

    def test_displayStatusInfo_JobIdleButProcessAlive_ChangesStatusToRunningAndShowsProgress(
        self,
    ):
        """
        Tests that if job_status is "Idle" but the pipeline_process is alive,
        the status changes to "Running" and the progress bar is displayed (if 'progress' is set).
        """
        # Create a completely fresh session_state to avoid interference from other tests
        self.mock_st.session_state = {
            "job_status": "Idle",  # Initial status
            "progress": 30,
            "log_messages": [],  # Include this to avoid KeyError in other places
        }

        # Explicitly configure button to return False (not clicked)
        self.mock_st.button.return_value = False

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True  # Process is alive
        self.mock_st.session_state["pipeline_process"] = mock_process

        # Mock process_queue_messages as it's called within the tested function
        with patch("streamlit_app.app.process_queue_messages", MagicMock()):
            app.display_monitoring_section()

        self.assertEqual(self.mock_st.session_state["job_status"], "Running")
        self.mock_st.progress.assert_called_once_with(0.30)


if __name__ == "__main__":
    unittest.main()

# Added newline at the end of the file


@patch("streamlit_app.app.st")  # Patch st where it's used in the app module
@patch("streamlit_app.app.os")  # Patch os for temp file operations
class TestProcessDataCSVValidation(unittest.TestCase):
    """Tests for the CSV upload and validation functionality in process_data."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Import process_data directly
        from streamlit_app.app import process_data

        self.process_data = process_data

        # Mock StringIO and BytesIO objects
        self.mock_stringio = MagicMock(spec=io.StringIO)
        self.mock_bytesio = MagicMock(spec=io.BytesIO)

        # Set up test data
        self.valid_csv_content = "company name,location,url\nCompany A,Location A,http://www.a.com\nCompany B,Location B,http://www.b.com"
        self.missing_columns_csv_content = (
            "name,city,website\nCompany A,Location A,http://www.a.com"
        )
        self.empty_csv_content = "company name,location,url"

    def test_process_data_validCSV_populatesCompanyList(self, mock_os, mock_st):
        """
        Test that process_data correctly processes a valid CSV file.

        When a valid CSV with all required columns is uploaded,
        the function should parse the data and populate the company_list in session_state.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
            "config": {},
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = self.valid_csv_content.encode("utf-8")

        # Mock StringIO to return the test content
        with patch("streamlit_app.app.io.StringIO", return_value=self.mock_stringio):
            self.mock_stringio.getvalue.return_value = self.valid_csv_content

            # Mock pandas read_csv to return a proper DataFrame
            valid_df = pd.DataFrame(
                {
                    "company name": ["Company A", "Company B"],
                    "location": ["Location A", "Location B"],
                    "url": ["http://www.a.com", "http://www.b.com"],
                }
            )

            with patch("streamlit_app.app.pd.read_csv", return_value=valid_df):
                # Mock tempfile.NamedTemporaryFile to avoid file operations
                mock_temp_file = MagicMock()
                mock_temp_file.name = "/tmp/test.csv"
                mock_temp_file.__enter__.return_value = mock_temp_file

                with patch(
                    "streamlit_app.app.tempfile.NamedTemporaryFile",
                    return_value=mock_temp_file,
                ):
                    # Mock Process to avoid actual process creation
                    mock_process = MagicMock()
                    with patch("streamlit_app.app.Process", return_value=mock_process):
                        # Mock Manager and Queue
                        mock_manager = MagicMock()
                        mock_queue = MagicMock()
                        mock_manager.Queue.return_value = mock_queue
                        with patch(
                            "streamlit_app.app.Manager", return_value=mock_manager
                        ):
                            self.process_data()

        # Assert session state was updated correctly
        self.assertEqual(mock_st.session_state["job_status"], "Running")
        self.assertEqual(mock_st.session_state["current_phase"], "Starting Pipeline")
        self.assertEqual(len(mock_st.session_state.get("company_list", [])), 2)
        self.assertTrue(mock_process.start.called)
        mock_st.info.assert_any_call("Starting enrichment for 2 companies...")

    def test_process_data_csvMissingRequiredColumns_showsError(self, mock_os, mock_st):
        """
        Test that process_data detects and reports missing required columns in a CSV.

        When a CSV is uploaded that is missing required columns (company name, location, url),
        the function should report an error and not start processing.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = self.missing_columns_csv_content.encode(
            "utf-8"
        )

        # Mock StringIO to return the test content
        with patch("streamlit_app.app.io.StringIO", return_value=self.mock_stringio):
            self.mock_stringio.getvalue.return_value = self.missing_columns_csv_content

            # Mock pandas read_csv to return a DataFrame with wrong columns
            invalid_df = pd.DataFrame(
                {
                    "name": ["Company A"],
                    "city": ["Location A"],
                    "website": ["http://www.a.com"],
                }
            )

            with patch("streamlit_app.app.pd.read_csv", return_value=invalid_df):
                self.process_data()

        # Assert error was shown and processing was not started
        mock_st.error.assert_called_with(
            "Uploaded file is missing required columns: company name, location, url"
        )
        self.assertEqual(mock_st.session_state["job_status"], "Error")

    def test_process_data_emptyCSV_showsWarning(self, mock_os, mock_st):
        """
        Test that process_data handles empty CSVs correctly.

        When an empty CSV (or one with no data rows) is uploaded,
        the function should show a warning and not proceed with processing.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = self.empty_csv_content.encode("utf-8")

        # Mock StringIO to return the test content
        with patch("streamlit_app.app.io.StringIO", return_value=self.mock_stringio):
            self.mock_stringio.getvalue.return_value = self.empty_csv_content

            # Mock pandas read_csv to return an empty DataFrame with correct columns
            empty_df = pd.DataFrame(columns=["company name", "location", "url"])

            with patch("streamlit_app.app.pd.read_csv", return_value=empty_df):
                self.process_data()

        # Assert warning was shown and processing was marked as completed (no data)
        mock_st.warning.assert_called_with(
            "No valid data found in the uploaded file after cleaning."
        )
        self.assertEqual(mock_st.session_state["job_status"], "Completed (No Data)")

    def test_process_data_unsupportedFileType_showsError(self, mock_os, mock_st):
        """
        Test that process_data rejects unsupported file types.

        When a file with an unsupported extension is uploaded,
        the function should show an error and not attempt to process it.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.txt"  # Unsupported extension

        self.process_data()

        # Assert error was shown
        mock_st.error.assert_called_with("Unsupported file type.")
        self.assertEqual(mock_st.session_state["job_status"], "Error")

    def test_process_data_malformedCSV_showsError(self, mock_os, mock_st):
        """
        Test that process_data handles malformed CSV files correctly.

        When a CSV file with parsing errors is uploaded,
        the function should catch the exception and show an error.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = (
            "malformed,csv,data\nrow with too many,columns,here,extra".encode("utf-8")
        )

        # Mock StringIO to return the test content
        with patch("streamlit_app.app.io.StringIO", return_value=self.mock_stringio):
            # Mock pandas read_csv to raise a parsing error
            with patch(
                "streamlit_app.app.pd.read_csv",
                side_effect=pd.errors.ParserError("Parsing error"),
            ):
                self.process_data()

        # Assert error was shown
        mock_st.error.assert_called_with(
            "Error reading or processing file: Parsing error"
        )
        self.assertEqual(mock_st.session_state["job_status"], "Error")

    def test_process_data_csvWithFormattingIssues_cleansAndProcessesData(
        self, mock_os, mock_st
    ):
        """
        Test that process_data handles CSV files with formatting issues.

        When a CSV file with formatting issues (like extra whitespace in values) is uploaded,
        the function should clean the data and continue processing if all required columns are present.
        """
        # Set up mocks
        mock_st.session_state = {
            "input_method": "File Upload",
            "uploaded_file_data": MagicMock(),
            "log_messages": [],
            "job_status": "Idle",
            "config": {},
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.csv"

        # CSV with extra whitespace in values
        formatting_issues_content = "company name,  location  ,    url    \n Company A ,  Location A  ,  http://www.a.com  "
        mock_file.getvalue.return_value = formatting_issues_content.encode("utf-8")

        # Mock StringIO to return the test content
        with patch("streamlit_app.app.io.StringIO", return_value=self.mock_stringio):
            self.mock_stringio.getvalue.return_value = formatting_issues_content

            # First return DataFrame with whitespace issues, then return cleaned DataFrame
            # to simulate the cleaning operation
            df_with_issues = pd.DataFrame(
                {
                    "company name": [" Company A "],
                    "  location  ": ["  Location A  "],
                    "    url    ": ["  http://www.a.com  "],
                }
            )

            # The cleaning operation is performed in the function, so we mock a DataFrame
            # with correctly named columns but still containing whitespace
            with patch("streamlit_app.app.pd.read_csv", return_value=df_with_issues):
                # Mock tempfile.NamedTemporaryFile to avoid file operations
                mock_temp_file = MagicMock()
                mock_temp_file.name = "/tmp/test.csv"
                mock_temp_file.__enter__.return_value = mock_temp_file

                with patch(
                    "streamlit_app.app.tempfile.NamedTemporaryFile",
                    return_value=mock_temp_file,
                ):
                    # Mock Process to avoid actual process creation
                    mock_process = MagicMock()
                    with patch("streamlit_app.app.Process", return_value=mock_process):
                        # Mock Manager and Queue
                        mock_manager = MagicMock()
                        mock_queue = MagicMock()
                        mock_manager.Queue.return_value = mock_queue
                        with patch(
                            "streamlit_app.app.Manager", return_value=mock_manager
                        ):
                            self.process_data()

        # Check if process was started despite formatting issues
        self.assertEqual(mock_st.session_state["job_status"], "Running")
        self.assertTrue(mock_process.start.called)
        # Check if data was properly cleaned and stored in company_list
        self.assertIsNotNone(mock_st.session_state.get("company_list"))
