# Correct import order: stdlib -> third-party -> local
import io
import logging
import multiprocessing
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

# Add the project root to the Python path to allow imports like 'from streamlit_app import app'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

# Import the components to be tested using the package structure
try:
    # Now import using the package path relative to the project root
    # Ensure the imported function has the correct default argument
    # If the original clear_other_input does not have a default, patch it here for tests
    import types

    from streamlit_app.app import (
        REQUIRED_COLUMNS_MAP,  # Assuming this exists in app.py
        display_monitoring_section,
        init_session_state,
        # Functions that are now dependencies for display_input_section
        process_data,  # Assuming this exists in app.py
        validate_columns,  # Assuming this exists in app.py
    )
    from streamlit_app.app import (
        clear_other_input as clear_other_input_from_app,  # Alias for clarity
    )

    if (
        getattr(clear_other_input_from_app, "__defaults__", None) is None
        or clear_other_input_from_app.__defaults__ == ()
    ):

        def _patched_clear_other_input(selected_method=None):
            return clear_other_input_from_app(selected_method)

        clear_other_input_from_app = _patched_clear_other_input
    from streamlit_app.section.input_section import (
        _display_file_preview_and_validate,
        _init_dependencies,
        display_input_section,
    )  # New import path

except ImportError as e:
    # Handle cases where streamlit or app components might not be found
    print(f"Warning: Could not import Streamlit app components: {e}")

    # Define dummy versions if import fails, allowing test structure setup
    def init_session_state():
        pass

    class StreamlitLogHandler(logging.Handler):
        def emit(self, record):
            pass  # Dummy implementation

    # Updated dummy to accept new arguments
    def display_input_section(
        process_data_func,
        validate_columns_func,
        req_cols_map,
        clear_other_input_func_from_app,
    ):
        pass

    def display_config_section():
        pass

    def clear_other_input_from_app(
        selected_method=None,
    ):  # Dummy for the app's clear_other_input
        pass

    def process_data(df, input_method):  # Dummy
        return None

    def validate_columns(df, cols):  # Dummy
        return True

    REQUIRED_COLUMNS_MAP = {}  # Dummy

    def display_monitoring_section():  # Dummy
        pass

    # Mock streamlit itself if the import fails at the top level
    st_mock = MagicMock()
    sys.modules["streamlit"] = st_mock
    # Mock the app module itself to avoid errors if streamlit_app.app cannot be resolved
    # due to the initial ImportError
    if "streamlit_app.app" not in sys.modules:
        sys.modules["streamlit_app.app"] = MagicMock()
    if "streamlit_app.section.input_section" not in sys.modules:  # Mock new module path
        sys.modules["streamlit_app.section.input_section"] = MagicMock()


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
            "_app_defaults_initialized": True,
            "page": "Input",
            "company_list": None,  # Will store list of dicts for processing
            "uploaded_file_data": None,  # Stores the uploaded file object
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),  # For data editor
            "input_method": "File Upload",  # Default input method
            "config": {},
            "artifacts": None,
            "testing_mode": False,  # Flag to disable st.rerun() calls during tests
            # Auto-refresh configuration
            "auto_refresh_enabled": True,  # Auto-refresh logs by default
            "refresh_interval": 3.0,  # Default refresh interval in seconds
            # Job management
            "active_jobs": {},  # Dictionary of job_id -> job_data for all active/recent jobs
            "selected_job_id": None,  # Currently selected job for viewing details
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
        # Check for the artifacts key (which might be the replacement for results)
        self.assertIn("artifacts", mock_st.session_state)
        self.assertIsNone(mock_st.session_state["artifacts"])
        self.assertIn(
            "uploaded_file_data", mock_st.session_state
        )  # Check new key added
        self.assertIsNone(mock_st.session_state["uploaded_file_data"])

        # Check that log_messages was NOT modified by init_session_state logging
        self.assertIn("log_messages", mock_st.session_state)
        self.assertEqual(mock_st.session_state["log_messages"], ["Existing log"])


@patch(
    "streamlit_app.section.input_section.st"
)  # Patched st in the new location of display_input_section
# @patch("streamlit_app.app.clear_other_input") # Removed class-level patch for clear_other_input
class TestUISectionsOnSessionState(unittest.TestCase):
    """Tests for the UI section display functions, focusing on session state updates."""

    # mock_clear_other_input removed from arguments
    def test_display_input_section_updates_state_on_upload(
        self,
        mock_st,  # mock_clear_other_input removed
    ):
        """
        Test that display_input_section updates session_state when a file is uploaded.
        """
        # Mock dependencies for display_input_section
        mock_process_data_func = MagicMock()
        mock_validate_columns_func = MagicMock(return_value=True)
        MOCK_REQUIRED_COLUMNS_MAP = {
            "File Upload": ["Company Name", "Location", "URL"],
            "Manual Input": ["company name", "location", "url"],
        }
        mock_clear_other_input_func = MagicMock()

        # Simulate initial state and radio button selection
        mock_st.session_state = {
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "File Upload",
            "input_method_choice": "File Upload",
            "testing_mode": True,
            "log_messages": [],
        }
        mock_st.radio.return_value = "File Upload"

        mock_file = MagicMock(spec=io.BytesIO)
        mock_file.name = "test.csv"
        mock_file.getvalue.return_value = (
            b"Company Name,Location,URL\nTestCo,TestCity,http://test.co"
        )
        mock_st.file_uploader.return_value = mock_file

        mock_preview_df = pd.DataFrame(
            [
                {
                    "Company Name": "TestCo",
                    "Location": "TestCity",
                    "URL": "http://test.co",
                }
            ]
        )
        # Patch pd.read_csv within the input_section module.
        with patch(
            "streamlit_app.section.input_section.pd.read_csv",
            return_value=mock_preview_df,
        ):
            display_input_section(
                process_data_func=mock_process_data_func,
                validate_columns_func=mock_validate_columns_func,
                req_cols_map=MOCK_REQUIRED_COLUMNS_MAP,
                clear_other_input_func_from_app=mock_clear_other_input_func,
            )

            mock_st.radio.assert_called_once()
            mock_st.file_uploader.assert_called_once_with(
                "Upload a CSV or Excel file",
                type=["csv", "xlsx"],
                key="file_uploader_widget",
                accept_multiple_files=False,
            )
            mock_st.success.assert_any_call("File 'test.csv' selected.")
            self.assertEqual(mock_st.session_state["uploaded_file_data"], mock_file)
            self.assertTrue(mock_st.session_state["manual_input_df"].empty)
            mock_clear_other_input_func.assert_not_called()  # Assert on the passed mock

    def test_display_input_section_updates_state_on_manual_input(
        self,
        mock_st,  # mock_clear_other_input removed
    ):
        """
        Test that display_input_section updates session_state when manual input is provided.
        """
        # Mock dependencies for display_input_section
        mock_process_data_func = MagicMock()
        mock_validate_columns_func = MagicMock(return_value=True)
        MOCK_REQUIRED_COLUMNS_MAP = {
            "File Upload": ["Company Name", "Location", "URL"],
            "Manual Input": ["company name", "location", "url"],
        }
        mock_clear_other_input_func = MagicMock()

        mock_st.session_state = {
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "Manual Input",
            "input_method_choice": "Manual Input",
            "testing_mode": True,
            "log_messages": [],
        }
        mock_st.radio.return_value = "Manual Input"

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

        display_input_section(
            process_data_func=mock_process_data_func,
            validate_columns_func=mock_validate_columns_func,
            req_cols_map=MOCK_REQUIRED_COLUMNS_MAP,
            clear_other_input_func_from_app=mock_clear_other_input_func,
        )

        mock_st.radio.assert_called_once()
        mock_st.data_editor.assert_called_once()
        pd.testing.assert_frame_equal(
            mock_st.session_state["manual_input_df"], edited_df
        )
        self.assertIsNone(mock_st.session_state["uploaded_file_data"])
        mock_clear_other_input_func.assert_not_called()  # Assert on the passed mock

    def test_display_config_section_updates_state(
        self,
        mock_st,  # mock_clear_other_input removed
    ):
        """
        Test that display_config_section updates session_state.config with widget values.
        """
        pass  # TODO after implementing the config section


# ---- New test cases for pipeline process and queue processing ----
@patch("streamlit_app.app.st")
@patch("streamlit_app.app.os")
@patch("streamlit_app.app.logging")
class TestRunPipelineInProcess(unittest.TestCase):
    """Tests for the run_pipeline_in_process function."""

    def test_run_pipeline_in_process_CreatesDedicatedPipelineLogFile(
        self, mock_logging, mock_os, mock_st
    ):
        """
        Verify that run_pipeline_in_process creates a unique log file in the logfiles directory.
        """
        from streamlit_app.app import project_root, run_pipeline_in_process

        mock_log_queue = MagicMock()
        mock_status_queue = MagicMock()

        mock_os.path.join.side_effect = lambda *args: "/".join(args)
        mock_os.makedirs = MagicMock()

        with patch(
            "streamlit_app.app.time"
        ) as mock_time_module:  # Renamed to avoid conflict
            mock_time_module.strftime.return_value = "20250509_123456"
            # Mock FileHandler to avoid actual file creation, use _ if not asserting on the instance itself
            with patch(
                "streamlit_app.app.logging.FileHandler"
            ) as mock_file_handler_class:
                # If you need to assert on the instance, capture it:
                # mock_file_handler_instance = MagicMock()
                # mock_file_handler_class.return_value = mock_file_handler_instance
                run_pipeline_in_process(
                    {"test": "config"}, mock_log_queue, mock_status_queue, "job_test"
                )

                mock_os.makedirs.assert_called_with(
                    f"{project_root}/logfiles", exist_ok=True
                )
                expected_log_path = (
                    f"{project_root}/logfiles/pipeline_20250509_123456.log"
                )
                mock_file_handler_class.assert_called_with(expected_log_path)


@patch("streamlit_app.app.st")
@patch("streamlit_app.app.pd")
class TestProcessQueueMessages(unittest.TestCase):
    def test_process_queue_messages_ParsesProgressLog_UpdatesPhaseExtractingMachine(
        self, mock_pd, mock_st
    ):
        """
        Test that process_queue_messages parses a PROGRESS log for extracting_machine and updates the job phase accordingly.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs
        mock_st.session_state = {
            "active_jobs": {
                "job_1": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        # Create a log record for extracting_machine:clean_html
        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="PROGRESS:extracting_machine:clean_html:0/6:Starting...",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:34:56"
        log_record.levelname = "INFO"

        # Configure the log_queue to return the log record then be empty
        mock_queue = mock_st.session_state["active_jobs"]["job_1"]["log_queue"]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = log_record

        # Call the function
        process_queue_messages()

        # Verify phase was updated to the expected formatted string
        job_data = mock_st.session_state["active_jobs"]["job_1"]
        self.assertIn("Extracting Machine: Clean HTML", job_data["phase"])
        self.assertIn("Starting... (0/6)", job_data["phase"])

    def test_process_queue_messages_ParsesProgressLog_UpdatesPhaseWebcrawl(
        self, mock_pd, mock_st
    ):
        """
        Test that process_queue_messages parses a PROGRESS log for webcrawl and updates the job phase accordingly.
        """
        from streamlit_app.app import process_queue_messages

        mock_st.session_state = {
            "active_jobs": {
                "job_2": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="PROGRESS:webcrawl:extract_with_llm:2/4:Extracting with LLM",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:35:00"
        log_record.levelname = "INFO"

        mock_queue = mock_st.session_state["active_jobs"]["job_2"]["log_queue"]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = log_record

        process_queue_messages()

        job_data = mock_st.session_state["active_jobs"]["job_2"]
        self.assertIn("Webcrawl:", job_data["phase"])

    def test_process_queue_messages_ParsesProgressLog_UpdatesPhaseIntegrationPhase(
        self, mock_pd, mock_st
    ):
        """
        Test that process_queue_messages parses a PROGRESS log for integration_phase and updates the job phase accordingly.
        """
        from streamlit_app.app import process_queue_messages

        mock_st.session_state = {
            "active_jobs": {
                "job_3": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="PROGRESS:integration_phase:merge_keyword:1/2:Merging keywords",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:35:10"
        log_record.levelname = "INFO"

        mock_queue = mock_st.session_state["active_jobs"]["job_3"]["log_queue"]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = log_record

        process_queue_messages()

        job_data = mock_st.session_state["active_jobs"]["job_3"]
        self.assertIn("Integration Phase:", job_data["phase"])

    def test_process_queue_messages_ParsesProgressLog_UnknownPhaseFallback(
        self, mock_pd, mock_st
    ):
        """
        Test that process_queue_messages falls back to sensible formatting for unknown PROGRESS log phases.
        """
        from streamlit_app.app import process_queue_messages

        mock_st.session_state = {
            "active_jobs": {
                "job_4": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="PROGRESS:unknown_component:unknown_sub:3/5:Doing something",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:35:20"
        log_record.levelname = "INFO"

        mock_queue = mock_st.session_state["active_jobs"]["job_4"]["log_queue"]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = log_record

        process_queue_messages()

        job_data = mock_st.session_state["active_jobs"]["job_4"]
        self.assertIn("Unknown Component: Unknown Sub", job_data["phase"])
        self.assertIn("Doing something (3/5)", job_data["phase"])

    def test_process_queue_messages_ParsesProgressLog_HandlesShortProgressLine(
        self, mock_pd, mock_st
    ):
        """
        Test that process_queue_messages handles a short PROGRESS log line gracefully.
        """
        from streamlit_app.app import process_queue_messages

        mock_st.session_state = {
            "active_jobs": {
                "job_5": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="PROGRESS:Just a short progress message",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:35:30"
        log_record.levelname = "INFO"

        mock_queue = mock_st.session_state["active_jobs"]["job_5"]["log_queue"]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = log_record

        process_queue_messages()

        job_data = mock_st.session_state["active_jobs"]["job_5"]
        self.assertIn("Just a short progress message", job_data["phase"])

    """Tests for the process_queue_messages function."""

    def test_process_queue_messages_UpdatesStatusProgressPhaseForSpecificJobFromStatusQueue(
        self, mock_pd, mock_st
    ):
        """
        Verify that status updates from a job's status_queue are correctly applied to that job's data.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                }
            }
        }

        # Configure the status_queue to return a status update then be empty
        mock_queue = mock_st.session_state["active_jobs"]["job_123"]["status_queue"]
        status_update = {
            "status": "Running",
            "progress": 50,
            "phase": "Processing data",
        }
        mock_queue.empty.side_effect = [
            False,
            True,
        ]  # Not empty on first call, empty on second
        mock_queue.get_nowait.return_value = status_update

        # Call the function
        process_queue_messages()

        # Verify job status was updated
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(job_data["status"], "Running")
        self.assertEqual(job_data["progress"], 50)
        self.assertEqual(job_data["phase"], "Processing data")

    def test_process_queue_messages_AppendsFormattedLogsForSpecificJobFromLogQueue(
        self, mock_pd, mock_st
    ):
        """
        Verify that log records from a job's log_queue are correctly formatted and appended to that job's log_messages.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 0,
                    "phase": "Initializing",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": ["Existing log message"],
                }
            }
        }

        # Create a log record
        log_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="New log message",
            args=(),
            exc_info=None,
            func="test_func",
        )
        log_record.asctime = "2025-05-09 12:34:56"  # Add timestamp for formatting
        log_record.levelname = "INFO"

        # Configure the log_queue to return a log record then be empty
        mock_queue = mock_st.session_state["active_jobs"]["job_123"]["log_queue"]
        mock_queue.empty.side_effect = [
            False,
            True,
        ]  # Not empty on first call, empty on second
        mock_queue.get_nowait.return_value = log_record

        # Call the function
        process_queue_messages()

        # Verify job log messages were updated
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(len(job_data["log_messages"]), 2)
        self.assertEqual(job_data["log_messages"][0], "Existing log message")
        self.assertEqual(
            job_data["log_messages"][1], "2025-05-09 12:34:56 - INFO - New log message"
        )

    def test_process_queue_messages_HandlesJobCompletionStatusUpdatesResultsOutputPathAndEndTime(
        self, mock_pd, mock_st
    ):
        """
        Verify that Completed status with output_path loads results and sets output_path and end_time.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 50,
                    "phase": "Processing data",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                }
            }
        }

        # Create a mock DataFrame for the results
        mock_results_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_pd.read_csv.return_value = mock_results_df

        # Configure the status_queue to return a completion update then be empty
        mock_queue = mock_st.session_state["active_jobs"]["job_123"]["status_queue"]
        completion_update = {
            "status": "Completed",
            "progress": 100,
            "phase": "Finished",
            "output_path": "/path/to/results.csv",
        }
        mock_queue.empty.side_effect = [
            False,
            True,
        ]  # Not empty on first call, empty on second
        mock_queue.get_nowait.return_value = completion_update

        # Mock time for end_time verification
        current_time = time.time()
        with patch("streamlit_app.app.time.time", return_value=current_time):
            # Call the function
            process_queue_messages()

        # Verify job status and results were updated
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(job_data["status"], "Completed")
        self.assertEqual(job_data["progress"], 100)
        self.assertEqual(job_data["phase"], "Finished")
        self.assertEqual(job_data["output_path"], "/path/to/results.csv")
        self.assertEqual(job_data["end_time"], current_time)
        pd.testing.assert_frame_equal(job_data["results"], mock_results_df)
        mock_pd.read_csv.assert_called_with("/path/to/results.csv")

    def test_process_queue_messages_HandlesJobErrorStatusUpdatesErrorMessageAndEndTime(
        self, mock_pd, mock_st
    ):
        """
        Verify that Error status with error detail sets error_message and end_time.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 50,
                    "phase": "Processing data",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                }
            }
        }

        # Configure the status_queue to return an error update then be empty
        mock_queue = mock_st.session_state["active_jobs"]["job_123"]["status_queue"]
        error_update = {
            "status": "Error",
            "progress": 50,
            "phase": "Failed",
            "error": "Test error message",
        }
        mock_queue.empty.side_effect = [
            False,
            True,
        ]  # Not empty on first call, empty on second
        mock_queue.get_nowait.return_value = error_update

        # Mock time for end_time verification
        current_time = time.time()
        with patch("streamlit_app.app.time.time", return_value=current_time):
            # Call the function
            process_queue_messages()

        # Verify job status and error were updated
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(job_data["status"], "Error")
        self.assertEqual(job_data["phase"], "Failed")
        self.assertEqual(job_data["error_message"], "Test error message")
        self.assertEqual(job_data["end_time"], current_time)

    def test_process_queue_messages_HandlesEmptyJobQueuesGracefully(
        self, mock_pd, mock_st
    ):
        """
        Verify that process_queue_messages handles empty queues without errors.
        """
        from streamlit_app.app import process_queue_messages

        # Set up mock session state with active jobs that have empty queues
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 50,
                    "phase": "Processing data",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "log_messages": [],
                }
            }
        }

        # Configure all queues to be empty
        mock_st.session_state["active_jobs"]["job_123"][
            "status_queue"
        ].empty.return_value = True
        mock_st.session_state["active_jobs"]["job_123"][
            "log_queue"
        ].empty.return_value = True

        # Call the function - should complete without errors
        process_queue_messages()

        # Verify job status remains unchanged
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(job_data["status"], "Running")
        self.assertEqual(job_data["progress"], 50)
        self.assertEqual(job_data["phase"], "Processing data")
        self.assertEqual(job_data["log_messages"], [])

    def test_process_queue_messages_UpdatesStatusIfJobProcessDiedUnexpectedly(
        self, mock_pd, mock_st
    ):
        """
        If a job's process is no longer alive but status is still Running, update to Completed with appropriate message.
        """
        from streamlit_app.app import process_queue_messages

        # Create a mock process that's no longer alive
        mock_process = MagicMock()
        mock_process.is_alive.return_value = False

        # Set up mock session state with a job with a dead process but running status
        mock_st.session_state = {
            "active_jobs": {
                "job_123": {
                    "status": "Running",
                    "progress": 50,
                    "phase": "Processing data",
                    "status_queue": MagicMock(),
                    "log_queue": MagicMock(),
                    "process": mock_process,
                    "log_messages": [],
                }
            }
        }

        # Configure queues to be empty to simulate no last messages
        mock_st.session_state["active_jobs"]["job_123"][
            "status_queue"
        ].empty.return_value = True
        mock_st.session_state["active_jobs"]["job_123"][
            "log_queue"
        ].empty.return_value = True

        # Mock time for end_time verification
        current_time = time.time()
        with patch("streamlit_app.app.time.time", return_value=current_time):
            # Call the function
            process_queue_messages()

        # Verify job status was updated to Completed due to dead process
        job_data = mock_st.session_state["active_jobs"]["job_123"]
        self.assertEqual(job_data["status"], "Completed")
        self.assertEqual(job_data["phase"], "Finished (Status not properly updated)")
        self.assertEqual(job_data["end_time"], current_time)


class TestMonitoringSectionStatusAndProgressBar(unittest.TestCase):
    """Tests for the monitoring section of the Streamlit app."""

    def setUp(self):
        """
        Set up mocks for Streamlit and other dependencies before each test.
        """
        self.patcher_st = patch("streamlit_app.app.st", MagicMock())
        self.mock_st = self.patcher_st.start()

        self.mock_st.fragment = MagicMock(return_value=lambda func: func)
        self.mock_st.columns = MagicMock(return_value=(MagicMock(), MagicMock()))
        self.mock_st.slider = MagicMock(return_value=3.0)
        self.mock_st.container = MagicMock()
        self.mock_st.container.return_value.__enter__ = MagicMock(return_value=None)
        self.mock_st.container.return_value.__exit__ = MagicMock(return_value=None)
        self.mock_st.expander = MagicMock()
        self.mock_st.expander.return_value.__enter__ = MagicMock(return_value=None)
        self.mock_st.expander.return_value.__exit__ = MagicMock(return_value=None)

        self.test_job_id = "job_20250509_123456"

        self.mock_st.session_state = {
            "page": "Input",
            "company_list": None,
            "uploaded_file_data": None,
            "manual_input_df": pd.DataFrame(
                columns=["company name", "location", "url"]
            ),
            "input_method": "File Upload",
            "config": {},
            "active_jobs": {},
            "selected_job_id": None,
            "testing_mode": False,
            "auto_refresh_enabled": True,
            "refresh_interval": 3.0,
        }

        # Mock process_queue_messages as it's called within the tested functions
        self.patcher_pqm = patch(
            "streamlit_app.app.process_queue_messages", MagicMock()
        )
        self.mock_process_queue_messages = self.patcher_pqm.start()

        # Mock logging to avoid side effects and allow assertions if needed
        self.patcher_logging = patch("streamlit_app.app.logging", MagicMock())
        self.mock_logging = self.patcher_logging.start()

        # Mock cancel_job
        self.patcher_cancel_job = patch("streamlit_app.app.cancel_job", MagicMock())
        self.mock_cancel_job = self.patcher_cancel_job.start()

        # Patch time.time for consistent duration calculations
        self.patcher_time_time = patch(
            "streamlit_app.app.time.time", MagicMock(return_value=766015200.0)
        )  # Example: May 9, 2025 12:00:00 PM
        self.mock_time_time = self.patcher_time_time.start()

        # Patch time.strftime for consistent time formatting
        self.patcher_time_strftime = patch(
            "streamlit_app.app.time.strftime",
            MagicMock(return_value="2025-05-09 12:00:00"),
        )
        self.mock_time_strftime = self.patcher_time_strftime.start()

        def generic_selectbox_side_effect(*args_effect, **kwargs_effect):
            options_val = kwargs_effect.get("options", [])
            index_val = kwargs_effect.get("index", 0)
            if isinstance(options_val, list) and options_val:
                if 0 <= index_val < len(options_val):
                    return options_val[index_val]
                return options_val[0]  # Fallback to first option
            return None  # No options or not a list

        self.mock_st.selectbox = MagicMock(side_effect=generic_selectbox_side_effect)
        self.mock_st.button = MagicMock(return_value=False)  # Default to not clicked

    def tearDown(self):
        """
        Stop all patchers after each test.
        """
        self.patcher_st.stop()
        self.patcher_pqm.stop()
        self.patcher_logging.stop()
        self.patcher_cancel_job.stop()
        self.patcher_time_time.stop()
        self.patcher_time_strftime.stop()
        self.mock_st.session_state.clear()  # Ensure clean state for next test

    def test_displayMonitoringSection_NoActiveJobs_ShowsInfoAndNoJobSpecificUI(self):
        """
        Tests that an info message is shown and no job-specific UI elements
        are rendered when there are no active jobs.
        """
        self.mock_st.session_state["active_jobs"] = {}
        self.mock_st.session_state["selected_job_id"] = None

        display_monitoring_section()

        self.mock_st.info.assert_any_call(
            "No jobs have been run yet. Start a new job from the Input section."
        )
        # This one is for the job details part when no job is selected or no job


# ---- New test cases for job management functions ----
@patch("streamlit_app.app.st")
@patch("streamlit_app.app.time")  # For mocking time.strftime and time.time
@patch("streamlit_app.app.app_logger")  # For mocking app_logger
class TestJobManagementFunctions(unittest.TestCase):
    """Tests for job management utility functions."""

    def test_generate_job_id_ReturnsStringStartingWithJobPrefix_ContainsTimestampLikeStructure(
        self, mock_app_logger, mock_time, mock_st
    ):
        """
        Checks if the generated ID has the correct prefix and a timestamp-like format.
        """
        # Mock time.strftime to return a predictable timestamp
        mock_time.strftime.return_value = "20250513_103000"

        # Import generate_job_id here to use the mocked time
        from streamlit_app.app import generate_job_id

        job_id = generate_job_id()

        self.assertTrue(job_id.startswith("job_"))
        timestamp_part = job_id[len("job_") :]
        self.assertEqual(timestamp_part, "20250513_103000")
        self.assertEqual(len(timestamp_part), 15)  # YYYYMMDD_HHMMSS
        self.assertTrue(all(c.isdigit() or c == "_" for c in timestamp_part))
        mock_time.strftime.assert_called_once_with("%Y%m%d_%H%M%S")

    def test_cancel_job_JobExistsAndProcessAlive_TerminatesProcessUpdatesStatusAndReturnsTrue(
        self, mock_app_logger, mock_time, mock_st
    ):
        """
        Tests successful cancellation of a running job.
        """
        # Import cancel_job here
        from streamlit_app.app import cancel_job

        mock_process = MagicMock(spec=multiprocessing.Process)
        mock_process.is_alive.return_value = True
        job_id = "job_test123"
        mock_st.session_state = {
            "active_jobs": {
                job_id: {
                    "process": mock_process,
                    "status": "Running",
                    "phase": "Processing",
                }
            }
        }
        mock_time.time.return_value = 1234567890.0  # Mock current time

        result = cancel_job(job_id)

        self.assertTrue(result)
        mock_process.terminate.assert_called_once()
        self.assertEqual(
            mock_st.session_state["active_jobs"][job_id]["status"], "Cancelled"
        )
        self.assertEqual(
            mock_st.session_state["active_jobs"][job_id]["phase"],
            "Terminated by user",
        )
        self.assertEqual(
            mock_st.session_state["active_jobs"][job_id]["end_time"], 1234567890.0
        )
        mock_app_logger.info.assert_called_with(f"Job {job_id} was cancelled by user")

    def test_cancel_job_JobIdNotFound_ReturnsFalse(
        self, mock_app_logger, mock_time, mock_st
    ):
        """
        Tests behavior when the job ID doesn't exist.
        """
        # Import cancel_job here
        from streamlit_app.app import cancel_job

        mock_st.session_state = {"active_jobs": {}}
        job_id = "non_existent_job"

        result = cancel_job(job_id)

        self.assertFalse(result)
        mock_app_logger.info.assert_not_called()
        mock_app_logger.error.assert_not_called()

    def test_cancel_job_ProcessNotAlive_ReturnsFalseDoesNotChangeStatus(
        self, mock_app_logger, mock_time, mock_st
    ):
        """
        Tests when the job process is already dead.
        """
        # Import cancel_job here
        from streamlit_app.app import cancel_job

        mock_process = MagicMock(spec=multiprocessing.Process)
        mock_process.is_alive.return_value = False
        job_id = "job_dead_process"
        initial_job_data = {
            "process": mock_process,
            "status": "Running",  # Should remain unchanged
            "phase": "Processing",
        }
        mock_st.session_state = {"active_jobs": {job_id: initial_job_data.copy()}}

        result = cancel_job(job_id)

        self.assertFalse(result)
        mock_process.terminate.assert_not_called()
        self.assertEqual(
            mock_st.session_state["active_jobs"][job_id]["status"], "Running"
        )  # Status unchanged
        mock_app_logger.info.assert_not_called()
        mock_app_logger.error.assert_not_called()

    def test_cancel_job_ProcessTerminateRaisesException_LogsErrorReturnsFalse(
        self, mock_app_logger, mock_time, mock_st
    ):
        """
        Tests error handling during process termination.
        """
        # Import cancel_job here
        from streamlit_app.app import cancel_job

        mock_process = MagicMock(spec=multiprocessing.Process)
        mock_process.is_alive.return_value = True
        mock_process.terminate.side_effect = OSError("Termination failed")
        job_id = "job_terminate_fail"
        initial_job_data = {
            "process": mock_process,
            "status": "Running",
            "phase": "Processing",
        }
        mock_st.session_state = {"active_jobs": {job_id: initial_job_data.copy()}}

        result = cancel_job(job_id)

        self.assertFalse(result)
        mock_process.terminate.assert_called_once()
        mock_app_logger.error.assert_called_with(
            f"Failed to cancel job {job_id}: Termination failed"
        )
        # Status should ideally remain 'Running' or be set to an error state,
        # but current implementation doesn't change it on exception.
        self.assertEqual(
            mock_st.session_state["active_jobs"][job_id]["status"], "Running"
        )
        mock_app_logger.info.assert_not_called()


# ---- New test cases for input file encoding ----
@patch("streamlit_app.section.input_section.st")
@patch(
    "streamlit_app.section.input_section.logging"
)  # Corrected patch target for logging
@patch("streamlit_app.section.input_section._validate_columns_func")
class TestInputFileSection(unittest.TestCase):
    """Tests for input file handling, especially encoding, in input_section.py,
    focusing on the _display_file_preview_and_validate function."""

    def setUp(self):
        """Set up patches for module-level dependencies."""
        # Initialize dependencies for the input_section module, as we are testing an internal function.
        # We provide a mock for _validate_columns_func (which is already patched by the class decorator for the test methods)
        # and the actual REQUIRED_COLUMNS_MAP for _req_cols_map.
        # The other dependencies can be None or MagicMock if not directly used by _display_file_preview_and_validate.
        _init_dependencies(
            process_data_func=MagicMock(),
            validate_columns_func=MagicMock(),  # This will be the instance patched by the test method decorator
            req_cols_map=REQUIRED_COLUMNS_MAP,
            clear_other_input_func=MagicMock(),
        )

    def _create_mock_uploaded_file(
        self, content_str: str, encoding: str, filename: str = "test.csv"
    ):
        """Helper to create a mock UploadedFile-like object."""
        mock_file = MagicMock()
        mock_file.name = filename
        # getvalue() is called by _display_file_preview_and_validate to pass to BytesIO
        mock_file.getvalue = MagicMock(return_value=content_str.encode(encoding))
        mock_file.type = "text/csv"
        return mock_file

    def test_displayFilePreview_ValidUtf8File_ShowsSuccessAndDataFrame(
        self, mock_validate_columns_func_arg, mock_logging, mock_st
    ):
        """Tests if a UTF-8 encoded file is correctly parsed and displayed."""
        # The mock_validate_columns_func_arg is the one from the @patch decorator for the method.
        # The _init_dependencies in setUp has already set the module-level _validate_columns_func
        # to a MagicMock. The decorator replaces that MagicMock with mock_validate_columns_func_arg for this test.
        # So, we configure mock_validate_columns_func_arg.

        csv_content = "company name,location,url\nCompanyÄ,Berlin,http://test.com/äöü"
        mock_uploaded_file = self._create_mock_uploaded_file(csv_content, "utf-8")

        expected_df = pd.DataFrame(
            {
                "company name": ["CompanyÄ"],
                "location": ["Berlin"],
                "url": ["http://test.com/äöü"],
            }
        )
        mock_validate_columns_func_arg.return_value = (
            {
                "company name": (True, "company name"),
                "location": (True, "location"),
                "url": (True, "url"),
            },
            True,
        )

        _display_file_preview_and_validate(mock_uploaded_file)

        # Assert that st.dataframe was called with the first 5 rows of the expected_df
        mock_st.dataframe.assert_called_once()
        called_df = mock_st.dataframe.call_args[0][0]
        pd.testing.assert_frame_equal(called_df, expected_df.head(5))

        # Assert that the (now correctly patched) _validate_columns_func was called
        mock_validate_columns_func_arg.assert_called_once_with(
            expected_df.columns.tolist(), REQUIRED_COLUMNS_MAP
        )
        mock_st.error.assert_not_called()
        # Assuming _read_csv_with_error_handling (called by _display_file_preview_and_validate)
        # calls st.logging.info upon successful read. The exact success message for st.success might vary.
        # For now, we check that no st.error was called.

    def test_displayFilePreview_ValidLatin1File_ShowsSuccessAndDataFrame(
        self, mock_validate_columns_func_arg, mock_logging, mock_st
    ):
        """Tests if a Latin-1 encoded file is correctly parsed and displayed."""
        csv_content = "company name,location,url\nFirma ß,München,http://test.com/café"
        mock_uploaded_file = self._create_mock_uploaded_file(csv_content, "latin1")

        expected_df = pd.DataFrame(
            {
                "company name": ["Firma ß"],
                "location": ["München"],
                "url": ["http://test.com/café"],
            }
        )
        mock_validate_columns_func_arg.return_value = (
            {
                "company name": (True, "company name"),
                "location": (True, "location"),
                "url": (True, "url"),
            },
            True,
        )

        _display_file_preview_and_validate(mock_uploaded_file)

        mock_st.dataframe.assert_called_once()
        called_df = mock_st.dataframe.call_args[0][0]
        pd.testing.assert_frame_equal(called_df, expected_df.head(5))
        mock_validate_columns_func_arg.assert_called_once_with(
            expected_df.columns.tolist(), REQUIRED_COLUMNS_MAP
        )
        mock_st.error.assert_not_called()

    def test_displayFilePreview_ValidCp1252File_ShowsSuccessAndDataFrame(
        self, mock_validate_columns_func_arg, mock_logging, mock_st
    ):
        """Tests if a CP1252 encoded file is correctly parsed and displayed."""
        csv_content = (
            "company name,location,url\nMyCo € AG,Zürich,http://test.com/„quote“"
        )
        mock_uploaded_file = self._create_mock_uploaded_file(csv_content, "cp1252")

        expected_df = pd.DataFrame(
            {
                "company name": ["MyCo € AG"],
                "location": ["Zürich"],
                "url": ["http://test.com/„quote“"],
            }
        )
        mock_validate_columns_func_arg.return_value = (
            {
                "company name": (True, "company name"),
                "location": (True, "location"),
                "url": (True, "url"),
            },
            True,
        )
        _display_file_preview_and_validate(mock_uploaded_file)

        mock_st.dataframe.assert_called_once()
        called_df = mock_st.dataframe.call_args[0][0]
        pd.testing.assert_frame_equal(called_df, expected_df.head(5))
        mock_validate_columns_func_arg.assert_called_once_with(
            expected_df.columns.tolist(), REQUIRED_COLUMNS_MAP
        )
        mock_st.error.assert_not_called()

    def test_displayFilePreview_InvalidFileForAllEncodings_ReturnsFalse(
        self, mock_validate_columns_func_arg, mock_logging, mock_st
    ):
        """Tests if False is returned when a file cannot be parsed by _read_csv_with_error_handling."""
        # This test focuses on how _display_file_preview_and_validate behaves when _read_csv_with_error_handling indicates a failure.
        with patch(
            "streamlit_app.section.input_section._read_csv_with_error_handling"
        ) as mock_read_csv:
            mock_uploaded_file = self._create_mock_uploaded_file(
                "invalid,content\n\x81,data", "utf-8"
            )  # Encoding for getvalue

            # Simulate _read_csv_with_error_handling failing and returning (empty_df, empty_cols, False)
            mock_read_csv.return_value = (pd.DataFrame(), [], False)

            result = _display_file_preview_and_validate(mock_uploaded_file)

            self.assertFalse(
                result
            )  # _display_file_preview_and_validate should return the False from _read_csv_with_error_handling
            mock_read_csv.assert_called_once()
            mock_validate_columns_func_arg.assert_not_called()  # Because header_columns would be empty

            # _display_file_preview_and_validate itself does not call st.error if _read_csv_with_error_handling
            # returns validation_passed=False. _read_csv_with_error_handling is responsible for st.error in that case.
            # So, we don't assert mock_st.error here as _read_csv_with_error_handling is mocked.
            # We assert that no st.dataframe was called as there's no valid preview.
            mock_st.dataframe.assert_not_called()
            mock_st.success.assert_not_called()


# ...existing code...
