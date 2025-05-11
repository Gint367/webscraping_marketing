# Correct import order: stdlib -> third-party -> local
import io
import logging
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

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


@patch("streamlit_app.app.st")  # Patch st where it's used in the app module
@patch(
    "streamlit_app.app.open", new_callable=unittest.mock.mock_open
)  # Mock file operations
class TestStreamlitLogHandler(unittest.TestCase):
    """Tests for the custom StreamlitLogHandler."""

    def test_emit_appends_formatted_message_to_session_state(self, mock_open, mock_st):
        """
        Test that the handler formats a log record and appends it to session_state.log_messages.
        """
        # This test is for legacy behavior - keeping for compatibility
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

    def test_streamlit_log_handler_emit_CreatesAndAppendsToAppLogFile(
        self, mock_open, mock_st
    ):
        """
        Verify that when StreamlitLogHandler is initialized and emit is called,
        the streamlit_app.log file is created in the logfiles directory and the log message is appended.
        """
        # Set up the session state
        mock_st.session_state = {}

        # Create the handler
        handler = StreamlitLogHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        # Create a log record
        record = logging.LogRecord(
            name="testlogger",
            level=logging.INFO,
            pathname="testpath",
            lineno=1,
            msg="App log message",
            args=(),
            exc_info=None,
            func="test_func",
        )

        # Call emit
        handler.emit(record)

        # Check that file was opened for appending
        mock_open.assert_called_with(handler.log_file_path, "a")

        # Check that correct content was written to the file
        mock_open().write.assert_called_with("INFO:App log message\n")

    def test_streamlit_log_handler_emit_RoutesLogToSelectedJobMessages(
        self, mock_open, mock_st
    ):
        """
        Ensure that if a selected_job_id is set and exists in active_jobs,
        logs are appended to that job's log_messages list.
        """
        # Set up the session state with selected job
        mock_st.session_state = {
            "selected_job_id": "job_123",
            "active_jobs": {"job_123": {"log_messages": ["Existing job log"]}},
        }

        # Create the handler
        handler = StreamlitLogHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        # Create a log record
        record = logging.LogRecord(
            name="testlogger",
            level=logging.INFO,
            pathname="testpath",
            lineno=1,
            msg="Job specific log message",
            args=(),
            exc_info=None,
            func="test_func",
        )

        # Call emit
        handler.emit(record)

        # Verify log was added to the selected job's log_messages
        job_logs = mock_st.session_state["active_jobs"]["job_123"]["log_messages"]
        self.assertEqual(len(job_logs), 2)
        self.assertEqual(job_logs[1], "INFO:Job specific log message")

        # Verify log was also written to app log file
        mock_open.assert_called_with(handler.log_file_path, "a")

    def test_streamlit_log_handler_emit_DoesNotRouteLogToUnselectedJobMessages(
        self, mock_open, mock_st
    ):
        """
        Verify logs are not added to a job's log_messages if that job is not the selected_job_id.
        """
        # Set up the session state with selected job and another job
        mock_st.session_state = {
            "selected_job_id": "job_123",
            "active_jobs": {
                "job_123": {"log_messages": ["Selected job log"]},
                "job_456": {"log_messages": ["Unselected job log"]},
            },
        }

        # Create the handler
        handler = StreamlitLogHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        # Create a log record
        record = logging.LogRecord(
            name="testlogger",
            level=logging.INFO,
            pathname="testpath",
            lineno=1,
            msg="Should only go to job_123",
            args=(),
            exc_info=None,
            func="test_func",
        )

        # Call emit
        handler.emit(record)

        # Verify log was added to the selected job's log_messages
        selected_job_logs = mock_st.session_state["active_jobs"]["job_123"][
            "log_messages"
        ]
        self.assertEqual(len(selected_job_logs), 2)
        self.assertEqual(selected_job_logs[1], "INFO:Should only go to job_123")

        # Verify log was NOT added to the unselected job's log_messages
        unselected_job_logs = mock_st.session_state["active_jobs"]["job_456"][
            "log_messages"
        ]
        self.assertEqual(len(unselected_job_logs), 1)
        self.assertEqual(unselected_job_logs[0], "Unselected job log")

    def test_streamlit_log_handler_emit_HandlesNoSelectedJobForJobMessagesGracefully(
        self, mock_open, mock_st
    ):
        """
        If no selected_job_id is set or it doesn't exist in active_jobs, ensure logs are still written to app log
        but don't cause errors.
        """
        # Set up the session state with no selected job
        mock_st.session_state = {
            "selected_job_id": None,
            "active_jobs": {"job_123": {"log_messages": ["Existing job log"]}},
        }

        # Create the handler
        handler = StreamlitLogHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        # Create a log record
        record = logging.LogRecord(
            name="testlogger",
            level=logging.INFO,
            pathname="testpath",
            lineno=1,
            msg="With no selected job",
            args=(),
            exc_info=None,
            func="test_func",
        )

        # Call emit
        handler.emit(record)

        # Verify no changes to active_jobs
        job_logs = mock_st.session_state["active_jobs"]["job_123"]["log_messages"]
        self.assertEqual(len(job_logs), 1)
        self.assertEqual(job_logs[0], "Existing job log")

        # Verify log was written to app log file despite no selected job
        mock_open.assert_called_with(handler.log_file_path, "a")


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

        # Mock log queue and status queue
        mock_log_queue = MagicMock()
        mock_status_queue = MagicMock()

        # Mock os.path.join to track log file path creation
        mock_os.path.join.side_effect = lambda *args: "/".join(args)
        mock_os.makedirs = MagicMock()

        # Mock time to control timestamp
        with patch("streamlit_app.app.time") as mock_time:
            mock_time.strftime.return_value = "20250509_123456"

            # Mock FileHandler to avoid actual file creation
            with patch("streamlit_app.app.logging.FileHandler") as mock_file_handler:
                # Run the function with minimal config
                run_pipeline_in_process(
                    {"test": "config"}, mock_log_queue, mock_status_queue, "job_test"
                )

                # Verify log directory was created
                mock_os.makedirs.assert_called_with(
                    f"{project_root}/logfiles", exist_ok=True
                )

                # Verify a log file with timestamp was created
                expected_log_path = (
                    f"{project_root}/logfiles/pipeline_20250509_123456.log"
                )
                # Check the call to FileHandler
                mock_file_handler.assert_called_with(expected_log_path)

    def test_run_pipeline_in_process_PutsLogsIntoJobSpecificLogQueue(
        self, mock_logging, mock_os, mock_st
    ):
        """
        Check that log messages from run_pipeline_in_process are sent to the job-specific log_queue.
        """
        from streamlit_app.app import run_pipeline_in_process

        # Mock the queues
        mock_log_queue = MagicMock()
        mock_status_queue = MagicMock()

        # Create a handler that will capture the QueueHandler that gets created
        queue_handler_instance = None
        original_handler = logging.Handler

        class MockHandler(logging.Handler):
            def __init__(self, *args, **kwargs):
                nonlocal queue_handler_instance
                if queue_handler_instance is None and args == () and kwargs == {}:
                    queue_handler_instance = self
                original_handler.__init__(self, *args, **kwargs)

            def emit(self, record):
                pass

        # Replace Handler with our mock to capture the instance
        logging.Handler = MockHandler

        try:
            # Mock FileHandler to avoid actual file creation
            with patch("streamlit_app.app.logging.FileHandler") as mock_file_handler:
                # Mock run_pipeline to avoid actual execution and set a return value
                with patch("streamlit_app.app.run_pipeline") as mock_run_pipeline:
                    # Set a return value for the mocked run_pipeline
                    mock_run_pipeline.return_value = "/tmp/test_output/results.csv"

                    # Create a proper config with required fields
                    test_config = {
                        "input_csv": "/tmp/test_input.csv",
                        "output_dir": "/tmp/test_output",
                        "test": "config",
                    }

                    # Run the function with proper config
                    run_pipeline_in_process(
                        test_config,
                        mock_log_queue,
                        mock_status_queue,
                        "job_test",
                    )

                    # Verify status messages were sent to status queue
                    # Use assert_any_call instead of assert_called_with to check for specific calls among potentially many
                    mock_status_queue.put.assert_any_call(
                        {
                            "status": "Running",
                            "progress": 0,
                            "phase": "Initializing",
                            "job_id": "job_test",
                        }
                    )

                    # Now manually trigger the queue handler's emit to verify it sends to log_queue
                    if queue_handler_instance:
                        test_record = logging.LogRecord(
                            name="test",
                            level=logging.INFO,
                            pathname="test.py",
                            lineno=1,
                            msg="Test pipeline log",
                            args=(),
                            exc_info=None,
                            func="test_func",
                        )
                        queue_handler_instance.emit(test_record)

                        # Verify the record was put on the log queue
                        mock_log_queue.put.assert_called_once()
        finally:
            # Restore original Handler class
            logging.Handler = original_handler


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
        self.assertIn("Webcrawl: Extract with LLM", job_data["phase"])
        self.assertIn("Extracting with LLM (2/4)", job_data["phase"])

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
        self.assertIn("Integration: Merge Keyword", job_data["phase"])
        self.assertIn("Merging keywords (1/2)", job_data["phase"])

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
        # This one is for the job details part when no job is selected or no jobs exist
        self.mock_st.info.assert_any_call(
            "No jobs have been started yet. Use the 'Input' section to start a new job."
        )
        self.mock_st.dataframe.assert_not_called()
        self.mock_st.selectbox.assert_not_called()  # Specifically the job selector

        # Check that markdown for job details header is not called
        job_header_found = False
        for call_args in self.mock_st.markdown.call_args_list:
            if call_args[0][0].startswith("### Job:"):
                job_header_found = True
                break
        self.assertFalse(
            job_header_found, "Job details header was unexpectedly rendered."
        )

        # process_queue_messages is called at the start and in fragments
        self.assertTrue(self.mock_process_queue_messages.called)

    def test_displayMonitoringSection_WithActiveJobs_DisplaysTableAndSelectsMostRecentJobByDefault(
        self,
    ):
        """
        Tests that the jobs table is displayed and the most recent job is
        selected by default if no job is initially selected.
        """
        current_ts = 1715256000.0  # May 9, 2025 12:00:00 PM
        self.mock_time_time.return_value = current_ts

        job1_id = "job_old"
        job2_id = "job_recent"  # More recent

        # Important: Create jobs in reverse insertion order (oldest first)
        # to test that the code still selects the most recent job
        active_jobs = {
            job1_id: {
                "status": "Completed",
                "start_time": current_ts - 200,  # Older timestamp
                "end_time": current_ts - 100,
                "progress": 100,
                "file_info": {"type": "CSV", "name": "data1.csv", "record_count": 10},
            }
        }
        # Add job2 after job1 to ensure a specific insertion order
        active_jobs[job2_id] = {
            "status": "Running",
            "start_time": current_ts - 50,  # More recent timestamp
            "progress": 50,
            "file_info": {
                "type": "Manual",
                "name": "Manual Input",
                "record_count": 5,
            },
        }

        self.mock_st.session_state["active_jobs"] = active_jobs
        self.mock_st.session_state["selected_job_id"] = None

        display_monitoring_section()

        self.mock_st.dataframe.assert_called_once()

        # Verify that the most recent job (job2_id) was selected based on timestamp, not insertion order
        self.assertEqual(self.mock_st.session_state["selected_job_id"], job2_id)
        self.mock_st.markdown.assert_any_call(f"### Job: {job2_id}")
        self.assertTrue(self.mock_process_queue_messages.called)

        self.assertEqual(self.mock_st.session_state["selected_job_id"], job2_id)
        self.mock_st.markdown.assert_any_call(f"### Job: {job2_id}")
        self.assertTrue(self.mock_process_queue_messages.called)

    def test_displayMonitoringSection_JobCancellation_CancelRunningJob_CallsCancelJobAndShowsSuccess(
        self,
    ):
        """
        Tests that cancelling a running job calls cancel_job and shows a success message.
        """
        job_id = self.test_job_id
        self.mock_st.session_state["active_jobs"] = {
            job_id: {
                "status": "Running",
                "start_time": time.time() - 60,
                "progress": 10,
                "file_info": {"type": "Test", "name": "test.csv", "record_count": 1},
            }
        }
        self.mock_st.session_state["selected_job_id"] = job_id

        self.mock_st.button.return_value = True  # Simulate button click
        self.mock_cancel_job.return_value = True

        display_monitoring_section()

        self.mock_st.button.assert_called_with(
            "Cancel Selected Job", key="cancel_job_btn"
        )
        self.mock_cancel_job.assert_called_with(job_id)
        self.mock_st.success.assert_called_with(f"Job {job_id} has been cancelled.")
        self.assertTrue(self.mock_process_queue_messages.called)

    def test_displayMonitoringSection_JobCancellation_CancelCompletedJob_ShowsWarningAndNotCancelled(
        self,
    ):
        """
        Tests that attempting to cancel a completed job shows a warning
        and cancel_job is not called.
        """
        job_id = self.test_job_id
        job_status = "Completed"
        self.mock_st.session_state["active_jobs"] = {
            job_id: {
                "status": job_status,
                "start_time": time.time() - 120,
                "end_time": time.time() - 60,
                "progress": 100,
                "file_info": {"type": "Test", "name": "test.csv", "record_count": 1},
            }
        }
        self.mock_st.session_state["selected_job_id"] = job_id

        self.mock_st.button.return_value = True  # Simulate button click

        display_monitoring_section()

        self.mock_st.button.assert_called_with(
            "Cancel Selected Job", key="cancel_job_btn"
        )
        self.mock_st.warning.assert_called_with(
            f"Job {job_id} is not running (status: {job_status}). Only running jobs can be cancelled."
        )
        self.mock_cancel_job.assert_not_called()
        self.assertTrue(self.mock_process_queue_messages.called)


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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
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

                        # Mock time.strftime for consistent job ID generation
                        with patch("streamlit_app.app.time.strftime") as mock_strftime:
                            mock_strftime.return_value = "20250509_125158"

                            with patch(
                                "streamlit_app.app.Manager", return_value=mock_manager
                            ):
                                self.process_data()

        # Assert session state was updated correctly
        # Check that a job was created
        self.assertGreater(len(mock_st.session_state["active_jobs"]), 0)

        # Get the created job (there should be only one)
        job_id = list(mock_st.session_state["active_jobs"].keys())[0]
        job_data = mock_st.session_state["active_jobs"][job_id]

        # Check job status and process
        self.assertEqual(job_data["status"], "Running")
        self.assertEqual(job_data["phase"], "Starting Pipeline")
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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
            "config": {},  # Add config to prevent KeyError
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
                # Mock time.strftime for consistent job ID generation
                with patch("streamlit_app.app.time.strftime") as mock_strftime:
                    mock_strftime.return_value = "20250509_125158"
                    self.process_data()

        # Assert error was shown and processing was not started
        mock_st.error.assert_called_with(
            "Uploaded file is missing required columns: company name, location, url"
        )

        # In the new implementation, job_status might be handled differently
        # Let's update the test to verify that no active job was created on error
        self.assertEqual(len(mock_st.session_state["active_jobs"]), 0)

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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
            "config": {},  # Add config to prevent KeyError
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
                # Mock time.strftime for consistent job ID generation
                with patch("streamlit_app.app.time.strftime") as mock_strftime:
                    mock_strftime.return_value = "20250509_125158"
                    self.process_data()

        # Assert warning was shown and processing was marked as completed (no data)
        mock_st.warning.assert_called_with(
            "No valid data found in the uploaded file after cleaning."
        )

        # In the new implementation, check that no job was created
        self.assertEqual(len(mock_st.session_state["active_jobs"]), 0)

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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
            "config": {},  # Add config to prevent KeyError
        }
        mock_file = mock_st.session_state["uploaded_file_data"]
        mock_file.name = "test.txt"  # Unsupported extension

        # Mock time.strftime for consistent job ID generation
        with patch("streamlit_app.app.time.strftime") as mock_strftime:
            mock_strftime.return_value = "20250509_125158"
            self.process_data()

        # Assert error was shown
        mock_st.error.assert_called_with("Unsupported file type.")

        # In the new implementation, check that no job was created
        self.assertEqual(len(mock_st.session_state["active_jobs"]), 0)

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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
            "config": {},  # Add config to prevent KeyError
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
                # Mock time.strftime for consistent job ID generation
                with patch("streamlit_app.app.time.strftime") as mock_strftime:
                    mock_strftime.return_value = "20250509_125158"
                    self.process_data()

        # Assert error was shown
        mock_st.error.assert_called_with(
            "Error reading or processing file: Parsing error"
        )

        # In the new implementation, check that no job was created
        self.assertEqual(len(mock_st.session_state["active_jobs"]), 0)

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
            "active_jobs": {},  # Initialize with empty active_jobs dictionary
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

                        # Mock time.strftime for consistent job ID generation
                        with patch("streamlit_app.app.time.strftime") as mock_strftime:
                            mock_strftime.return_value = "20250509_125158"

                            with patch(
                                "streamlit_app.app.Manager", return_value=mock_manager
                            ):
                                self.process_data()

        # Check if process was started despite formatting issues
        # Check that a job was created
        self.assertGreater(len(mock_st.session_state["active_jobs"]), 0)

        # Get the created job (there should be only one)
        job_id = list(mock_st.session_state["active_jobs"].keys())[0]
        job_data = mock_st.session_state["active_jobs"][job_id]

        # Check job status and process
        self.assertEqual(job_data["status"], "Running")
        self.assertTrue(mock_process.start.called)

        # Check if data was properly cleaned and stored in company_list
        self.assertIsNotNone(mock_st.session_state.get("company_list"))


if __name__ == "__main__":
    unittest.main()

# Added newline at the end of the file
