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

        # mock_log_queue is no longer a parameter for run_pipeline_in_process
        mock_status_queue = MagicMock()
        config = {
            "test": "config",
            "input_csv": "dummy.csv",
        }  # Ensure input_csv is in config
        job_id = "job_test"

        mock_os.path.join.side_effect = lambda *args: "/".join(args)
        mock_os.makedirs = MagicMock()
        mock_os.path.exists.return_value = False  # For the finally block cleanup

        with patch(
            "streamlit_app.app.time"
        ) as mock_time_module:  # Renamed to avoid conflict
            mock_time_module.strftime.return_value = "20250509_123456"
            # Mock FileHandler to avoid actual file creation, use _ if not asserting on the instance itself
            with (
                patch(
                    "streamlit_app.app.logging.FileHandler"
                ) as mock_file_handler_class,
                patch("streamlit_app.app.run_pipeline") as mock_run_pipeline,
            ):  # Mock the core pipeline function
                mock_run_pipeline.return_value = (
                    "output/dummy_output.csv"  # Mock its return
                )

                run_pipeline_in_process(
                    config,
                    mock_status_queue,
                    job_id,  # Removed mock_log_queue
                )

                mock_os.makedirs.assert_called_with(
                    f"{project_root}/logfiles", exist_ok=True
                )
                expected_log_path = (
                    f"{project_root}/logfiles/pipeline_20250509_123456.log"
                )
                mock_file_handler_class.assert_called_with(expected_log_path)

    def test_run_pipeline_in_process_sendsLogFilePathViaStatusQueue(
        self, mock_logging_module, mock_os_module, mock_st_module
    ):
        """
        Verify that run_pipeline_in_process sends the pipeline_log_file_path via the status_queue.
        """
        from streamlit_app.app import project_root, run_pipeline_in_process

        mock_status_queue = MagicMock()
        test_job_id = "job_test_logpath"
        config = {"input_csv": "dummy.csv"}

        mock_os_module.path.join.side_effect = lambda *args: "/".join(args)
        mock_os_module.makedirs = MagicMock()
        mock_os_module.path.exists.return_value = False

        with (
            patch("streamlit_app.app.time") as mock_time,
            patch("streamlit_app.app.logging.FileHandler"),
            patch("streamlit_app.app.run_pipeline") as mock_run_pipeline,
        ):
            mock_time.strftime.return_value = "20250101_120000"
            mock_run_pipeline.return_value = "output/dummy_output.csv"

            run_pipeline_in_process(config, mock_status_queue, test_job_id)

            expected_log_path = f"{project_root}/logfiles/pipeline_20250101_120000.log"

            self.assertTrue(mock_status_queue.put.called)
            initial_status_call = mock_status_queue.put.call_args_list[0][0][0]

            self.assertIn("pipeline_log_file_path", initial_status_call)
            self.assertEqual(
                initial_status_call["pipeline_log_file_path"], expected_log_path
            )
            self.assertEqual(initial_status_call["status"], "Running")
            self.assertEqual(initial_status_call["job_id"], test_job_id)


@patch("streamlit_app.app.st")
@patch("streamlit_app.app.pd")
class TestProcessQueueMessages(unittest.TestCase):
    def test_process_queue_messages_storesPipelineLogFilePath(self, mock_pd, mock_st):
        """
        Test that process_queue_messages correctly stores pipeline_log_file_path
        from a status update into job_data.
        """
        from streamlit_app.app import process_queue_messages

        mock_st.session_state = {
            "active_jobs": {
                "job_log_test": {
                    "status": "Running",
                    "pipeline_log_file_path": None,
                    "status_queue": MagicMock(),
                    "process": MagicMock(is_alive=lambda: True),
                }
            }
        }
        expected_log_path = "/path/to/pipeline_job_log_test.log"
        status_update_with_log_path = {
            "pipeline_log_file_path": expected_log_path,
            "status": "Running",
            "phase": "Logging initialized",
        }

        mock_queue = mock_st.session_state["active_jobs"]["job_log_test"][
            "status_queue"
        ]
        mock_queue.empty.side_effect = [False, True]
        mock_queue.get_nowait.return_value = status_update_with_log_path

        process_queue_messages()

        updated_job_data = mock_st.session_state["active_jobs"]["job_log_test"]
        self.assertEqual(updated_job_data["pipeline_log_file_path"], expected_log_path)


if __name__ == "__main__":
    unittest.main()
