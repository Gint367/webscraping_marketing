# Correct import order: stdlib -> third-party -> local
import logging
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the project root to the Python path to allow imports like 'from streamlit_app import app'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Import the components to be tested using the package structure
try:
    # Now import using the package path relative to the project root
    from streamlit_app.app import (
        StreamlitLogHandler,
        display_config_section,
        display_input_section,
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
            pass # Dummy implementation
    def display_input_section():
        pass
    def display_config_section():
        pass
    # Mock streamlit itself if the import fails at the top level
    st_mock = MagicMock()
    sys.modules['streamlit'] = st_mock
    # Mock the app module itself to avoid errors if streamlit_app.app cannot be resolved
    # due to the initial ImportError
    if 'streamlit_app.app' not in sys.modules:
        sys.modules['streamlit_app.app'] = MagicMock()


# --- Test Cases ---
# Use @patch('streamlit_app.app.st') to target streamlit within the app module

@patch('streamlit_app.app.logging.info') # Patch logging.info to prevent log side effects during init
@patch('streamlit_app.app.st') # Patch st where it's used in the app module
class TestStreamlitAppSetup(unittest.TestCase):
    """Tests for the setup and initialization logic of the Streamlit app."""

    def test_init_session_state_initializes_defaults(self, mock_st, mock_logging_info):
        """
        Test that init_session_state sets default values in an empty session_state.
        """
        mock_st.session_state = {} # Start with an empty session state
        init_session_state()
        expected_defaults = {
            "page": "Input",
            "company_list": None,
            "config": {},
            "job_status": "Idle",
            "results": None,
            "log_messages": [] # Expect empty logs *during* init test
        }
        self.assertEqual(mock_st.session_state, expected_defaults)
        # Optionally, assert that logging.info was called if needed, but not required for this fix
        # mock_logging_info.assert_called_once_with("Session state initialized.")

    def test_init_session_state_does_not_overwrite_existing(self, mock_st, mock_logging_info):
        """
        Test that init_session_state does not overwrite existing values.
        """
        mock_st.session_state = {
            "page": "Output",
            "job_status": "Running",
            "custom_key": "custom_value",
            "log_messages": ["Existing log"]
        }
        init_session_state()
        # Check that existing keys were not overwritten
        self.assertEqual(mock_st.session_state["page"], "Output")
        self.assertEqual(mock_st.session_state["job_status"], "Running")
        self.assertEqual(mock_st.session_state["custom_key"], "custom_value")
        # Check that missing default keys were added
        self.assertIn("company_list", mock_st.session_state)
        self.assertIsNone(mock_st.session_state["company_list"])
        self.assertIn("config", mock_st.session_state)
        self.assertEqual(mock_st.session_state["config"], {})
        self.assertIn("results", mock_st.session_state)
        self.assertIsNone(mock_st.session_state["results"])
        # Check that log_messages was NOT modified by init_session_state logging
        self.assertIn("log_messages", mock_st.session_state)
        self.assertEqual(mock_st.session_state["log_messages"], ["Existing log"])
        # Optionally, assert that logging.info was called if needed
        # mock_logging_info.assert_called_once_with("Session state initialized.")


@patch('streamlit_app.app.st') # Patch st where it's used in the app module
class TestStreamlitLogHandler(unittest.TestCase):
    """Tests for the custom StreamlitLogHandler."""

    def test_emit_appends_formatted_message_to_session_state(self, mock_st):
        """
        Test that the handler formats a log record and appends it to session_state.log_messages.
        """
        mock_st.session_state = {"log_messages": []} # Initialize log_messages
        handler = StreamlitLogHandler()
        # Use a standard formatter for testing
        handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))

        record = logging.LogRecord(
            name='testlogger', level=logging.INFO, pathname='testpath', lineno=1,
            msg='Test log message', args=(), exc_info=None, func='test_func'
        )
        handler.emit(record)

        self.assertEqual(len(mock_st.session_state["log_messages"]), 1)
        self.assertEqual(mock_st.session_state["log_messages"][0], "INFO:Test log message")

    def test_emit_handles_exceptions_gracefully(self, mock_st):
        """
        Test that the handler calls handleError if formatting or appending fails.
        """
        # Simulate failure by making session_state not have the 'log_messages' key
        mock_st.session_state = {}

        handler = StreamlitLogHandler()
        # Mock handleError to check if it's called
        handler.handleError = MagicMock()

        record = logging.LogRecord(
            name='testlogger', level=logging.INFO, pathname='testpath', lineno=1,
            msg='Test log message', args=(), exc_info=None, func='test_func'
        )
        handler.emit(record)

        handler.handleError.assert_called_once_with(record)


@patch('streamlit_app.app.st') # Patch st where it's used in the app module
class TestUISections(unittest.TestCase):
    """Tests for the UI section display functions, focusing on session state updates."""

    def test_display_input_section_updates_state_on_upload(self, mock_st):
        """
        Test that display_input_section updates session_state when a file is uploaded.
        """
        mock_st.session_state = {"company_list": None}
        mock_file = MagicMock()
        mock_file.name = "test.csv"
        mock_st.file_uploader.return_value = mock_file
        mock_st.text_area.return_value = "" # No manual URLs

        display_input_section()

        mock_st.file_uploader.assert_called_once_with("Upload Company List", type=["csv", "xlsx"])
        mock_st.success.assert_called_once_with("File 'test.csv' uploaded.")
        self.assertEqual(mock_st.session_state["company_list"], "File: test.csv") # Checks placeholder logic

    def test_display_input_section_updates_state_on_manual_urls(self, mock_st):
        """
        Test that display_input_section updates session_state when manual URLs are entered.
        """
        mock_st.session_state = {"company_list": None}
        mock_st.file_uploader.return_value = None # No file uploaded
        mock_st.text_area.return_value = "http://example.com\nhttp://anotherexample.com"

        display_input_section()

        mock_st.text_area.assert_called_once_with("Or Enter URLs (one per line)")
        mock_st.success.assert_called_once_with("2 URLs entered.")
        self.assertEqual(mock_st.session_state["company_list"], "URLs: 2") # Checks placeholder logic

    def test_display_config_section_updates_state(self, mock_st):
        """
        Test that display_config_section updates session_state.config with widget values.
        """
        mock_st.session_state = {"config": {}}
        # Simulate return values from streamlit widgets
        mock_st.slider.return_value = 3
        mock_st.selectbox.return_value = "OpenAI"
        mock_st.text_input.return_value = "test_api_key"

        display_config_section()

        mock_st.slider.assert_called_once_with("Crawling Depth", 1, 5, 2)
        mock_st.selectbox.assert_called_once_with("LLM Provider", ["OpenAI", "Anthropic", "Gemini", "Mock"])
        mock_st.text_input.assert_called_once_with("API Key", type="password")

        expected_config = {
            'depth': 3,
            'llm_provider': "OpenAI",
            'api_key': "test_api_key"
        }
        self.assertEqual(mock_st.session_state["config"], expected_config)


if __name__ == '__main__':
    unittest.main()

# Added newline at the end of the file
