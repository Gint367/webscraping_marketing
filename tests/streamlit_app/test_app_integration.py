import multiprocessing
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from queue import Empty
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

# Add project root to Python path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from streamlit_app.models.job_data_model import JobDataModel  # noqa: E402

# The Streamlit app file to test
APP_FILE = os.path.join(project_root, "streamlit_app", "app.py")

# Attempt to import DEFAULT_OUTPUT_DIR_BASE for more robust assertions
# This might fail if app.py has side effects on direct import,
# in which case, path components will be hardcoded in assertions.
try:
    # To import constants from app.py without running the full Streamlit app,
    # we might need to be careful or mock st.
    # For now, we'll rely on the known structure or hardcode parts of the path.
    # from streamlit_app.app import DEFAULT_OUTPUT_DIR_BASE
    # Instead, we know DEFAULT_OUTPUT_DIR_BASE is typically 'outputs/streamlit_jobs' relative to project root
    EXPECTED_OUTPUT_BASE_DIR = Path(project_root) / "outputs" / "streamlit_jobs"
except ImportError:
    EXPECTED_OUTPUT_BASE_DIR = None


class TestStreamlitAppIntegration(unittest.TestCase):
    """
    Integration tests for the Streamlit application, focusing on the pipeline invocation.
    """

    def create_dummy_csv(self, temp_dir_path: str, filename: str = "dummy_input.csv") -> str:
        """
        Creates a dummy CSV file in the specified temporary directory.

        Args:
            temp_dir_path: Path to the temporary directory.
            filename: Name of the dummy CSV file.

        Returns:
            Absolute path to the created dummy CSV file.
        """
        data = {
            'company name': ['TestCo 1', 'TestCo 2'],
            'location': ['TestCity A', 'TestCity B'],
            'url': ['http://testco1.com', 'http://testco2.com']
        }
        df = pd.DataFrame(data)
        file_path = os.path.join(temp_dir_path, filename)
        df.to_csv(file_path, index=False)
        return file_path

    @patch('streamlit_app.app.multiprocessing.Process')  # Mock the Process class
    @patch('streamlit_app.app.db_utils.init_db')
    @patch('streamlit_app.app.db_utils.add_or_update_job_in_db') # Corrected function name
    @patch('streamlit_app.app.db_utils.load_jobs_from_db')
    def test_runPipeline_fileUploadAndStartProcessing_pipelineCalledWithCorrectConfig(
        self,
        mock_load_jobs_from_db: MagicMock,
        mock_add_or_update_job_in_db: MagicMock, # Corrected mock name
        mock_init_db: MagicMock,
        mock_process: MagicMock
    ):
        """
        Tests the scenario where a user uploads a file, provides a category,
        clicks 'Start Processing', and verifies that `run_pipeline` is called
        with the appropriately structured configuration.
        """
        # Setup mocks
        # Create a mock for Process instance
        mock_process_instance = MagicMock()
        mock_process_instance.pid = 12345  # Mock PID
        mock_process.return_value = mock_process_instance
        
        # Simulate an empty database. load_jobs_from_db should return a Dict[str, JobDataModel]
        mock_load_jobs_from_db.return_value = {} # Changed from DataFrame to empty dict
        mock_add_or_update_job_in_db.return_value = None # Simulate successful DB add/update
        mock_init_db.return_value = None # Simulate successful DB init

        at = AppTest.from_file(APP_FILE, default_timeout=30) # Increased timeout for slower machines
        at.run()
        self.assertFalse(at.exception, f"Streamlit App raised an exception on initial run: {at.exception}")

        with tempfile.TemporaryDirectory() as temp_dir:
            dummy_csv_path = self.create_dummy_csv(temp_dir)
            test_category = "test integration"

            # --- Simulate User Interactions ---

            # 1. Simulate file upload by directly setting session_state
            # Read the dummy CSV content
            with open(dummy_csv_path, 'rb') as f:
                dummy_csv_bytes = f.read()

            # Create a mock UploadedFile object
            mock_uploaded_file = MagicMock()
            mock_uploaded_file.name = os.path.basename(dummy_csv_path)
            mock_uploaded_file.getvalue.return_value = dummy_csv_bytes

            # Set the session state as if a file was uploaded
            at.session_state["uploaded_file_data"] = mock_uploaded_file
            at.session_state["input_method"] = "File Upload" # Ensure this is set
            at.run() # Allow the app to process the session state change if needed

            # 2. Simulate category input by directly setting session state
            # The UI element for category input is currently not on the Input page.
            # This sets the category in the config, which process_data() expects.
            if "config" not in at.session_state:
                at.session_state["config"] = {}
            at.session_state["config"]["category"] = test_category
            # We might need an at.run() here if the app reacts to config changes,
            # but for now, process_data will pick it up when "Start Processing" is clicked.
            at.run()

            # 3. Simulate clicking "Start Processing" button
            try:
                start_button_key = "start_processing_button"  # Key added to the button in input_section.py
                at.button(key=start_button_key).click()
                at.run()
            except Exception as e:
                self.fail(
                    f"Failed to click 'Start Processing' button (key: '{start_button_key}'). "
                    f"Ensure a button with this key exists on the Input page. Error: {e}"
                )

            # Allow a moment for the app to process the click
            time.sleep(1)

            # --- Assertions ---
            
            # 1. Verify Process was created and started with the correct arguments
            mock_process.assert_called()
            
            # Get the arguments passed to Process constructor
            process_args = mock_process.call_args
            
            # Verify function and arguments passed to Process
            target_func = process_args[1].get('target')
            args_list = process_args[1].get('args')
            
            self.assertIsNotNone(target_func, "No target function passed to Process constructor")
            self.assertEqual(target_func.__name__, "run_pipeline_in_process", 
                         "Wrong target function passed to Process constructor")
            
            # Check the arguments passed to run_pipeline_in_process
            self.assertTrue(len(args_list) >= 1, "No arguments passed to run_pipeline_in_process")
            config_arg = args_list[0]  # First argument should be the config
            
            # Check job_id format in the config
            job_id_from_config = config_arg.get('job_id')
            self.assertIsNotNone(job_id_from_config, "job_id not found in config passed to Process")
            self.assertTrue(job_id_from_config.startswith("job_"),
                        f"job_id '{job_id_from_config}' does not start with 'job_'")
            
            # Check input_csv in the config
            self.assertIn('input_csv', config_arg, "input_csv not in config passed to Process")
            input_csv_path = config_arg.get('input_csv')
            self.assertIsNotNone(input_csv_path, "input_csv in config is None")
            # The app creates a temporary file, so we can't check the exact filename
            # but we can verify it's a temporary file with the correct extension
            input_csv_filename = os.path.basename(input_csv_path) if input_csv_path else None
            self.assertTrue(input_csv_filename.endswith('.csv'),  # type: ignore
                         f"Input CSV filename doesn't end with .csv: {input_csv_filename}")
                         
            # Check category in config
            self.assertEqual(config_arg.get('category'), test_category,
                         f"Wrong category in config. Expected '{test_category}', got '{config_arg.get('category')}'")
            
            # Check if start method was called
            mock_process_instance.start.assert_called_once()
            
            # 2. Verify that add_or_update_job_in_db was called for this job_id in "queued" state
            #    and that the JobDataModel had the correct details.
            self.assertTrue(mock_add_or_update_job_in_db.called, "add_or_update_job_in_db was not called.")
            
            # Process calls to find job state - can be Running since we're checking after process start
            job_model_arg = None
            job_found_for_id = False
            
            for call_args_instance in mock_add_or_update_job_in_db.call_args_list:
                if len(call_args_instance.args) > 1:
                    job_candidate = call_args_instance.args[1]
                    if isinstance(job_candidate, JobDataModel) and job_candidate.id == job_id_from_config:
                        # Found a call for our job_id
                        job_model_arg = job_candidate
                        job_found_for_id = True
                        
                        # Assertions for the content of the JobDataModel
                        self.assertIsNotNone(job_model_arg.config, 
                                             f"Job {job_id_from_config} has no config.")
                        self.assertEqual(job_model_arg.config.get('category'), test_category,
                                         f"Job {job_id_from_config} has incorrect category. "
                                         f"Expected '{test_category}', got '{job_model_arg.config.get('category')}'.")
                        
                        self.assertIsNotNone(job_model_arg.file_info, 
                                             f"Job {job_id_from_config} has no file_info.")
                        self.assertEqual(job_model_arg.file_info.get('name'), mock_uploaded_file.name,
                                         f"Job {job_id_from_config} has incorrect file name. "
                                         f"Expected '{mock_uploaded_file.name}', got '{job_model_arg.file_info.get('name')}'.")
                        break # Found a job state for our job_id

            self.assertTrue(job_found_for_id,
                            f"No job state for job_id '{job_id_from_config}' found in calls to add_or_update_job_in_db. "
                            f"Review app logic for saving job state. Calls: {mock_add_or_update_job_in_db.call_args_list}")

        self.assertFalse(at.exception, f"Streamlit App raised an exception during test execution: {at.exception}")

if __name__ == '__main__':
    unittest.main()
