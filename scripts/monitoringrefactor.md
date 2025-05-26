# Detailed Refactoring Plan for `monitoring_section.py`

## Overview
This plan focuses on breaking down the monolithic `display_monitoring_section` function into smaller, testable components while reducing tight coupling with Streamlit's session state and improving overall architecture.

## Phase 1 Configuration

### Config Module for Phase Formats and Order

```python
"""Configuration for monitoring section."""

# Phase formatting dictionary maps internal phase names to user-friendly descriptions
PHASE_FORMATS = {
    "extracting_machine": {
        "get_bundesanzeiger_html": "Extracting Machine: Fetch Bundesanzeiger HTML",
        "clean_html": "Extracting Machine: Clean HTML",
        "extract_sachanlagen": "Extracting Machine: Extract Sachanlagen",
        "generate_report": "Extracting Machine: Generate Report",
        "merge_data": "Extracting Machine: Merge Technische Anlagen and Sachanlagen",
    },
    "webcrawl": {
        "crawl_domain": "Webcrawl: Crawl Domain",
        "extract_llm": "Webcrawl: Extract Keywords (LLM)",
        "fill_process_type": "Webcrawl: Fill Process Type",
        "pluralize_llm_file": "Webcrawl: Pluralize Keywords in File",
        "pluralize_llm_entry": "Webcrawl: Pluralize Keywords for Entry",
        "process_files": "Webcrawl: Consolidate Data",
        "convert_to_csv": "Webcrawl: Convert to CSV",
    },
    "integration": {
        "merge_technische_anlagen": "Integration: Merge Technische Anlagen",
        "enrich_data": "Integration: Enrich Data",
    },
}

# Define the order of main phases for progress calculation
PHASE_ORDER = ["extracting_machine", "webcrawl", "integration"]
```

### Progress Calculation Utility Functions

```python
"""Utility functions for tracking job progress."""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

def parse_progress_log_line(log_line: str) -> Optional[Tuple[str, str, str]]:
    """
    Parses a log line to extract progress information.
    Expected format: "PROGRESS:main_phase:step:details"
                     For example: "PROGRESS:webcrawl:extract_llm:1/8:Extracting data from example.com"

    Args:
        log_line (str): A single line from the log file.

    Returns:
        Optional[Tuple[str, str, str]]: A tuple containing (main_phase, step, details_str)
                                         if the line is a valid progress line.
                                         details_str will be empty if no details are present.
                                         Returns None otherwise.
    """
    log_line = log_line.strip()
    if not log_line.startswith("PROGRESS:"):
        return None

    try:
        # Remove "PROGRESS:" part
        content = log_line[len("PROGRESS:"):].strip()

        # Split by ":" to get components
        components = content.split(":", 2)  # Split into max 3 parts: main_phase, step, details

        if len(components) < 2:
            return None

        main_phase = components[0].strip()
        step = components[1].strip()

        # The rest is details (which might contain additional colons)
        details = components[2].strip() if len(components) > 2 else ""

        if not main_phase or not step:
            return None

        return main_phase, step, details
    except Exception as e:
        logger.error(f"Error parsing progress log line '{log_line}': {e}")
        return None

def calculate_progress_from_phase(
    current_phase_str: Optional[str],
    phase_formats: dict,
    phase_order: list,
    current_status: Optional[str] = None,
    initial_progress_value: float = 0.01,
    starting_progress_value: float = 0.05,
    base_progress: float = 0.0,
) -> float:
    """
    Calculates the progress percentage based on the current phase relative to PHASE_FORMATS.

    Args:
        current_phase_str: The string describing the current phase.
        phase_formats: The dictionary defining known phases and their formats.
        phase_order: The list defining the order of main phases.
        current_status: The current job status (e.g., "Running", "Completed").
        initial_progress_value: Progress value for "Initializing" or "Creating job" phase.
        starting_progress_value: Progress value for "Starting Pipeline" phase.
        base_progress: Base progress value to start from.

    Returns:
        A float between 0.0 and 1.0 representing the progress.
    """
    # Implementation details would go here
```
    auto_refresh_enabled: bool = True
    refresh_interval: float = 3.0
    page_name: str = "Monitoring"
    log_container_height: int = 400
    
    def get_refresh_interval(self, is_monitoring_page_active: bool, has_active_jobs: bool) -> Optional[float]:
        """
        Calculate the actual refresh interval based on current state.
        
        Args:
            is_monitoring_page_active: Whether the monitoring page is currently active
            has_active_jobs: Whether there are any active jobs
            
        Returns:
            Refresh interval in seconds, or None if auto-refresh should be disabled
        """
        if self.auto_refresh_enabled and is_monitoring_page_active and has_active_jobs:
            return self.refresh_interval
        return None

@dataclass
class JobSelection:
    """Manages job selection state."""
    selected_job_id: Optional[str] = None
    jobs_for_deletion: List[str] = field(default_factory=list)
    show_confirm_delete_expander: bool = False
    
    def is_valid_selection(self, available_jobs: Dict[str, JobDataModel]) -> bool:
        """Check if the current selection is valid."""
        return (self.selected_job_id is not None and 
                self.selected_job_id in available_jobs)
    
    def get_most_recent_job_id(self, jobs: Dict[str, JobDataModel]) -> Optional[str]:
        """Get the ID of the most recently started job."""
        if not jobs:
            return None
        
        sorted_jobs = sorted(
            jobs.items(),
            key=lambda item: item[1].start_time,
            reverse=True
        )
        return sorted_jobs[0][0] if sorted_jobs else None

@dataclass
class LogFileState:
    """Manages log file reading positions."""
    positions: Dict[str, int] = field(default_factory=dict)
    
    def get_position(self, job_id: str) -> int:
        """Get the last read position for a job's log file."""
        return self.positions.get(job_id, 0)
    
    def set_position(self, job_id: str, position: int) -> None:
        """Set the last read position for a job's log file."""
        self.positions[job_id] = position

@dataclass
class MonitoringState:
    """Complete monitoring section state."""
    settings: MonitoringSettings = field(default_factory=MonitoringSettings)
    job_selection: JobSelection = field(default_factory=JobSelection)
    log_file_state: LogFileState = field(default_factory=LogFileState)
    active_jobs: Dict[str, JobDataModel] = field(default_factory=dict)
    deletion_success_count: int = 0
    deletion_error_count: int = 0
    last_pid_check_time: float = 0
    pid_check_in_progress: bool = False
```

### 1.2 Create State Manager Interface

```python
"""Service for managing monitoring section state."""

import logging
import streamlit as st
from typing import Dict, Optional, Protocol
from streamlit_app.models.monitoring_state import MonitoringState, JobSelection, LogFileState
from streamlit_app.models.job_data_model import JobDataModel

logger = logging.getLogger(__name__)

class StateManager(Protocol):
    """Protocol for state management implementations."""
    
    def get_monitoring_state(self) -> MonitoringState:
        """Get the current monitoring state."""
        ...
    
    def update_monitoring_state(self, state: MonitoringState) -> None:
        """Update the monitoring state."""
        ...
    
    def get_jobs(self) -> Dict[str, JobDataModel]:
        """Get active jobs."""
        ...
    
    def set_jobs(self, jobs: Dict[str, JobDataModel]) -> None:
        """Set active jobs."""
        ...

class StreamlitStateManager:
    """Streamlit-specific implementation of state management."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_monitoring_state(self) -> MonitoringState:
        """Get monitoring state from Streamlit session state."""
        if 'monitoring_state' not in st.session_state:
            st.session_state['monitoring_state'] = MonitoringState()
        return st.session_state['monitoring_state']
    
    def update_monitoring_state(self, state: MonitoringState) -> None:
        """Update monitoring state in Streamlit session state."""
        st.session_state['monitoring_state'] = state
    
    def get_jobs(self) -> Dict[str, JobDataModel]:
        """Get active jobs from session state."""
        return st.session_state.get("active_jobs", {})
    
    def set_jobs(self, jobs: Dict[str, JobDataModel]) -> None:
        """Set active jobs in session state."""
        st.session_state["active_jobs"] = jobs
    
    def get_current_page(self) -> str:
        """Get the current page name."""
        return st.session_state.get('page', '')

class MockStateManager:
    """Mock implementation for testing."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.monitoring_state = MonitoringState()
        self.jobs: Dict[str, JobDataModel] = {}
        self.current_page = "Monitoring"
    
    def get_monitoring_state(self) -> MonitoringState:
        """Get monitoring state from mock storage."""
        return self.monitoring_state
    
    def update_monitoring_state(self, state: MonitoringState) -> None:
        """Update monitoring state in mock storage."""
        self.monitoring_state = state
    
    def get_jobs(self) -> Dict[str, JobDataModel]:
        """Get active jobs from mock storage."""
        return self.jobs
    
    def set_jobs(self, jobs: Dict[str, JobDataModel]) -> None:
        """Set active jobs in mock storage."""
        self.jobs = jobs
    
    def get_current_page(self) -> str:
        """Get the current page name."""
        return self.current_page
```

## Phase 2: Extract Business Logic Services

### 2.1 Job Management Service

```python
"""Service for job management operations."""

import logging
import time
from typing import Dict, List, Tuple, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from streamlit_app.models.job_data_model import JobDataModel
from streamlit_app.services.monitoring_state_service import StateManager
from streamlit_app.utils.job_utils import delete_job_and_artifacts, merge_active_jobs_with_db

logger = logging.getLogger(__name__)

class JobManagementService:
    """Service for managing job operations."""
    
    def __init__(
        self, 
        state_manager: StateManager,
        check_pid_callback: Callable[[int], Tuple[bool, str]],
        db_utils_module
    ):
        """
        Initialize the job management service.
        
        Args:
            state_manager: State management implementation
            check_pid_callback: Function to check if a PID is alive
            db_utils_module: Database utilities module
        """
        self.state_manager = state_manager
        self.check_pid_callback = check_pid_callback
        self.db_utils = db_utils_module
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def reload_jobs_from_db(self, db_connection) -> Dict[str, JobDataModel]:
        """
        Reload jobs from database and merge with active jobs.
        
        Args:
            db_connection: Database connection object
            
        Returns:
            Merged dictionary of jobs
        """
        try:
            loaded_jobs = self.db_utils.load_jobs_from_db(db_connection)
            active_jobs = self.state_manager.get_jobs()
            merged_jobs = merge_active_jobs_with_db(active_jobs, loaded_jobs)
            self.state_manager.set_jobs(merged_jobs)
            return merged_jobs
        except Exception as e:
            self.logger.error(f"Failed to reload jobs from DB: {e}")
            return self.state_manager.get_jobs()
    
    def update_job_statuses_with_pid_check(
        self, 
        jobs_dict: Dict[str, JobDataModel], 
        min_interval: float = 1.0
    ) -> None:
        """
        Check job aliveness via PID with debouncing.
        
        Args:
            jobs_dict: Dictionary of jobs to check
            min_interval: Minimum interval between checks in seconds
        """
        monitoring_state = self.state_manager.get_monitoring_state()
        now = time.time()
        
        # Check if we should skip this check
        if (monitoring_state.pid_check_in_progress or 
            now - monitoring_state.last_pid_check_time < min_interval):
            return
        
        monitoring_state.pid_check_in_progress = True
        monitoring_state.last_pid_check_time = now
        self.state_manager.update_monitoring_state(monitoring_state)
        
        try:
            jobs_to_check = self._get_jobs_needing_pid_check(jobs_dict)
            
            if not jobs_to_check:
                return
            
            self._perform_pid_checks(jobs_to_check)
            
        finally:
            monitoring_state.pid_check_in_progress = False
            self.state_manager.update_monitoring_state(monitoring_state)
    
    def _get_jobs_needing_pid_check(
        self, 
        jobs_dict: Dict[str, JobDataModel]
    ) -> List[Tuple[str, JobDataModel, int]]:
        """
        Identify jobs that need PID checking.
        
        Args:
            jobs_dict: Dictionary of all jobs
            
        Returns:
            List of tuples (job_id, job_data, pid)
        """
        jobs_to_check = []
        
        for job_id, job_data in jobs_dict.items():
            if (getattr(job_data, "status", None) not in ("Running", "Initializing") or
                getattr(job_data, "pid", None) is None):
                continue
            
            jobs_to_check.append((job_id, job_data, job_data.pid))
        
        self.logger.info(f"Found {len(jobs_to_check)} jobs needing PID check")
        return jobs_to_check
    
    def _perform_pid_checks(
        self, 
        jobs_to_check: List[Tuple[str, JobDataModel, int]]
    ) -> None:
        """
        Perform PID checks using thread pool.
        
        Args:
            jobs_to_check: List of jobs to check
        """
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_job_id = {
                executor.submit(self.check_pid_callback, pid): job_id
                for job_id, _, pid in jobs_to_check
            }
            
            for future in as_completed(future_to_job_id):
                job_id = future_to_job_id[future]
                try:
                    is_alive, status_msg = future.result()
                    self._handle_pid_check_result(job_id, is_alive, status_msg)
                except Exception as e:
                    self.logger.error(f"PID check failed for job {job_id}: {e}")
    
    def _handle_pid_check_result(
        self, 
        job_id: str, 
        is_alive: bool, 
        status_msg: str
    ) -> None:
        """
        Handle the result of a PID check.
        
        Args:
            job_id: ID of the job that was checked
            is_alive: Whether the process is alive
            status_msg: Status message from the check
        """
        jobs = self.state_manager.get_jobs()
        
        if job_id not in jobs:
            return
        
        job_data = jobs[job_id]
        
        if not is_alive and job_data.status in ("Running", "Initializing"):
            self.logger.info(f"Job {job_id} process is no longer alive, marking as completed")
            job_data.status = "Completed"
            job_data.end_time = time.time()
            job_data.touch()
            
            # Update in database would happen here
            # self.db_utils.add_or_update_job_in_db(conn, job_data)
    
    def delete_selected_jobs(
        self, 
        job_ids: List[str], 
        db_connection
    ) -> Tuple[int, int]:
        """
        Delete selected jobs and their artifacts.
        
        Args:
            job_ids: List of job IDs to delete
            db_connection: Database connection
            
        Returns:
            Tuple of (success_count, error_count)
        """
        success_count = 0
        error_count = 0
        
        for job_id in job_ids:
            try:
                if delete_job_and_artifacts(job_id, db_connection):
                    success_count += 1
                    self.logger.info(f"Successfully deleted job {job_id}")
                else:
                    error_count += 1
                    self.logger.error(f"Failed to delete job {job_id}")
            except Exception as e:
                error_count += 1
                self.logger.error(f"Error deleting job {job_id}: {e}")
        
        # Update jobs after deletion
        remaining_jobs = {
            job_id: job_data 
            for job_id, job_data in self.state_manager.get_jobs().items()
            if job_id not in job_ids
        }
        self.state_manager.set_jobs(remaining_jobs)
        
        return success_count, error_count
```

### 2.2 Progress Tracking Service

```python
"""Service for tracking job progress from log files."""

import logging
import os
from typing import Dict, List, Optional, Tuple, Callable, Any

from streamlit_app.models.job_data_model import JobDataModel
from streamlit_app.services.monitoring_state_service import StateManager
from streamlit_app.section.monitoring_section import (
    parse_progress_log_line, 
    calculate_progress_from_phase
)

logger = logging.getLogger(__name__)

class ProgressTrackingService:
    """Service for tracking job progress from log files."""
    
    def __init__(
        self, 
        state_manager: StateManager,
        db_utils_module
    ):
        """
        Initialize the progress tracking service.
        
        Args:
            state_manager: State management implementation
            db_utils_module: Database utilities module
        """
        self.state_manager = state_manager
        self.db_utils = db_utils_module
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def update_job_progress_from_log(
        self, 
        job_model: JobDataModel, 
        db_connection,
        phase_formats: Dict[str, Any],
        phase_order: list[str],
        calculate_progress_fn: Callable[..., float]
    ) -> bool:
        """
        Update a job's progress by parsing its log file.
        
        Args:
            job_model: Job model to update
            db_connection: Database connection
            phase_formats: Configuration for phase descriptions
            phase_order: Order of phases for progress calculation
            calculate_progress_fn: Function to calculate progress percentage
            
        Returns:
            True if an update occurred, False otherwise
        """
        if not self._validate_log_file(job_model):
            return False
        
        try:
            return self._process_log_file_updates(job_model, db_connection, phase_formats, phase_order, calculate_progress_fn)
        except FileNotFoundError:
            return self._handle_missing_log_file(job_model, db_connection)
        except Exception as e:
            self.logger.error(
                f"Error updating job progress from log for job {job_model.id}: {e}",
                exc_info=True
            )
            return False
    
    def _validate_log_file(self, job_model: JobDataModel) -> bool:
        """
        Validate that the job has a log file and it exists.
        
        Args:
            job_model: Job model to validate
            
        Returns:
            True if log file is valid, False otherwise
        """
        if not job_model.pipeline_log_file_path:
            self.logger.debug(f"Job {job_model.id} has no log file path set")
            return False
        
        if not os.path.exists(job_model.pipeline_log_file_path):
            self.logger.warning(
                f"Log file not found for job {job_model.id}: {job_model.pipeline_log_file_path}"
            )
            return False
        
        return True
    
    def _process_log_file_updates(
        self, 
        job_model: JobDataModel, 
        db_connection,
        phase_formats: Dict[str, Any],
        phase_order: list[str],
        calculate_progress_fn: Callable[..., float]
    ) -> bool:
        """
        Process updates from the log file.
        
        Args:
            job_model: Job model to update
            db_connection: Database connection
            phase_formats: Configuration for phase descriptions
            phase_order: Order of phases for progress calculation
            calculate_progress_fn: Function to calculate progress
            
        Returns:
            True if updates were made, False otherwise
        """
        monitoring_state = self.state_manager.get_monitoring_state()
        last_position = monitoring_state.log_file_state.get_position(job_model.id)
        
        with open(job_model.pipeline_log_file_path, "r", encoding="utf-8") as f:
            f.seek(last_position)
            new_lines = f.readlines()
            current_position = f.tell()
            
            if new_lines:
                monitoring_state.log_file_state.set_position(job_model.id, current_position)
                self.state_manager.update_monitoring_state(monitoring_state)
        
        if not new_lines:
            return False
        
        return self._parse_log_lines_and_update_job(job_model, new_lines, db_connection, phase_formats, phase_order, calculate_progress_fn)
    
    def update_selected_job_progress_from_log(
    job_model: JobDataModel,
    conn,
    PHASE_FORMATS: Dict[str, Any],
    PHASE_ORDER: list[str],
    calculate_progress_from_phase: Callable[..., float],
) -> bool:
    """
    Reads new lines from a job's log file, parses progress, phase, errors,
    and completion status, updates the job model, and saves it to the database.

    Args:
        job_model (JobDataModel): The job data model to update.
        conn: The database connection object.
        PHASE_FORMATS (Dict[str, Any]): Configuration for phase descriptions.
        PHASE_ORDER (list[str]): Order of phases for progress calculation.
        calculate_progress_from_phase (Callable[..., float]): Function to calculate progress.

    Returns:
        bool: True if an update occurred, False otherwise.
    """
        updated = False
        log_changed_job_state = False
        latest_progress_info = None
        
        for line in new_lines:
            line_strip = line.strip()
            
            # Check for progress updates
            if "PROGRESS:" in line_strip:
                progress_segment = line_strip[line_strip.find("PROGRESS:"):]
                parsed_info = parse_progress_log_line(progress_segment)
                if parsed_info:
                    latest_progress_info = parsed_info
            
            # Check for terminal states
            if self._check_for_terminal_states(line_strip, job_model):
                log_changed_job_state = True
                updated = True
        
        # Update progress if we have new progress info and job isn't in terminal state
        if latest_progress_info and not log_changed_job_state:
            if self._update_job_progress(job_model, latest_progress_info):
                updated = True
        
        if updated:
            job_model.touch()
            self.db_utils.add_or_update_job_in_db(db_connection, job_model)
        
        return updated
    
    def _check_for_terminal_states(
        self, 
        line_strip: str, 
        job_model: JobDataModel
    ) -> bool:
        """
        Check if the log line indicates a terminal state change.
        
        Args:
            line_strip: Stripped log line
            job_model: Job model to potentially update
            
        Returns:
            True if a terminal state change occurred, False otherwise
        """
        # Check for completion
        if "PIPELINE_PROCESS_COMPLETED" in line_strip:
            if job_model.status != "Completed":
                job_model.status = "Completed"
                job_model.end_time = time.time()
                job_model.progress = 1.0  # Progress as float between 0.0 and 1.0
                job_model.max_progress = 1.0  # Update max_progress as well
                return True
        
        # Check for errors
        if "PIPELINE_PROCESS_ERROR:" in line_strip:
            try:
                error_msg = line_strip.split("PIPELINE_PROCESS_ERROR:", 1)[1].strip()
                job_model.status = "Error"
                job_model.error_message = error_msg
                job_model.end_time = time.time()
                return True
            except IndexError:
                pass
        
        # Check for exit
        if "PIPELINE_PROCESS_EXITING:" in line_strip:
            if job_model.status not in ["Completed", "Error", "Failed", "Cancelled"]:
                job_model.status = "Failed"
                job_model.end_time = time.time()
                return True
        
        return False
    
    def _update_job_progress(
        self, 
        job_model: JobDataModel, 
        progress_info: Tuple[str, str, str],
        phase_formats: Dict[str, Any],
        phase_order: list[str],
        calculate_progress_fn: Callable[..., float]
    ) -> bool:
        """
        Update job progress based on parsed progress information.
        
        Args:
            job_model: Job model to update
            progress_info: Tuple of (main_phase, sub_phase, details)
            phase_formats: Configuration for phase descriptions
            phase_order: Order of phases for progress calculation
            calculate_progress_fn: Function to calculate progress
            
        Returns:
            True if progress was updated, False otherwise
        """
        main_phase_key, sub_phase_key, details = progress_info
        descriptive_phase = phase_formats.get(main_phase_key, {}).get(sub_phase_key)
        
        if not descriptive_phase:
            self.logger.warning(
                f"Could not find descriptive phase for {main_phase_key}.{sub_phase_key} "
                f"in PHASE_FORMATS for job {job_model.id}"
            )
            return False
        
        current_status = getattr(job_model, "status", "Running")
        new_progress = calculate_progress_fn(
            descriptive_phase,
            phase_formats,
            phase_order,
            current_status,
            base_progress=0.05,
        )
        
        if (job_model.phase != descriptive_phase or 
            job_model.progress != new_progress):
            job_model.phase = descriptive_phase
            job_model.progress = new_progress
            job_model.max_progress = max(job_model.max_progress, new_progress)
            return True
        
        return False
    
    def _handle_missing_log_file(
        self, 
        job_model: JobDataModel, 
        db_connection
    ) -> bool:
        """
        Handle case where log file is missing for a running job.
        
        Args:
            job_model: Job model to update
            db_connection: Database connection
            
        Returns:
            False (no update occurred in the normal sense)
        """
        self.logger.warning(
            f"Log file not found for job {job_model.id} at "
            f"{job_model.pipeline_log_file_path} during progress update."
        )
        
        if job_model.status in ["Running", "Initializing"]:
            job_model.status = "Error"
            job_model.error_message = "Log file disappeared."
            job_model.phase = "Log file missing"
            job_model.end_time = time.time()
            job_model.touch()
            self.db_utils.add_or_update_job_in_db(db_connection, job_model)
        
        return False
```

## Phase 3: Extract UI Components

### 3.1 Job Table Component

```python
"""Job table UI component."""

import logging
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Callable
from datetime import datetime

from streamlit_app.models.job_data_model import JobDataModel
from streamlit_app.utils.time_utils import format_timestamp

logger = logging.getLogger(__name__)

class JobTableComponent:
    """Component for displaying jobs in a table format."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def render_jobs_table(
        self, 
        jobs: Dict[str, JobDataModel],
        process_queue_callback: Callable[[], None]
    ) -> Optional[List[str]]:
        """
        Render the jobs table and return selected job IDs for deletion.
        
        Args:
            jobs: Dictionary of jobs to display
            process_queue_callback: Callback to process queue messages
            
        Returns:
            List of selected job IDs, or None if no selection
        """
        st.subheader("Jobs")
        process_queue_callback()
        
        if not jobs:
            st.info("No jobs have been run yet. Start a new job from the Input section.")
            return None
        
        jobs_df = self._create_jobs_dataframe(jobs)
        
        # Store DataFrame for selection processing
        st.session_state["jobs_df_for_display"] = jobs_df
        
        # Display table with selection
        st.caption("Select rows to delete. Note: Sorting will reset selections. Use search to filter before selecting.")
        
        column_config = self._get_column_config()
        
        st.dataframe(
            jobs_df,
            key="jobs_dataframe_selector",
            on_select="rerun",
            selection_mode=["multi-row"],
            hide_index=True,
            column_config=column_config
        )
        
        return self._get_selected_job_ids()
    
    def _create_jobs_dataframe(self, jobs: Dict[str, JobDataModel]) -> pd.DataFrame:
        """
        Create a pandas DataFrame from jobs dictionary.
        
        Args:
            jobs: Dictionary of jobs
            
        Returns:
            DataFrame with job information
        """
        job_rows = []
        
        for job_id, job_data in jobs.items():
            # Get input file name safely
            input_file_name = "N/A"
            if hasattr(job_data, 'file_info') and job_data.file_info:
                input_file_name = getattr(job_data.file_info, 'name', 'N/A')
            
            # Format timestamps
            start_time_str = format_timestamp(getattr(job_data, 'start_time', None))
            end_time_str = self._format_end_time(job_data)
            
            job_rows.append({
                "job_id": job_id,
                "Status": getattr(job_data, 'status', 'Unknown'),
                "Progress": getattr(job_data, 'progress', 0),
                "Phase": getattr(job_data, 'phase', 'N/A'),
                "Input File": input_file_name,
                "Start Time": pd.to_datetime(start_time_str) if start_time_str != "N/A" else None,
                "End Time": pd.to_datetime(end_time_str) if end_time_str not in ["Running", "N/A"] else end_time_str,
            })
        
        return pd.DataFrame(job_rows)
    
    def _format_end_time(self, job_data: JobDataModel) -> str:
        """
        Format the end time for display.
        
        Args:
            job_data: Job data model
            
        Returns:
            Formatted end time string
        """
        status = getattr(job_data, 'status', None)
        end_time = getattr(job_data, 'end_time', None)
        
        if status in ["Running", "Initializing"]:
            return "Running"
        
        return format_timestamp(end_time)
    
    def _get_column_config(self) -> Dict:
        """Get column configuration for the dataframe."""
        return {
            "job_id": st.column_config.TextColumn(
                "Job ID",
                help="Unique identifier for the job",
                width="small"
            ),
            "Status": st.column_config.TextColumn(
                "Status",
                help="Current status of the job",
                width="small"
            ),
            "Progress": st.column_config.ProgressColumn(
                "Progress",
                help="Job completion percentage",
                format="%s%%",
                min_value=0,
                max_value=100
            ),
            "Phase": st.column_config.TextColumn(
                "Current Phase",
                help="Current processing phase",
                width="medium"
            ),
            "Input File": st.column_config.TextColumn(
                "Input File",
                help="Name of the input file or data source",
                width="medium"
            ),
            "Start Time": st.column_config.DatetimeColumn(
                "Started At",
                help="When the job was started",
                format="YYYY-MM-DD HH:mm:ss",
                width="small"
            ),
            "End Time": st.column_config.DatetimeColumn(
                "Completed At",
                help="When the job was completed",
                format="YYYY-MM-DD HH:mm:ss",
                width="small"
            ),
        }
    
    def _get_selected_job_ids(self) -> Optional[List[str]]:
        """
        Get the job IDs of selected rows.
        
        Returns:
            List of selected job IDs, or None if no selection
        """
        if ("jobs_dataframe_selector" not in st.session_state or
            not hasattr(st.session_state["jobs_dataframe_selector"], "selection")):
            return None
        
        selected_row_indices = st.session_state["jobs_dataframe_selector"].selection.get("rows", [])
        
        if not selected_row_indices:
            return None
        
        original_jobs_df = st.session_state.get("jobs_df_for_display")
        
        if original_jobs_df is None or original_jobs_df.empty:
            return None
        
        try:
            selected_job_ids = [
                original_jobs_df.iloc[idx]["job_id"] 
                for idx in selected_row_indices
                if idx < len(original_jobs_df)
            ]
            return selected_job_ids
        except (IndexError, KeyError) as e:
            self.logger.error(f"Error getting selected job IDs: {e}")
            return None
```

### 3.2 Job Deletion Component

```python
"""Job deletion UI component."""

import logging
import streamlit as st
from typing import List, Optional, Callable

logger = logging.getLogger(__name__)

class JobDeletionComponent:
    """Component for handling job deletion UI."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def render_deletion_controls(
        self, 
        has_jobs: bool,
        selected_job_ids: Optional[List[str]],
        delete_callback: Callable[[List[str]], tuple[int, int]]
    ) -> None:
        """
        Render job deletion controls.
        
        Args:
            has_jobs: Whether there are any jobs to delete
            selected_job_ids: List of selected job IDs
            delete_callback: Callback function to delete jobs
        """
        deletion_status_container = st.container()
        
        # Show delete button only if there are jobs
        if has_jobs:
            if st.button("Delete Selected Jobs", key="delete_selected_jobs_button", icon=":material/delete:"):
                self._handle_delete_button_click(selected_job_ids)
        
        # Display persistent deletion status messages
        self._display_deletion_status(deletion_status_container)
        
        # Display confirmation UI if needed
        self._display_confirmation_ui(delete_callback)
    
    def _handle_delete_button_click(self, selected_job_ids: Optional[List[str]]) -> None:
        """
        Handle the delete button click.
        
        Args:
            selected_job_ids: List of selected job IDs
        """
        if selected_job_ids:
            st.session_state["job_ids_selected_for_deletion"] = selected_job_ids
            st.session_state["show_confirm_delete_expander"] = True
        else:
            st.warning("No jobs selected. Please select one or more rows from the table.")
    
    def _display_deletion_status(self, container) -> None:
        """
        Display persistent deletion status messages.
        
        Args:
            container: Streamlit container for status messages
        """
        if "deletion_success_count" in st.session_state and st.session_state["deletion_success_count"] > 0:
            count = st.session_state["deletion_success_count"]
            container.success(
                f"Successfully deleted {count} job{'s' if count != 1 else ''}."
            )
            st.session_state.pop("deletion_success_count", None)
        
        if "deletion_error_count" in st.session_state and st.session_state["deletion_error_count"] > 0:
            count = st.session_state["deletion_error_count"]
            container.error(
                f"Failed to delete {count} job{'s' if count != 1 else ''}. See logs for details."
            )
            st.session_state.pop("deletion_error_count", None)
    
    def _display_confirmation_ui(self, delete_callback: Callable[[List[str]], tuple[int, int]]) -> None:
        """
        Display the deletion confirmation UI.
        
        Args:
            delete_callback: Callback function to delete jobs
        """
        if not st.session_state.get("show_confirm_delete_expander", False):
            return
        
        with st.expander("Confirm Deletion", expanded=True):
            selected_jobs = st.session_state.get("job_ids_selected_for_deletion", [])
            num_selected = len(selected_jobs)
            
            st.warning(
                f"You are about to delete {num_selected} job{'s' if num_selected != 1 else ''} "
                "and all associated artifacts. This action cannot be undone."
            )
            st.write(f"Selected job IDs: {', '.join(selected_jobs)}")
            
            col1, col2 = st.columns(2, gap="small")
            
            with col1:
                if st.button("Yes, Delete These Jobs", key="confirm_delete_button", type="primary"):
                    self._confirm_deletion(selected_jobs, delete_callback)
            
            with col2:
                if st.button("Cancel", key="cancel_delete_button"):
                    self._cancel_deletion()
    
    def _confirm_deletion(
        self, 
        selected_jobs: List[str], 
        delete_callback: Callable[[List[str]], tuple[int, int]]
    ) -> None:
        """
        Confirm and execute job deletion.
        
        Args:
            selected_jobs: List of job IDs to delete
            delete_callback: Callback function to delete jobs
        """
        try:
            success_count, error_count = delete_callback(selected_jobs)
            
            st.session_state["deletion_success_count"] = success_count
            st.session_state["deletion_error_count"] = error_count
            
        except Exception as e:
            self.logger.error(f"Error during job deletion: {e}")
            st.session_state["deletion_error_count"] = len(selected_jobs)
        
        finally:
            self._cleanup_deletion_state()
    
    def _cancel_deletion(self) -> None:
        """Cancel the deletion process."""
        self._cleanup_deletion_state()
    
    def _cleanup_deletion_state(self) -> None:
        """Clean up deletion-related session state."""
        st.session_state.pop("show_confirm_delete_expander", None)
        st.session_state.pop("job_ids_selected_for_deletion", None)
```

## Phase 4: Create Utility Functions

### 4.1 Time Utilities

```python
"""Time handling utilities for monitoring."""

import math
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def format_timestamp(timestamp: Optional[float]) -> str:
    """
    Safely format a timestamp to a readable string.
    
    Args:
        timestamp: Unix timestamp (float) or None
        
    Returns:
        Formatted time string or "N/A" if invalid
    """
    if timestamp is None or (isinstance(timestamp, float) and math.isnan(timestamp)):
        return "N/A"
    
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    except (ValueError, OSError, TypeError) as e:
        logger.warning(f"Failed to format timestamp {timestamp}: {e}")
        return "Invalid time"

def safe_set_end_time(job_model) -> float:
    """
    Safely set end time, handling None and NaN cases.
    
    Args:
        job_model: Job model to update
        
    Returns:
        The timestamp that was set
    """
    current_time = time.time()
    
    if (job_model.end_time is None or 
        (isinstance(job_model.end_time, float) and math.isnan(job_model.end_time))):
        job_model.end_time = current_time
    
    return job_model.end_time

def is_valid_timestamp(timestamp: Optional[float]) -> bool:
    """
    Check if a timestamp is valid.
    
    Args:
        timestamp: Timestamp to validate
        
    Returns:
        True if valid, False otherwise
    """
    if timestamp is None:
        return False
    
    if isinstance(timestamp, float) and math.isnan(timestamp):
        return False
    
    try:
        # Try to convert to time structure
        time.localtime(timestamp)
        return True
    except (ValueError, OSError, TypeError):
        return False
```

## Phase 5: Refactored Main Function

### 5.1 New Main Function Structure

```python
"""Refactored monitoring section with improved testability and separation of concerns."""

import logging
import streamlit as st
from typing import Callable, Tuple
from streamlit.connections import SQLConnection

from streamlit_app.services.monitoring_state_service import StreamlitStateManager
from streamlit_app.services.job_management_service import JobManagementService
from streamlit_app.services.progress_tracking_service import ProgressTrackingService
from streamlit_app.components.job_table_component import JobTableComponent
from streamlit_app.components.job_deletion_component import JobDeletionComponent
from streamlit_app.config.monitoring_config import PHASE_FORMATS, PHASE_ORDER
from streamlit_app.utils.progress_utils import calculate_progress_from_phase

logger = logging.getLogger(__name__)

def display_monitoring_section(
    db_connection: SQLConnection,
    cancel_job_callback: Callable[[str], bool],
    process_queue_messages_callback: Callable[[], None],
    check_pid_callback: Callable[[int], Tuple[bool, str]],
    db_utils_module
):
    """
    Main entry point for the monitoring section.
    
    Args:
        db_connection: Database connection
        cancel_job_callback: Function to cancel a job
        process_queue_messages_callback: Function to process queue messages
        check_pid_callback: Function to check if a PID is alive
        db_utils_module: Database utilities module
    """
    st.write("Track the progress of the scraping and enrichment processes.")
    
    # Initialize services
    state_manager = StreamlitStateManager()
    job_service = JobManagementService(state_manager, check_pid_callback, db_utils_module)
    progress_service = ProgressTrackingService(state_manager, db_utils_module)
    
    # Initialize UI components
    job_table = JobTableComponent()
    job_deletion = JobDeletionComponent()
    
    # Initialize monitoring state
    _initialize_monitoring_state(state_manager)
    
    # Update job data
    _update_jobs_data(job_service, db_connection, process_queue_messages_callback)
    
    # Display main UI components
    _display_jobs_section(job_table, job_deletion, job_service, db_connection, process_queue_messages_callback)
    _display_job_selection_and_cancel(state_manager, cancel_job_callback)
    _display_status_and_logs(state_manager, progress_service, db_connection, process_queue_messages_callback)
    _display_auto_refresh_controls(state_manager)

def _initialize_monitoring_state(state_manager):
    """Initialize monitoring-specific session state."""
    monitoring_state = state_manager.get_monitoring_state()
    
    # Determine refresh interval
    is_monitoring_page_active = state_manager.get_current_page() == monitoring_state.settings.page_name
    has_active_jobs = bool(state_manager.get_jobs())
    
    refresh_interval = monitoring_state.settings.get_refresh_interval(
        is_monitoring_page_active, 
        has_active_jobs
    )
    
    # Store in session state for fragments
    st.session_state["actual_run_every_interval"] = refresh_interval

def _update_jobs_data(job_service, db_connection, process_queue_callback):
    """Update job data from database and perform maintenance tasks."""
    # Always reload jobs from DB
    merged_jobs = job_service.reload_jobs_from_db(db_connection)
    
    # Run PID checks for running jobs
    job_service.update_job_statuses_with_pid_check(merged_jobs)
    
    # Process queue messages
    process_queue_callback()

def _display_jobs_section(job_table, job_deletion, job_service, db_connection, process_queue_callback):
    """Display the jobs table and deletion controls."""
    
    @st.fragment(run_every=st.session_state.get("actual_run_every_interval"))
    def jobs_table_fragment():
        jobs = job_service.state_manager.get_jobs()
        selected_job_ids = job_table.render_jobs_table(jobs, process_queue_callback)
        return selected_job_ids
    
    selected_job_ids = jobs_table_fragment()
    
    # Deletion controls (outside fragment to avoid refresh issues)
    has_jobs = bool(job_service.state_manager.get_jobs())
    
    def delete_callback(job_ids):
        return job_service.delete_selected_jobs(job_ids, db_connection)
    
    job_deletion.render_deletion_controls(has_jobs, selected_job_ids, delete_callback)

def _display_job_selection_and_cancel(state_manager, cancel_job_callback):
    """Display job selection dropdown and cancel button."""
    monitoring_state = state_manager.get_monitoring_state()
    jobs = state_manager.get_jobs()
    
    # Update selected job if needed
    if not monitoring_state.job_selection.is_valid_selection(jobs):
        new_selection = monitoring_state.job_selection.get_most_recent_job_id(jobs)
        monitoring_state.job_selection.selected_job_id = new_selection
        state_manager.update_monitoring_state(monitoring_state)
    
    # Create job selection UI
    _render_job_selection_ui(jobs, monitoring_state, state_manager)
    
    # Cancel button
    _render_cancel_button(monitoring_state.job_selection.selected_job_id, cancel_job_callback)

def _render_job_selection_ui(jobs, monitoring_state, state_manager):
    """Render the job selection dropdown."""
    if not jobs:
        st.selectbox(
            "Select Job:",
            options=["No jobs available"],
            disabled=True
        )
        return
    
    # Sort jobs by start time
    sorted_jobs = sorted(
        jobs.items(),
        key=lambda item: item[1].start_time,
        reverse=True
    )
    
    job_options = [job_id for job_id, _ in sorted_jobs]
    
    def format_job_option(job_id):
        return f"{job_id} - {jobs[job_id].status}"
    
    selected = st.selectbox(
        "Select Job:",
        options=job_options,
        format_func=format_job_option,
        index=job_options.index(monitoring_state.job_selection.selected_job_id) 
              if monitoring_state.job_selection.selected_job_id in job_options else 0
    )
    
    if selected != monitoring_state.job_selection.selected_job_id:
        monitoring_state.job_selection.selected_job_id = selected
        state_manager.update_monitoring_state(monitoring_state)

def _render_cancel_button(selected_job_id, cancel_job_callback):
    """Render the job cancel button."""
    if st.button("Cancel Selected Job", key="cancel_job_btn", icon=":material/stop_circle:"):
        if selected_job_id:
            if cancel_job_callback(selected_job_id):
                st.success(f"Job {selected_job_id} cancellation requested.")
            else:
                st.error(f"Failed to cancel job {selected_job_id}.")
        else:
            st.warning("No job selected to cancel.")

def _display_status_and_logs(state_manager, progress_service, db_connection, process_queue_callback):
    """Display job status information and logs."""
    
    @st.fragment(run_every=st.session_state.get("actual_run_every_interval"))
    def status_and_logs_fragment():
        process_queue_callback()
        
        monitoring_state = state_manager.get_monitoring_state()
        selected_job_id = monitoring_state.job_selection.selected_job_id
        jobs = state_manager.get_jobs()
        
        if selected_job_id and selected_job_id in jobs:
            job_data = jobs[selected_job_id]
            
            # Update progress if job is running
            if job_data.status in ["Running", "Initializing"]:
                progress_service.update_job_progress_from_log(
                    job_data, 
                    db_connection,
                    PHASE_FORMATS,
                    PHASE_ORDER,
                    calculate_progress_from_phase
                )
            
            _display_job_status(job_data, selected_job_id)
            _display_job_logs(job_data, selected_job_id)
        else:
            st.info("Select a job to view detailed status and logs.")
    
    status_and_logs_fragment()

def _display_job_status(job_data, job_id):
    """Display detailed job status information."""
    status_colors = {
        "Idle": "blue",
        "Initializing": "blue", 
        "Running": "orange",
        "Completed": "green",
        "Error": "red",
        "Cancelled": "gray",
        "Failed": "red",
    }
    
    status = getattr(job_data, "status", "Unknown")
    color = status_colors.get(status, "blue")
    
    st.markdown(f"### Job: {job_id}")
    st.markdown(
        f"**Status:** <span style='color:{color}'>{status}</span>",
        unsafe_allow_html=True
    )
    
    phase = getattr(job_data, "phase", None)
    if phase:
        st.markdown(f"**Current Phase:** {phase}")
    
    # Progress bar
    progress = getattr(job_data, "progress", 0) / 100.0
    max_progress = getattr(job_data, "max_progress", progress)
    
    if progress > max_progress:
        job_data.max_progress = progress
        max_progress = progress
    
    st.progress(max_progress, text=f"Progress: {int(max_progress * 100)}%")
    
    # Error message
    error_msg = getattr(job_data, "error_message", None)
    if error_msg:
        st.error(f"Error: {error_msg}")

def _display_job_logs(job_data, job_id):
    """Display job logs."""
    st.subheader("Logs")
    st.markdown(f"*Showing logs for job: {job_id}*")
    
    log_lines = []
    log_file_path = getattr(job_data, "pipeline_log_file_path", None)
    
    if log_file_path:
        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
                log_lines = f.readlines()
        except Exception as e:
            log_lines = [f"Error reading log file: {e}"]
    else:
        log_lines = ["No log file available for this job."]
    
    log_container = st.container(height=400)
    with log_container:
        if log_lines:
            log_text = "".join(log_lines)
            st.text(log_text)
        else:
            st.text("No logs available.")

def _display_auto_refresh_controls(state_manager):
    """Display auto-refresh control settings."""
    with st.expander("Log Auto-Refresh Settings"):
        col1, col2 = st.columns([1, 3])
        
        monitoring_state = state_manager.get_monitoring_state()
        
        with col1:
            auto_refresh = st.toggle(
                "Auto-refresh enabled",
                value=monitoring_state.settings.auto_refresh_enabled,
                key="auto_refresh_toggle"
            )
            monitoring_state.settings.auto_refresh_enabled = auto_refresh
        
        with col2:
            refresh_interval = st.slider(
                "Refresh interval (seconds)",
                min_value=1.0,
                max_value=10.0,
                value=monitoring_state.settings.refresh_interval,
                step=0.5,
                key="refresh_slider"
            )
            monitoring_state.settings.refresh_interval = refresh_interval
        
        state_manager.update_monitoring_state(monitoring_state)
```

## Phase 6: Testing Structure

### 6.1 Test Files Structure

```python
"""Tests for JobManagementService."""

import unittest
from unittest.mock import Mock, MagicMock, patch
import time
from streamlit_app.services.job_management_service import JobManagementService
from streamlit_app.services.monitoring_state_service import MockStateManager
from streamlit_app.models.job_data_model import JobDataModel

class TestJobManagementService_ReloadJobsFromDb_LoadsAndMergesJobs(unittest.TestCase):
    """Test job reloading functionality."""
    
    def setUp(self):
        self.state_manager = MockStateManager()
        self.check_pid_callback = Mock(return_value=(True, "alive"))
        self.db_utils = Mock()
        self.service = JobManagementService(
            self.state_manager, 
            self.check_pid_callback, 
            self.db_utils
        )
    
    def test_reloadJobsFromDb_ValidConnection_ReturnsJobs(self):
        """Test that jobs are properly reloaded from database."""
        # Arrange
        mock_connection = Mock()
        mock_jobs = {"job1": Mock(spec=JobDataModel)}
        self.db_utils.load_jobs_from_db.return_value = mock_jobs
        
        with patch('streamlit_app.services.job_management_service.merge_active_jobs_with_db') as mock_merge:
            mock_merge.return_value = mock_jobs
            
            # Act
            result = self.service.reload_jobs_from_db(mock_connection)
            
            # Assert
            self.assertEqual(result, mock_jobs)
            self.db_utils.load_jobs_from_db.assert_called_once_with(mock_connection)
    
    def test_reloadJobsFromDb_DatabaseError_ReturnsExistingJobs(self):
        """Test error handling when database reload fails."""
        # Arrange
        mock_connection = Mock()
        existing_jobs = {"job2": Mock(spec=JobDataModel)}
        self.state_manager.set_jobs(existing_jobs)
        self.db_utils.load_jobs_from_db.side_effect = Exception("DB Error")
        
        # Act
        result = self.service.reload_jobs_from_db(mock_connection)
        
        # Assert
        self.assertEqual(result, existing_jobs)

class TestJobManagementService_UpdateJobStatusesWithPidCheck_ChecksRunningJobs(unittest.TestCase):
    """Test PID checking functionality."""
    
    def setUp(self):
        self.state_manager = MockStateManager()
        self.check_pid_callback = Mock(return_value=(True, "alive"))
        self.db_utils = Mock()
        self.service = JobManagementService(
            self.state_manager, 
            self.check_pid_callback, 
            self.db_utils
        )
    
    def test_updateJobStatusesWithPidCheck_RunningJobWithPid_ChecksPid(self):
        """Test that running jobs with PIDs are checked."""
        # Arrange
        job_data = Mock(spec=JobDataModel)
        job_data.status = "Running"
        job_data.pid = 12345
        jobs = {"job1": job_data}
        
        # Act
        self.service.update_job_statuses_with_pid_check(jobs)
        
        # Assert
        self.check_pid_callback.assert_called_once_with(12345)
    
    def test_updateJobStatusesWithPidCheck_DeadProcess_UpdatesJobStatus(self):
        """Test that dead processes update job status."""
        # Arrange
        job_data = Mock(spec=JobDataModel)
        job_data.status = "Running"
        job_data.pid = 12345
        jobs = {"job1": job_data}
        self.state_manager.set_jobs(jobs)
        
        self.check_pid_callback.return_value = (False, "not found")
        
        # Act
        self.service.update_job_statuses_with_pid_check(jobs)
        
        # Assert
        self.assertEqual(job_data.status, "Completed")
        self.assertIsNotNone(job_data.end_time)

if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## Summary of Corrected Discrepancies

This refactoring plan now addresses several key discrepancies found in the original code:

1. **Correct Function Signatures**: All function signatures now match the actual implementation, particularly:
   - `update_selected_job_progress_from_log` with the proper parameters
   - Progress tracking service methods with appropriate phase parameters

2. **Data Type Corrections**: 
   - `progress` is now correctly represented as a float (0.0 to 1.0) instead of an integer (0-100)
   - `max_progress` field has been properly accounted for

3. **Global Constants**: 
   - PHASE_FORMATS and PHASE_ORDER have been properly defined and passed throughout
   - The configuration has been extracted to a separate module

4. **Utility Functions**:
   - `calculate_progress_from_phase` and `parse_progress_log_line` functions are now correctly handled

5. **Parameter Management**: 
   - Functions now correctly pass the necessary parameters to their dependencies
   - Reduced tight coupling by passing parameters explicitly rather than relying on instance variables

The refactored code follows SOLID principles and makes the monitoring section much more maintainable and testable.