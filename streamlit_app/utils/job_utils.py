import os
import shutil
import logging
import streamlit as st
from typing import Any, Dict

from streamlit_app.models.job_data_model import JobDataModel

logger = logging.getLogger(__name__)

def delete_job_and_artifacts(
    conn: Any,
    job_id: str,
    active_jobs_from_state: Dict[str, JobDataModel]
) -> bool:
    """
    Deletes a job and its associated artifacts from both the database and filesystem.
    
    This function handles the complete removal of a job, including:
    - Deletion from database
    - Retrieval of job data for artifact cleanup
    - Deletion of filesystem artifacts (log files, output directories, temp files)
    - Cleanup of session state entries
    
    Args:
        conn: The Streamlit SQLConnection object.
        job_id: The ID of the job to delete.
        active_jobs_from_state: Dictionary of active jobs from session state.
        
    Returns:
        True if deletion was successful or job didn't exist, False on error.
    """
    logger.info(f"Starting deletion process for job '{job_id}'")
    # Step 1: Retrieve the JobDataModel
    job_data = active_jobs_from_state.get(job_id)
    # If not in session state, attempt to load from database
    from streamlit_app.utils.db_utils import get_job_from_db, delete_job_from_db
    if job_data is None:
        logger.info(f"Job '{job_id}' not found in session state, attempting to load from database")
        try:
            job_data = get_job_from_db(conn, job_id)
            if job_data is None:
                logger.warning(f"Job '{job_id}' not found in database, considering it already deleted")
                return True
        except Exception as e:
            logger.error(f"Error loading job '{job_id}' from database: {e}")
            return False
    # Step 2: Delete filesystem artifacts if job_data was retrieved
    if job_data:
        # Delete pipeline log file
        if job_data.pipeline_log_file_path:
            try:
                if os.path.exists(job_data.pipeline_log_file_path):
                    os.remove(job_data.pipeline_log_file_path)
                    logger.info(f"Successfully deleted pipeline log file: {job_data.pipeline_log_file_path}")
                else:
                    logger.info(f"Pipeline log file not found (already deleted or never created): {job_data.pipeline_log_file_path}")
            except OSError as e:
                logger.error(f"Error deleting pipeline log file {job_data.pipeline_log_file_path}: {e}")
        # Delete job-specific output directory
        output_dir = None
        try:
            if job_data.config and 'output_dir' in job_data.config:
                output_dir = job_data.config['output_dir']
            elif job_data.config and 'output_base_dir' in job_data.config:
                # Construct job-specific output directory path
                output_dir = os.path.join(job_data.config['output_base_dir'], job_id)
            if output_dir and os.path.exists(output_dir) and os.path.isdir(output_dir):
                shutil.rmtree(output_dir)
                logger.info(f"Successfully deleted job output directory: {output_dir}")
            elif output_dir:
                logger.info(f"Job output directory not found (already deleted or never created): {output_dir}")
        except OSError as e:
            logger.error(f"Error deleting job output directory {output_dir}: {e}")
        # Delete temporary input CSV file
        if job_data.temp_input_csv_path:
            try:
                if os.path.exists(job_data.temp_input_csv_path):
                    os.remove(job_data.temp_input_csv_path)
                    logger.info(f"Successfully deleted temporary input CSV file: {job_data.temp_input_csv_path}")
                else:
                    logger.info(f"Temporary input CSV file not found (already deleted or never created): {job_data.temp_input_csv_path}")
            except OSError as e:
                logger.error(f"Error deleting temporary input CSV file {job_data.temp_input_csv_path}: {e}")
    # Step 3: Delete from database (primary success/failure indicator)
    db_deletion_success = delete_job_from_db(conn, job_id)
    if db_deletion_success:
        # Step 4: Clean up session state entries if database deletion was successful
        # Remove job from active_jobs in session state if it exists
        if hasattr(st, 'session_state'):
            if 'active_jobs' in st.session_state and job_id in st.session_state.active_jobs:
                st.session_state.active_jobs.pop(job_id, None)
                logger.info(f"Removed job '{job_id}' from session state active_jobs")
            # Remove log file position tracking for this job
            if 'log_file_positions' in st.session_state and job_id in st.session_state.log_file_positions:
                st.session_state.log_file_positions.pop(job_id, None)
                logger.info(f"Removed job '{job_id}' from session state log_file_positions")
    else:
        logger.error(f"Failed to delete job '{job_id}' from database")
    logger.info(f"Completed deletion process for job '{job_id}'")
    return db_deletion_success

def merge_active_jobs_with_db(active_jobs: dict, db_jobs: dict) -> dict:
    """
    Merge jobs loaded from DB into the current active_jobs dict,
    preserving in-memory fields like process and status_queue.

    Args:
        active_jobs (dict): Current in-memory jobs (may have process objects).
        db_jobs (dict): Jobs loaded from the database.

    Returns:
        dict: Merged jobs dictionary.
    """
    merged = {}
    for job_id, db_job in db_jobs.items():
        if job_id in active_jobs:
            mem_job = active_jobs[job_id]
            # Copy all DB fields to mem_job, except in-memory only fields
            for field, value in db_job.model_dump().items():
                if field not in {"process", "status_queue"}:
                    setattr(mem_job, field, value)
            merged[job_id] = mem_job
        else:
            merged[job_id] = db_job
    # Optionally, keep jobs that are only in memory (not in DB)
    for job_id, mem_job in active_jobs.items():
        if job_id not in merged:
            merged[job_id] = mem_job
    return merged