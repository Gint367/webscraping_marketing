import json
import logging
import os
import shutil  # Add shutil for directory removal
import time
from typing import Any, Dict, List, Optional

import streamlit as st  # Import streamlit for session state access
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from streamlit.connections import SQLConnection

from streamlit_app.models.job_data_model import JobDataModel  # Import the JobDataModel

# Initialize logger for this module
logger = logging.getLogger(__name__)


def init_db(conn: SQLConnection) -> None:
    """
    Initializes the database and creates the 'jobs' table if it doesn't exist.

    Args:
        conn: The Streamlit SQLConnection object.
    """
    create_table_query = text("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        status TEXT,
        progress INTEGER,
        phase TEXT,
        start_time REAL,
        end_time REAL,
        config_json TEXT,
        pipeline_log_file_path TEXT,
        output_final_file_path TEXT,
        error_message TEXT,
        file_info_json TEXT,
        temp_input_csv_path TEXT,
        pid INTEGER,
        last_updated REAL
    );
    """)
    try:
        with conn.session as s:
            s.execute(create_table_query)
            s.commit()
        logger.info("Database initialized and 'jobs' table ensured.")
    except SQLAlchemyError as e:
        logger.error(f"Error initializing database or creating 'jobs' table: {e}")
        raise


def add_or_update_job_in_db(conn: SQLConnection, job_data: JobDataModel) -> None:
    """
    Adds a new job or updates an existing one in the 'jobs' table.

    Args:
        conn: The Streamlit SQLConnection object.
        job_data: A JobDataModel instance containing the job's data.
    """

    # Use a more lenient type check that works across module reloads
    if (
        not hasattr(job_data, "model_dump")
        or job_data.__class__.__name__ != "JobDataModel"
    ):
        logger.error(
            f"add_or_update_job_in_db expects a JobDataModel instance. Type of job_model before DB save: {type(job_data)}"
        )
        return

    job_id_for_logging = job_data.id

    # Prepare dict for DB, exclude non-serializable fields
    db_dict = job_data.model_dump(
        exclude={"process", "status_queue", "log_messages"}, exclude_none=True
    )

    # Add/update last_updated timestamp
    db_dict["last_updated"] = time.time()

    # Serialize config and file_info
    db_dict["config_json"] = json.dumps(db_dict.pop("config", {}))
    db_dict["file_info_json"] = json.dumps(db_dict.pop("file_info", {}))

    allowed_columns = {
        "id",
        "status",
        "progress",
        "phase",
        "start_time",
        "end_time",
        "config_json",
        "pipeline_log_file_path",
        "output_final_file_path",
        "error_message",
        "file_info_json",
        "temp_input_csv_path",
        "pid",
        "last_updated",
    }

    db_job_data = {
        k: v for k, v in db_dict.items() if k in allowed_columns and v is not None
    }

    if not db_job_data.get("id"):
        logger.error(
            f"Job ID '{job_id_for_logging}' is missing or None after filtering for database insertion."
        )
        return

    columns = ", ".join(db_job_data.keys())
    placeholders = ", ".join([f":{key}" for key in db_job_data.keys()])

    insert_or_replace_query = text(f"""
    INSERT OR REPLACE INTO jobs ({columns})
    VALUES ({placeholders});
    """)

    try:
        with conn.session as s:
            logger.debug(
                f"DB_UTILS: Attempting to save job {db_job_data.get('id')}, data: {db_job_data}"
            )  # Temporary log
            s.execute(insert_or_replace_query, db_job_data)
            s.commit()
        logger.info(f"Job '{db_job_data['id']}' added/updated in the database.")
    except SQLAlchemyError as e:
        logger.error(
            f"Error adding/updating job '{db_job_data.get('id', job_id_for_logging)}' in database: {e}"
        )
        logger.error(f"Data attempted: {db_job_data}")
        raise


def load_jobs_from_db(conn: SQLConnection) -> Dict[str, JobDataModel]:
    """
    Loads all job records from the 'jobs' table and returns a dict of JobDataModel instances.

    Args:
        conn: The Streamlit SQLConnection object.

    Returns:
        A dictionary where keys are job IDs and values are JobDataModel instances.
    """
    query = text("SELECT * FROM jobs ORDER BY last_updated DESC;")
    active_jobs: Dict[str, JobDataModel] = {}
    try:
        results_df = conn.query(str(query), ttl=0)

        if results_df is not None and not results_df.empty:
            for index, row in results_df.iterrows():
                job_data = row.to_dict()
                job_id = job_data.get("id")
                if not job_id:
                    logger.warning(f"Skipping job record with missing ID: {job_data}")
                    continue

                config_json_str = job_data.pop("config_json", None)
                if config_json_str:
                    try:
                        job_data["config"] = json.loads(config_json_str)
                    except json.JSONDecodeError:
                        logger.error(
                            f"Error decoding config_json for job {job_id}: '{config_json_str[:100]}...'"
                        )
                        job_data["config"] = {}
                else:
                    job_data["config"] = {}

                file_info_json_str = job_data.pop("file_info_json", None)
                if file_info_json_str:
                    try:
                        job_data["file_info"] = json.loads(file_info_json_str)
                    except json.JSONDecodeError:
                        logger.error(
                            f"Error decoding file_info_json for job {job_id}: '{file_info_json_str[:100]}...'"
                        )
                        job_data["file_info"] = {}
                else:
                    job_data["file_info"] = {}

                # Convert to JobDataModel
                try:
                    job_model = JobDataModel.model_validate(job_data)
                    active_jobs[job_id] = job_model
                except Exception as e:
                    logger.error(f"Failed to create JobDataModel for job {job_id}: {e}")
        logger.info(f"Loaded {len(active_jobs)} jobs from the database.")
    except SQLAlchemyError as e:
        logger.error(f"Error loading jobs from database: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading jobs: {e}")

    return active_jobs


def get_job_from_db(conn: SQLConnection, job_id: str) -> Optional[JobDataModel]:
    """
    Retrieves a single job by its ID from the database and returns a JobDataModel instance if found.

    Args:
        conn: The Streamlit SQLConnection object.
        job_id: The ID of the job to retrieve.

    Returns:
        A JobDataModel instance if found, else None.
    """
    query = text("SELECT * FROM jobs WHERE id = :job_id;")
    try:
        result_df = conn.query(str(query), params={"job_id": job_id})

        if result_df is not None and not result_df.empty:
            job_data = result_df.iloc[0].to_dict()

            config_json_str = job_data.pop("config_json", None)
            if config_json_str:
                try:
                    job_data["config"] = json.loads(config_json_str)
                except json.JSONDecodeError:
                    logger.error(
                        f"Error decoding config_json for job {job_id}: '{config_json_str[:100]}...'"
                    )
                    job_data["config"] = {}
            else:
                job_data["config"] = {}

            file_info_json_str = job_data.pop("file_info_json", None)
            if file_info_json_str:
                try:
                    job_data["file_info"] = json.loads(file_info_json_str)
                except json.JSONDecodeError:
                    logger.error(
                        f"Error decoding file_info_json for job {job_id}: '{file_info_json_str[:100]}...'"
                    )
                    job_data["file_info"] = {}
            else:
                job_data["file_info"] = {}

            try:
                job_model = JobDataModel.model_validate(job_data)
                logger.info(f"Successfully retrieved job '{job_id}' from database.")
                return job_model
            except Exception as e:
                logger.error(f"Failed to create JobDataModel for job {job_id}: {e}")
                return None
        else:
            logger.info(f"Job '{job_id}' not found in database.")
            return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving job '{job_id}' from database: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while retrieving job {job_id}: {e}")
        return None


def delete_job_from_db(conn: SQLConnection, job_id: str) -> bool:
    """
    Deletes a job by its ID from the database.

    Args:
        conn: The Streamlit SQLConnection object.
        job_id: The ID of the job to delete.

    Returns:
        True if deletion was successful or job didn't exist, False on error.
    """
    query = text("DELETE FROM jobs WHERE id = :job_id;")
    try:
        with conn.session as s:
            result = s.execute(query, {"job_id": job_id})
            s.commit()
            # SQLAlchemy 2.x: rowcount is not always available; use returned rowcount if present, else assume success
            deleted = getattr(result, "rowcount", None)
            if deleted is not None and deleted > 0:
                logger.info(f"Job '{job_id}' deleted from the database.")
            else:
                logger.info(
                    f"Job '{job_id}' not found in database for deletion or already deleted."
                )
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error deleting job '{job_id}' from database: {e}")
        return False


def update_job_status_in_db(
    conn: SQLConnection,
    job_id: str,
    status: str,
    end_time: Optional[float] = None,
    error_message: Optional[str] = None,
) -> bool:
    """
    Updates the status, end_time, and optionally error_message of a specific job in the database.

    Args:
        conn: The Streamlit SQLConnection object.
        job_id: The ID of the job to update.
        status: The new status for the job.
        end_time: The new end_time for the job (optional).
        error_message: An error message if the job failed (optional).

    Returns:
        True if the update was successful, False otherwise.
    """
    update_fields = {"status": status, "last_updated": time.time()}
    if end_time is not None:
        update_fields["end_time"] = end_time
    if error_message is not None:
        update_fields["error_message"] = error_message

    set_clauses = ", ".join([f"{key} = :{key}" for key in update_fields.keys()])
    query = text(f"UPDATE jobs SET {set_clauses} WHERE id = :job_id;")

    params = {**update_fields, "job_id": job_id}

    try:
        with conn.session as s:
            result = s.execute(query, params)
            s.commit()
            updated = getattr(result, "rowcount", None)
            if updated is not None and updated > 0:
                logger.info(
                    f"Successfully updated status for job '{job_id}' to '{status}'."
                )
                return True
            else:
                logger.warning(
                    f"Job '{job_id}' not found for status update or no change in values."
                )
                return False
    except SQLAlchemyError as e:
        logger.error(f"Error updating status for job '{job_id}': {e}")
        return False


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
