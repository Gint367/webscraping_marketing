import json
import logging
import time
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from streamlit.connections import SQLConnection

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


def add_or_update_job_in_db(conn: SQLConnection, job_data: Dict[str, Any]) -> None:
    """
    Adds a new job or updates an existing one in the 'jobs' table.

    Args:
        conn: The Streamlit SQLConnection object.
        job_data: A dictionary containing the job's data.
                  Expected keys match the 'jobs' table columns.
                  'config' and 'file_info' should be dicts if provided,
                  or 'config_json' and 'file_info_json' can be pre-serialized strings.
    """
    # Ensure 'id' is present and valid
    if "id" not in job_data or job_data["id"] is None:
        logger.error(
            "Job data must contain a valid 'id' to be added or updated in the database."
        )
        return

    job_id_for_logging = job_data["id"]

    # Add/update last_updated timestamp
    job_data["last_updated"] = time.time()

    # Process 'config' and 'config_json'
    if "config" in job_data and isinstance(job_data["config"], dict):
        job_data["config_json"] = json.dumps(job_data["config"])
        del job_data["config"]  # Remove original dict
    elif "config_json" not in job_data:
        job_data["config_json"] = None

    # Process 'file_info' and 'file_info_json'
    if "file_info" in job_data and isinstance(job_data["file_info"], dict):
        job_data["file_info_json"] = json.dumps(job_data["file_info"])
        del job_data["file_info"]  # Remove original dict
    elif "file_info_json" not in job_data:
        job_data["file_info_json"] = None

    # Ensure *_json fields are either string or None before database operation
    if "config_json" in job_data and not isinstance(
        job_data["config_json"], (str, type(None))
    ):
        logger.warning(
            f"Job '{job_id_for_logging}': 'config_json' was of type {type(job_data['config_json']).__name__}, not string or None. Converting to None."
        )
        job_data["config_json"] = None
    if "file_info_json" in job_data and not isinstance(
        job_data["file_info_json"], (str, type(None))
    ):
        logger.warning(
            f"Job '{job_id_for_logging}': 'file_info_json' was of type {type(job_data['file_info_json']).__name__}, not string or None. Converting to None."
        )
        job_data["file_info_json"] = None

    # Prepare for INSERT OR REPLACE
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
        "last_updated",
    }

    # Rename output_path to output_final_file_path if present
    if "output_path" in job_data:
        job_data["output_final_file_path"] = job_data.pop("output_path")

    db_job_data = {
        k: v for k, v in job_data.items() if k in allowed_columns and v is not None
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
            s.execute(insert_or_replace_query, db_job_data)
            s.commit()
        logger.info(f"Job '{db_job_data['id']}' added/updated in the database.")
    except SQLAlchemyError as e:
        logger.error(
            f"Error adding/updating job '{db_job_data.get('id', job_id_for_logging)}' in database: {e}"
        )
        logger.error(f"Data attempted: {db_job_data}")
        raise


def load_jobs_from_db(conn: SQLConnection) -> Dict[str, Dict[str, Any]]:
    """
    Loads all job records from the 'jobs' table.

    Args:
        conn: The Streamlit SQLConnection object.

    Returns:
        A dictionary where keys are job IDs and values are dictionaries
        of job data, mirroring the structure of st.session_state["active_jobs"].
        JSON string fields ('config_json', 'file_info_json') are deserialized.
    """
    query = text("SELECT * FROM jobs ORDER BY last_updated DESC;")
    active_jobs: Dict[str, Dict[str, Any]] = {}
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

                active_jobs[job_id] = job_data
        logger.info(f"Loaded {len(active_jobs)} jobs from the database.")
    except SQLAlchemyError as e:
        logger.error(f"Error loading jobs from database: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading jobs: {e}")

    return active_jobs


def get_job_from_db(conn: SQLConnection, job_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves a single job by its ID from the database.

    Args:
        conn: The Streamlit SQLConnection object.
        job_id: The ID of the job to retrieve.

    Returns:
        A dictionary containing the job's data if found, else None.
        JSON string fields are deserialized.
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

            logger.info(f"Successfully retrieved job '{job_id}' from database.")
            return job_data
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
            if result.rowcount > 0:
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
            if result.rowcount > 0:
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
