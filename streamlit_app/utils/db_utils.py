import json
import logging
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from streamlit.connections import SQLConnection

from streamlit_app.app import JobDataModel  # Import the JobDataModel

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
        job_data: A JobDataModel instance containing the job's data.
    """
    # Accept only JobDataModel
    if not isinstance(job_data, JobDataModel):
        logger.error("add_or_update_job_in_db expects a JobDataModel instance.")
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
