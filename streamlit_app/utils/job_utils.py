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