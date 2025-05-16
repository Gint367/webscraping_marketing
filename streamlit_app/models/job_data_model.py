import time
from multiprocessing import Process
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobDataModel(BaseModel):
    """
    Represents the data structure for a single job in the application.
    Uses Pydantic for data validation, type hinting, and settings management.
    """

    id: str
    status: str
    progress: float = 0.0
    phase: str
    start_time: float = Field(default_factory=time.time)
    end_time: Optional[float] = None
    process: Optional[Process] = None
    status_queue: Optional[Any] = None  # Using Any to support both Queue types
    config: Dict[str, Any] = Field(default_factory=dict)
    output_final_file_path: Optional[str] = None
    error_message: Optional[str] = None
    pipeline_log_file_path: Optional[str] = None
    temp_input_csv_path: Optional[str] = None
    file_info: Dict[str, Any] = Field(default_factory=dict)
    log_messages: List[str] = Field(default_factory=list)
    max_progress: float = 0.0

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True  # Needed for multiprocessing.Process and Queue
        validate_assignment = False
