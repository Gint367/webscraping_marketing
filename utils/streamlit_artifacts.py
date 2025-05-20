#!/usr/bin/env python3
"""
Artifact Interface for Streamlit UI

This module provides a streamlined interface for the Streamlit UI to interact with
pipeline artifacts. It wraps the ArtifactManager class and provides simplified methods
specifically designed for UI interaction.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from master_pipeline import PipelineArtifacts
from utils.artifact_manager import ArtifactManager

# Configure logger
logger = logging.getLogger(__name__)


class StreamlitArtifactInterface:
    """
    A simplified interface between the Streamlit UI and pipeline artifacts.
    Provides methods specifically designed for UI interaction with artifacts.
    """

    def __init__(self, storage_dir: Optional[str] = None):
        """
        Initialize the interface with an ArtifactManager.
        
        Args:
            storage_dir: Optional directory for artifact storage
        """
        # Use a default storage location if not provided
        if not storage_dir:
            base_dir = os.environ.get("PIPELINE_STORAGE_DIR", os.path.expanduser("~/.pipeline_artifacts"))
            storage_dir = os.path.join(base_dir, "streamlit_artifacts")
            
        # Ensure storage directory exists
        os.makedirs(storage_dir, exist_ok=True)
        
        self._manager = ArtifactManager(storage_dir)
        self._storage_dir = storage_dir
        logger.info(f"Initialized artifact interface with storage directory: {storage_dir}")
        
        # Load existing metadata if available
        self._manager.load_metadata()

    def get_downloadable_artifacts(self) -> List[Dict]:
        """
        Get a list of downloadable artifacts for display in the UI.
        
        Returns:
            List of artifact metadata dictionaries
        """
        artifacts = self._manager.as_list()
        
        # Filter out non-downloadable artifacts (no valid path)
        downloadable = []
        for artifact in artifacts:
            path = artifact.get("path", artifact.get("sample_path", ""))
            if path and (os.path.exists(path) or (
                isinstance(path, list) and any(os.path.exists(p) for p in path))):
                downloadable.append(artifact)
        
        return downloadable

    def get_artifact_for_download(self, artifact_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Prepare an artifact for download and return the file path and display name.
        
        Args:
            artifact_id: The ID of the artifact (phase:name)
            
        Returns:
            Tuple of (file_path, display_name) or (None, None) if preparation failed
        """
        file_path = self._manager.prepare_download(artifact_id)
        if not file_path:
            return None, None
            
        phase, name = artifact_id.split(':', 1)
        display_name = f"{phase}_{name}_{os.path.basename(file_path)}"
        
        return file_path, display_name

    def get_artifact_preview(self, artifact_id: str, max_rows: int = 100) -> Optional[pd.DataFrame]:
        """
        Get a preview of an artifact's content as a DataFrame (for CSV/Excel files).
        
        Args:
            artifact_id: The ID of the artifact (phase:name)
            max_rows: Maximum number of rows to preview
            
        Returns:
            DataFrame with the artifact content, or None if preview is not available
        """
        artifact = self._manager.get_artifact_by_id(artifact_id)
        if not artifact:
            return None
            
        # Get the first path from the artifact
        paths = artifact.get("path", [])
        if not paths:
            return None
            
        path = paths[0] if isinstance(paths, list) else paths
        
        if not os.path.exists(path):
            logger.error(f"Artifact file not found: {path}")
            return None
            
        try:
            # Determine file type by extension
            ext = os.path.splitext(path)[1].lower()
            
            if ext == '.csv':
                df = pd.read_csv(path, nrows=max_rows)
                return df
            elif ext in ['.xlsx', '.xls']:
                df = pd.read_excel(path, nrows=max_rows)
                return df
            elif ext == '.json':
                df = pd.read_json(path)
                if len(df) > max_rows:
                    df = df.head(max_rows)
                return df
            else:
                # For text files, try to read as text
                if os.path.getsize(path) < 1024 * 1024:  # Limit to 1MB
                    with open(path, 'r') as f:
                        content = f.read(10000)  # Read up to 10KB
                    return pd.DataFrame({'Content': [content]})
                else:
                    return None
        except Exception as e:
            logger.error(f"Error generating preview for {path}: {e}")
            return None

    def get_artifacts_by_phase(self) -> Dict[str, List[Dict]]:
        """
        Get artifacts organized by pipeline phase.
        
        Returns:
            Dictionary mapping phase names to lists of artifacts
        """
        artifacts = self._manager.as_list()
        by_phase = {}
        
        for artifact in artifacts:
            phase = artifact["phase"]
            if phase not in by_phase:
                by_phase[phase] = []
            by_phase[phase].append(artifact)
            
        return by_phase

    def register_from_pipeline(self, pipeline_artifacts: Union['PipelineArtifacts', Dict]) -> int:
        """
        Register artifacts from a pipeline run.
        
        Args:
            pipeline_artifacts: PipelineArtifacts object or dictionary from pipeline run
            
        Returns:
            Number of artifacts registered
        """
        count = 0
        
        # Handle PipelineArtifacts object
        if hasattr(pipeline_artifacts, "as_list"):
            artifacts_list = pipeline_artifacts.as_list()
            for artifact in artifacts_list:
                self._manager.register(
                    phase=artifact["phase"],
                    name=artifact["name"],
                    path=artifact["path"],
                    description=artifact["description"]
                )
                count += 1
                
        # Handle dictionary structure
        elif isinstance(pipeline_artifacts, dict):
            for phase, artifacts in pipeline_artifacts.items():
                for name, info in artifacts.items():
                    path = info.get("path", "")
                    description = info.get("description", "")
                    self._manager.register(
                        phase=phase,
                        name=name,
                        path=path,
                        description=description
                    )
                    count += 1
                    
        logger.info(f"Registered {count} artifacts from pipeline")
        return count

    def clear_artifacts(self) -> None:
        """Clear all artifact registrations."""
        self._manager.clear()
        logger.info("Cleared all artifact registrations")
