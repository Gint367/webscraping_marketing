#!/usr/bin/env python3
"""
Streamlit UI handler for pipeline artifacts.

This module provides functions to display the artifacts section in a Streamlit app.
"""

import logging
import os
from typing import Optional

import streamlit as st

from streamlit_app.components.artifact_display import ArtifactDisplay

logger = logging.getLogger(__name__)


def get_artifact_display() -> ArtifactDisplay:
    """
    Get or initialize the ArtifactDisplay instance.
    
    Returns:
        ArtifactDisplay: The singleton instance
    """
    if 'artifact_display' not in st.session_state:
        # Set a default storage location based on environment
        storage_dir = os.environ.get(
            "ARTIFACT_STORAGE_DIR", 
            os.path.join(os.path.expanduser("~"), ".pipeline_artifacts", "streamlit")
        )
        
        # Create singleton instance
        st.session_state['artifact_display'] = ArtifactDisplay(storage_dir)
        logger.info(f"Initialized ArtifactDisplay with storage directory: {storage_dir}")
    
    return st.session_state['artifact_display']


def display_artifacts_section(container=None) -> None:
    """
    Display the artifacts section in the Streamlit UI.
    
    Args:
        container: Optional Streamlit container to render in
    """
    display = get_artifact_display()
    display.display_artifact_section(container)


def register_pipeline_results(pipeline_output: Optional[dict] = None) -> None:
    """
    Register artifacts from a pipeline run.
    
    Args:
        pipeline_output: Optional dictionary with artifacts from pipeline run
    """
    if not pipeline_output:
        logger.warning("No pipeline output provided to register")
        return
    
    display = get_artifact_display()
    
    # Extract artifacts if present
    if isinstance(pipeline_output, tuple) and len(pipeline_output) == 2:
        # Extract artifacts object from tuple (path, artifacts)
        artifacts = pipeline_output[1]
        
        # Register artifacts
        count = display.interface.register_from_pipeline(artifacts)
        logger.info(f"Registered {count} artifacts from pipeline run")
        
        # Record the artifact registration in session state
        if 'pipeline_artifacts_count' not in st.session_state:
            st.session_state['pipeline_artifacts_count'] = 0
        st.session_state['pipeline_artifacts_count'] += count
    else:
        logger.warning("Pipeline output didn't contain artifacts in expected format")
