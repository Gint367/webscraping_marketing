#!/usr/bin/env python3
"""
Streamlit UI components for artifact management.

This module provides UI components for displaying and interacting with
pipeline artifacts in a Streamlit application.
"""

import logging
import os
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from utils.streamlit_artifacts import StreamlitArtifactInterface

logger = logging.getLogger(__name__)


class ArtifactDisplay:
    """
    Handles the display and interaction with artifacts in Streamlit UI.
    """
    
    def __init__(self, storage_dir: Optional[str] = None):
        """
        Initialize the display with an artifact interface.
        
        Args:
            storage_dir: Optional directory for artifact storage
        """
        self.interface = StreamlitArtifactInterface(storage_dir)
        
    def display_artifact_section(self, container=None) -> None:
        """
        Display the artifact management section in the UI.
        
        Args:
            container: Optional Streamlit container to render in
        """
        target = container if container else st
        
        with target.expander("Pipeline Artifacts", expanded=True):
            st.write("View and download intermediate artifacts from the pipeline process.")
            
            # Get artifacts by phase
            artifacts_by_phase = self.interface.get_artifacts_by_phase()
            
            if not artifacts_by_phase:
                st.info("No artifacts available yet. Run a pipeline to generate artifacts.")
                return
                
            # Create tabs for each phase
            phase_tabs = st.tabs(list(artifacts_by_phase.keys()))
            
            # Display artifacts in each tab
            for i, (phase, artifacts) in enumerate(artifacts_by_phase.items()):
                with phase_tabs[i]:
                    self._display_phase_artifacts(phase, artifacts)
    
    def _display_phase_artifacts(self, phase: str, artifacts: List[Dict]) -> None:
        """
        Display artifacts for a specific pipeline phase.
        
        Args:
            phase: Pipeline phase name
            artifacts: List of artifacts for this phase
        """
        if not artifacts:
            st.info(f"No artifacts available for {phase} phase.")
            return
            
        st.write(f"### {phase.title()} Phase Artifacts")
        
        # Create a table of artifacts with download buttons
        df_data = []
        for artifact in artifacts:
            name = artifact.get("name", "")
            desc = artifact.get("description", "")
            file_type = self._determine_file_type(artifact)
            size = artifact.get("filesize", {}).get("human_readable", "Unknown")
            timestamp = artifact.get("timestamp", "").split("T")[0]  # just date part
            
            df_data.append({
                "Artifact": name,
                "Description": desc,
                "Type": file_type,
                "Size": size,
                "Date": timestamp,
                "id": artifact.get("id", "")  # hidden column for reference
            })
            
        if df_data:
            df = pd.DataFrame(df_data)
            # Temporarily hiding the ID column
            display_columns = ["Artifact", "Description", "Type", "Size", "Date"]
            st.dataframe(df[display_columns], use_container_width=True)
            
            # Artifact selection for preview and download
            selected_artifact_id = None
            
            col1, col2 = st.columns([3, 1])
            with col1:
                selected_idx = st.selectbox(
                    "Select artifact to preview or download:",
                    options=list(range(len(df_data))),
                    format_func=lambda i: f"{df_data[i]['Artifact']} - {df_data[i]['Description'][:30]}...",
                    key=f"select_{phase}"
                )
                
                if selected_idx is not None:
                    selected_artifact_id = df_data[selected_idx]["id"]
            
            with col2:
                if selected_artifact_id:
                    file_path, display_name = self.interface.get_artifact_for_download(selected_artifact_id)
                    
                    if file_path and os.path.exists(file_path):
                        with open(file_path, "rb") as file:
                            st.download_button(
                                label="Download",
                                data=file,
                                file_name=display_name or os.path.basename(file_path),
                                mime="application/octet-stream",
                                key=f"download_{phase}_{selected_artifact_id}"
                            )
            
            # Show preview if applicable
            if selected_artifact_id:
                self._display_artifact_preview(selected_artifact_id)
    
    def _display_artifact_preview(self, artifact_id: str) -> None:
        """
        Display a preview of the selected artifact.
        
        Args:
            artifact_id: ID of the artifact to preview
        """
        st.write("### Preview")
        
        preview_data = self.interface.get_artifact_preview(artifact_id)
        
        if preview_data is not None:
            if isinstance(preview_data, pd.DataFrame):
                if "Content" in preview_data.columns and len(preview_data) == 1:
                    # This is a text content preview
                    st.text_area(
                        "File content (first 10KB):", 
                        value=preview_data["Content"].iloc[0],
                        height=300
                    )
                else:
                    # This is a tabular data preview
                    st.dataframe(preview_data, use_container_width=True)
            else:
                st.info("Preview not available for this artifact type.")
        else:
            st.info("No preview available for this artifact.")
    
    @staticmethod
    def _determine_file_type(artifact: Dict) -> str:
        """Determine a user-friendly file type from artifact metadata."""
        if artifact.get("type") == "directory":
            return "Directory"
            
        paths = artifact.get("path", [])
        if not paths:
            return "Unknown"
            
        path = paths[0] if isinstance(paths, list) else paths
        
        if isinstance(path, str):
            ext = os.path.splitext(path)[1].lower()
            if ext == '.csv':
                return "CSV"
            elif ext in ['.xlsx', '.xls']:
                return "Excel"
            elif ext == '.json':
                return "JSON"
            elif ext == '.md':
                return "Markdown"
            elif ext == '.html':
                return "HTML"
            elif ext == '.txt':
                return "Text"
            elif ext == '.zip':
                return "Archive"
            else:
                return ext.strip('.').upper() if ext else "File"
        return "File"
