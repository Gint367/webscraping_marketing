#!/usr/bin/env python3
"""
Artifact Manager for Pipeline Outputs

This module provides a centralized system for managing artifacts produced by different
pipeline phases. It allows registration, retrieval, and download preparation of artifacts,
making them accessible to the UI layers and other components.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Configure logger
logger = logging.getLogger(__name__)


class ArtifactManager:
    """
    Manages artifacts produced by pipeline phases, providing a unified interface
    for registration, retrieval, and download preparation of intermediate and final outputs.
    """

    def __init__(self, storage_root: Optional[str] = None) -> None:
        """
        Initialize the artifact manager.
        
        Args:
            storage_root: Optional root directory for artifact storage.
                         If None, artifacts are only tracked by reference.
        """
        self._artifacts: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._storage_root = Path(storage_root) if storage_root else None
        self._metadata_file = Path(self._storage_root) / "artifacts_metadata.json" if self._storage_root else None
        
        # Create storage directory if specified and doesn't exist
        if self._storage_root and not self._storage_root.exists():
            self._storage_root.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created artifact storage directory: {self._storage_root}")

    def register(self,
                phase: str,
                name: str,
                path: Union[str, List[str]],
                description: str = "",
                artifact_type: str = "file",
                metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Register an artifact for a pipeline phase.
        
        Args:
            phase: Name of the pipeline phase (e.g., 'extracting_machine')
            name: Artifact name (e.g., 'filtered_csv')
            path: Path(s) to the artifact file(s)
            description: Optional description of the artifact
            artifact_type: Type of artifact ('file', 'directory', 'data')
            metadata: Additional metadata for the artifact
            
        Returns:
            str: Unique artifact ID (phase:name)
        """
        artifact_id = f"{phase}:{name}"
        
        if phase not in self._artifacts:
            self._artifacts[phase] = {}
            
        # Convert single path to list for consistency
        if isinstance(path, str):
            path_list = [path]
        else:
            path_list = path
            
        # Create metadata dictionary
        artifact_metadata = {
            "path": path_list,
            "description": description,
            "type": artifact_type,
            "timestamp": datetime.now().isoformat(),
            "filesize": self._get_size_info(path_list),
            "custom_metadata": metadata or {}
        }
        
        self._artifacts[phase][name] = artifact_metadata
        logger.debug(f"Registered artifact: {artifact_id}")
        
        # Save metadata if storage is enabled
        self._save_metadata()
        
        return artifact_id

    def _get_size_info(self, paths: List[str]) -> Dict[str, Any]:
        """Calculate size information for files or directories."""
        size_info = {"total_bytes": 0, "file_count": 0}
        
        for path_str in paths:
            path = Path(path_str)
            if not path.exists():
                continue
                
            if path.is_file():
                size_info["total_bytes"] += path.stat().st_size
                size_info["file_count"] += 1
            elif path.is_dir():
                for file_path in path.glob('**/*'):
                    if file_path.is_file():
                        size_info["total_bytes"] += file_path.stat().st_size
                        size_info["file_count"] += 1
                        
        # Add human-readable size
        size_info["human_readable"] = self._format_size(size_info["total_bytes"])
        return size_info

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes to human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0 or unit == 'GB':
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0

    def list_artifacts(self, phase: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        List all registered artifacts, optionally filtered by phase.
        
        Args:
            phase: If provided, only list artifacts for this phase.
            
        Returns:
            Dict of artifacts.
        """
        if phase:
            return {phase: self._artifacts.get(phase, {})}
        return self._artifacts

    def get_artifact(self, phase: str, name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific artifact by phase and name.
        
        Args:
            phase: Pipeline phase
            name: Artifact name
            
        Returns:
            Dict with artifact metadata, or None if not found.
        """
        return self._artifacts.get(phase, {}).get(name)

    def get_artifact_by_id(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific artifact by its ID (phase:name).
        
        Args:
            artifact_id: Artifact ID in format "phase:name"
            
        Returns:
            Dict with artifact metadata, or None if not found.
        """
        try:
            phase, name = artifact_id.split(':', 1)
            return self.get_artifact(phase, name)
        except (ValueError, AttributeError):
            logger.error(f"Invalid artifact ID format: {artifact_id}")
            return None

    def as_list(self) -> List[Dict[str, Any]]:
        """
        Return all artifacts as a flat list (for UI display).
        
        Returns:
            List of dicts with phase, name, path, description, etc.
        """
        result = []
        for phase, artifacts in self._artifacts.items():
            for name, info in artifacts.items():
                artifact_data = {
                    "id": f"{phase}:{name}",
                    "phase": phase,
                    "name": name,
                    "description": info.get("description", ""),
                    "type": info.get("type", "file"),
                    "timestamp": info.get("timestamp", ""),
                    "filesize": info.get("filesize", {}),
                }
                
                # Add specific path info based on type
                if info.get("type") == "directory" or isinstance(info.get("path"), list) and len(info.get("path", [])) > 1:
                    artifact_data["path_count"] = len(info.get("path", []))
                    artifact_data["sample_path"] = info.get("path", [""])[0] if info.get("path") else ""
                else:
                    artifact_data["path"] = info.get("path", [""])[0] if isinstance(info.get("path"), list) else info.get("path", "")
                
                result.append(artifact_data)
        return result

    def get_phases(self) -> List[str]:
        """
        Get a list of all registered pipeline phases.
        
        Returns:
            List of phase names.
        """
        return list(self._artifacts.keys())

    def prepare_download(self, artifact_id: str) -> Optional[str]:
        """
        Prepare an artifact for download, potentially creating a zip for multiple files.
        
        Args:
            artifact_id: Artifact ID in format "phase:name"
            
        Returns:
            Path to the downloadable file, or None if preparation failed.
        """
        artifact = self.get_artifact_by_id(artifact_id)
        if not artifact:
            logger.error(f"Artifact not found: {artifact_id}")
            return None
            
        paths = artifact.get("path", [])
        if not paths:
            logger.error(f"No paths found for artifact: {artifact_id}")
            return None
            
        # Handle single file case
        if len(paths) == 1 and Path(paths[0]).is_file():
            return paths[0]
            
        # For multiple files or directories, create a zip archive
        if not self._storage_root:
            logger.error("Cannot prepare download: storage_root not set")
            return None
            
        phase, name = artifact_id.split(':', 1)
        zip_path = self._storage_root / f"{phase}_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        # Import here to avoid dependency at module level
        import zipfile
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for path_str in paths:
                    path = Path(path_str)
                    if path.is_file():
                        zipf.write(path, arcname=path.name)
                    elif path.is_dir():
                        for file_path in path.glob('**/*'):
                            if file_path.is_file():
                                # Calculate relative path for archive
                                rel_path = file_path.relative_to(path.parent)
                                zipf.write(file_path, arcname=str(rel_path))
            
            logger.info(f"Created download archive: {zip_path}")
            return str(zip_path)
        except Exception as e:
            logger.error(f"Failed to create download archive: {e}")
            return None

    def archive_artifacts(self, run_id: str) -> Optional[str]:
        """
        Archive all registered artifacts for long-term storage.
        
        Args:
            run_id: Unique identifier for this pipeline run
            
        Returns:
            Path to the archive, or None if archiving failed.
        """
        if not self._storage_root:
            logger.error("Cannot archive artifacts: storage_root not set")
            return None
            
        archive_dir = self._storage_root / "archives" / run_id
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy metadata
        archive_metadata = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "artifacts": self._artifacts
        }
        
        with open(archive_dir / "metadata.json", 'w') as f:
            json.dump(archive_metadata, f, indent=2)
            
        # Create an index of all artifact paths
        artifact_paths = []
        for phase, artifacts in self._artifacts.items():
            for name, info in artifacts.items():
                paths = info.get("path", [])
                if isinstance(paths, str):
                    artifact_paths.append(paths)
                else:
                    artifact_paths.extend(paths)
        
        # Copy files to archive (optional, can be resource-intensive)
        # This is commented out as it might duplicate large files
        # for path_str in artifact_paths:
        #     path = Path(path_str)
        #     if path.is_file():
        #         dest = archive_dir / path.name
        #         shutil.copy2(path, dest)
        
        logger.info(f"Archived artifacts metadata to: {archive_dir}")
        return str(archive_dir)

    def _save_metadata(self) -> None:
        """Save artifact metadata to storage if enabled."""
        if self._metadata_file:
            try:
                with open(self._metadata_file, 'w') as f:
                    json.dump({
                        "last_updated": datetime.now().isoformat(),
                        "artifacts": self._artifacts
                    }, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to save artifact metadata: {e}")

    def load_metadata(self) -> bool:
        """
        Load artifact metadata from storage.
        
        Returns:
            bool: True if metadata was loaded successfully, False otherwise.
        """
        if not self._metadata_file or not self._metadata_file.exists():
            return False
            
        try:
            with open(self._metadata_file, 'r') as f:
                data = json.load(f)
                self._artifacts = data.get("artifacts", {})
            logger.info(f"Loaded artifact metadata from: {self._metadata_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to load artifact metadata: {e}")
            return False

    def clear(self) -> None:
        """Clear all registered artifacts."""
        self._artifacts = {}
        self._save_metadata()
        logger.info("Cleared all artifact registrations")
