#!/usr/bin/env python3
"""
Tests for artifact management system.

This module contains tests for the ArtifactManager and StreamlitArtifactInterface classes.
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from utils.artifact_manager import ArtifactManager
from utils.streamlit_artifacts import StreamlitArtifactInterface


class TestArtifactManager(unittest.TestCase):
    """Test cases for ArtifactManager class."""
    
    def setUp(self):
        """Set up test fixture."""
        # Create a temporary directory for test artifacts
        self.test_dir = tempfile.mkdtemp()
        self.manager = ArtifactManager(self.test_dir)
        
        # Create some test files
        self.test_file1 = os.path.join(self.test_dir, "test1.txt")
        self.test_file2 = os.path.join(self.test_dir, "test2.csv")
        
        with open(self.test_file1, "w") as f:
            f.write("Test content 1")
            
        with open(self.test_file2, "w") as f:
            f.write("col1,col2\n1,2\n3,4")
    
    def tearDown(self):
        """Tear down test fixture."""
        shutil.rmtree(self.test_dir)
    
    def test_register_and_get_artifact(self):
        """Test registering and retrieving an artifact."""
        artifact_id = self.manager.register(
            phase="test",
            name="file1",
            path=self.test_file1,
            description="Test file 1"
        )
        
        # Check the returned ID
        self.assertEqual(artifact_id, "test:file1")
        
        # Retrieve the artifact
        artifact = self.manager.get_artifact("test", "file1")
        
        # Check artifact data
        self.assertIsNotNone(artifact)
        self.assertEqual(artifact["path"][0], self.test_file1)
        self.assertEqual(artifact["description"], "Test file 1")
        
        # Test get_artifact_by_id
        artifact2 = self.manager.get_artifact_by_id("test:file1")
        self.assertEqual(artifact, artifact2)
    
    def test_list_artifacts(self):
        """Test listing artifacts."""
        # Register two artifacts
        self.manager.register("test", "file1", self.test_file1, "Test file 1")
        self.manager.register("test", "file2", self.test_file2, "Test file 2")
        
        # List all artifacts
        all_artifacts = self.manager.list_artifacts()
        self.assertEqual(len(all_artifacts), 1)  # One phase
        self.assertEqual(len(all_artifacts["test"]), 2)  # Two artifacts
        
        # List by phase
        phase_artifacts = self.manager.list_artifacts("test")
        self.assertEqual(len(phase_artifacts["test"]), 2)
        
        # Get as list
        artifacts_list = self.manager.as_list()
        self.assertEqual(len(artifacts_list), 2)
        self.assertEqual(artifacts_list[0]["name"], "file1")
    
    def test_prepare_download(self):
        """Test preparing an artifact for download."""
        # Register an artifact
        self.manager.register("test", "file1", self.test_file1, "Test file 1")
        
        # Prepare for download
        download_path = self.manager.prepare_download("test:file1")
        
        # Should return the original path for a single file
        self.assertEqual(download_path, self.test_file1)
        
        # Register multiple files
        self.manager.register(
            "test", 
            "multi", 
            [self.test_file1, self.test_file2],
            "Multiple files"
        )
        
        # Prepare for download
        download_path = self.manager.prepare_download("test:multi")
        
        # Should create a zip file
        self.assertIsNotNone(download_path)
        self.assertTrue(os.path.exists(download_path))
        self.assertTrue(download_path.endswith('.zip'))


class TestStreamlitArtifactInterface(unittest.TestCase):
    """Test cases for StreamlitArtifactInterface class."""
    
    def setUp(self):
        """Set up test fixture."""
        # Create a temporary directory for test artifacts
        self.test_dir = tempfile.mkdtemp()
        self.interface = StreamlitArtifactInterface(self.test_dir)
        
        # Create some test files
        self.test_csv = os.path.join(self.test_dir, "test.csv")
        self.test_txt = os.path.join(self.test_dir, "test.txt")
        
        # Create a CSV file
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c']
        })
        df.to_csv(self.test_csv, index=False)
        
        # Create a text file
        with open(self.test_txt, "w") as f:
            f.write("This is a test file for preview.")
    
    def tearDown(self):
        """Tear down test fixture."""
        shutil.rmtree(self.test_dir)
    
    def test_register_from_pipeline(self):
        """Test registering artifacts from pipeline."""
        # Create a mock pipeline artifacts dict
        pipeline_artifacts = {
            "phase1": {
                "file1": {
                    "path": self.test_csv,
                    "description": "Test CSV"
                }
            }
        }
        
        # Register
        count = self.interface.register_from_pipeline(pipeline_artifacts)
        
        # Should register one artifact
        self.assertEqual(count, 1)
        
        # Check artifacts
        artifacts = self.interface.get_artifacts_by_phase()
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(len(artifacts["phase1"]), 1)
    
    def test_get_artifact_preview(self):
        """Test getting a preview of an artifact."""
        # Register artifacts
        self.interface._manager.register(
            "test", "csv", self.test_csv, "CSV file"
        )
        self.interface._manager.register(
            "test", "txt", self.test_txt, "Text file"
        )
        
        # Get CSV preview
        csv_preview = self.interface.get_artifact_preview("test:csv")
        self.assertIsNotNone(csv_preview)
        self.assertTrue(isinstance(csv_preview, pd.DataFrame))
        self.assertEqual(len(csv_preview), 3)
        
        # Get text preview
        txt_preview = self.interface.get_artifact_preview("test:txt")
        self.assertIsNotNone(txt_preview)
        self.assertTrue(isinstance(txt_preview, pd.DataFrame))
        self.assertEqual(txt_preview["Content"][0], "This is a test file for preview.")


if __name__ == "__main__":
    unittest.main()
