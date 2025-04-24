#!/usr/bin/env python3
"""
Unit tests for the master_pipeline.py script.

These tests follow Test-Driven Development (TDD) principles to define
the expected behavior of the master pipeline script before implementation.
"""

import json
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

# Ensure the scraper package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


class TestMasterPipeline(unittest.TestCase):
    """Test suite for the master_pipeline.py script."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create temporary directories and files for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)
        
        # Create test input CSV
        self.input_csv = self.test_dir / "input_companies.csv"
        with open(self.input_csv, 'w') as f:
            f.write("name,location,url\n")
            f.write("Test Company 1,Berlin,https://example1.com\n")
            f.write("Test Company 2,Munich,https://example2.com\n")
        
        # Create test config file
        self.config_file = self.test_dir / "test_config.json"
        test_config = {
            "default_output_dir": str(self.test_dir / "output"),
            "log_level": "INFO",
            "crawl_settings": {
                "max_pages": 10,
                "max_depth": 2,
                "timeout": 30
            },
            "llm_settings": {
                "temperature": 0.7,
                "max_retries": 3
            }
        }
        with open(self.config_file, 'w') as f:
            json.dump(test_config, f, indent=2)
        
        # Create expected output directory
        self.output_dir = self.test_dir / "output"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Save paths for testing
        self.log_file = self.test_dir / "pipeline.log"

    def tearDown(self):
        """Clean up after each test."""
        self.temp_dir.cleanup()

    def test_argument_parsing_required_args(self):
        """
        Test that argument parsing correctly handles required arguments.
        
        Method being tested: parse_arguments
        Scenario: Required arguments provided
        Expected behavior: Arguments are parsed correctly
        """
        with patch('sys.argv', ['master_pipeline.py', 
                               '--input-csv', str(self.input_csv),
                               '--output-dir', str(self.output_dir)]):
            from master_pipeline import parse_arguments
            
            args = parse_arguments()
            
            self.assertEqual(args.input_csv, Path(str(self.input_csv)))
            self.assertEqual(args.output_dir, Path(str(self.output_dir)))
            self.assertIsNone(args.category)
            self.assertFalse(args.verbose)

    def test_argument_parsing_all_args(self):
        """
        Test that argument parsing correctly handles all arguments.
        
        Method being tested: parse_arguments
        Scenario: All arguments provided
        Expected behavior: All arguments are parsed correctly
        """
        with patch('sys.argv', ['master_pipeline.py', 
                               '--input-csv', str(self.input_csv),
                               '--output-dir', str(self.output_dir),
                               '--category', 'maschinenbauer',
                               '--config-file', str(self.config_file),
                               '--verbose']):
            from master_pipeline import parse_arguments
            
            args = parse_arguments()
            
            self.assertEqual(args.input_csv, Path(str(self.input_csv)))
            self.assertEqual(args.output_dir, Path(str(self.output_dir)))
            self.assertEqual(args.category, 'maschinenbauer')
            self.assertEqual(args.config_file, Path(str(self.config_file)))
            self.assertTrue(args.verbose)

    def test_argument_parsing_missing_required(self):
        """
        Test that argument parsing raises an error when required arguments are missing.
        
        Method being tested: parse_arguments
        Scenario: Missing required arguments
        Expected behavior: SystemExit is raised
        """
        with patch('sys.argv', ['master_pipeline.py']):
            from master_pipeline import parse_arguments
            
            with self.assertRaises(SystemExit):
                parse_arguments()

    def test_input_validation_file_exists(self):
        """
        Test input validation when input file exists.
        
        Method being tested: validate_inputs
        Scenario: Input file exists
        Expected behavior: No exception is raised
        """
        from master_pipeline import validate_inputs
        
        # Should not raise any exceptions
        validate_inputs(self.input_csv, self.output_dir)

    def test_input_validation_file_not_exists(self):
        """
        Test input validation when input file does not exist.
        
        Method being tested: validate_inputs
        Scenario: Input file does not exist
        Expected behavior: FileNotFoundError is raised
        """
        from master_pipeline import validate_inputs
        
        nonexistent_file = self.test_dir / "nonexistent.csv"
        
        with self.assertRaises(FileNotFoundError):
            validate_inputs(nonexistent_file, self.output_dir)

    def test_input_validation_creates_output_dir(self):
        """
        Test input validation creates output directory if it doesn't exist.
        
        Method being tested: validate_inputs
        Scenario: Output directory does not exist
        Expected behavior: Directory is created
        """
        from master_pipeline import validate_inputs
        
        new_output_dir = self.test_dir / "new_output"
        self.assertFalse(new_output_dir.exists())
        
        validate_inputs(self.input_csv, new_output_dir)
        
        self.assertTrue(new_output_dir.exists())
        self.assertTrue(new_output_dir.is_dir())

    def test_load_config_valid_file(self):
        """
        Test loading configuration from a valid file.
        
        Method being tested: load_config
        Scenario: Valid config file
        Expected behavior: Configuration is loaded correctly
        """
        from master_pipeline import load_config
        
        config = load_config(self.config_file)
        
        self.assertEqual(config["log_level"], "INFO")
        self.assertEqual(config["crawl_settings"]["max_pages"], 10)
        self.assertEqual(config["llm_settings"]["temperature"], 0.7)

    def test_load_config_nonexistent_file(self):
        """
        Test loading configuration from a nonexistent file.
        
        Method being tested: load_config
        Scenario: Config file does not exist
        Expected behavior: FileNotFoundError is raised
        """
        from master_pipeline import load_config
        
        nonexistent_file = self.test_dir / "nonexistent_config.json"
        
        with self.assertRaises(FileNotFoundError):
            load_config(nonexistent_file)

    def test_load_config_invalid_json(self):
        """
        Test loading configuration from an invalid JSON file.
        
        Method being tested: load_config
        Scenario: Invalid JSON in config file
        Expected behavior: JSONDecodeError is raised
        """
        from master_pipeline import load_config
        
        invalid_config = self.test_dir / "invalid_config.json"
        with open(invalid_config, 'w') as f:
            f.write("This is not valid JSON")
        
        with self.assertRaises(json.JSONDecodeError):
            load_config(invalid_config)

    def test_merge_config_with_args(self):
        """
        Test merging configuration with command-line arguments.
        
        Method being tested: merge_config_with_args
        Scenario: Command-line args override config file
        Expected behavior: Args take precedence over config
        """
        from master_pipeline import merge_config_with_args
        
        config = {
            "default_output_dir": "/default/path",
            "category": "default_category",
            "log_level": "INFO"
        }
        
        args = MagicMock()
        args.output_dir = Path("/custom/output")
        args.category = "custom_category"
        args.verbose = True
        args.input_csv = Path("/path/to/input.csv")
        
        merged_config = merge_config_with_args(config, args)
        
        self.assertEqual(merged_config["output_dir"], "/custom/output")
        self.assertEqual(merged_config["category"], "custom_category")
        self.assertEqual(merged_config["log_level"], "DEBUG")  # Verbose should change log level
        self.assertEqual(merged_config["input_csv"], "/path/to/input.csv")

    def test_setup_logging(self):
        """
        Test setting up logging with the specified level.
        
        Method being tested: setup_logging
        Scenario: Setting up with DEBUG level
        Expected behavior: Logger is configured correctly
        """
        from master_pipeline import setup_logging
        
        with patch('logging.basicConfig') as mock_basic_config:
            logger = setup_logging("DEBUG", self.log_file)
            
            mock_basic_config.assert_called_once()
            self.assertEqual(logger.level, logging.DEBUG)

    @patch('master_pipeline.run_extracting_machine_pipeline')
    @patch('master_pipeline.run_webcrawl_pipeline')
    @patch('master_pipeline.run_integration_pipeline')
    def test_run_pipeline_calls_components(self, mock_integration, mock_webcrawl, mock_extracting):
        """
        Test that run_pipeline calls all component pipelines in the correct order.
        
        Method being tested: run_pipeline
        Scenario: All pipeline components should be executed
        Expected behavior: All components are called in sequence with correct arguments
        """
        from master_pipeline import run_pipeline
        
        # Set up mock returns
        mock_extracting.return_value = "/path/to/extracting_output.csv"
        mock_webcrawl.return_value = "/path/to/webcrawl_output.csv"
        mock_integration.return_value = "/path/to/final_output.csv"
        
        config = {
            "input_csv": str(self.input_csv),
            "output_dir": str(self.output_dir),
            "category": "maschinenbauer"
        }
        
        # Call the function being tested
        result = run_pipeline(config)
        
        # Verify all components were called in sequence
        mock_extracting.assert_called_once()
        mock_webcrawl.assert_called_once()
        mock_integration.assert_called_once()
        
        # Verify correct parameters were passed
        self.assertIn(str(self.input_csv), mock_extracting.call_args[0][0])
        self.assertIn(str(self.output_dir), mock_extracting.call_args[0][1])
        
        # Verify the result is the output of the integration pipeline
        self.assertEqual(result, "/path/to/final_output.csv")

    @patch('master_pipeline.run_extracting_machine_pipeline')
    def test_run_pipeline_handles_errors(self, mock_extracting):
        """
        Test that run_pipeline handles errors from component pipelines.
        
        Method being tested: run_pipeline
        Scenario: Component pipeline raises an exception
        Expected behavior: Exception is caught and logged
        """
        from master_pipeline import run_pipeline
        
        # Setup mock to raise an exception
        mock_extracting.side_effect = Exception("Pipeline error")
        
        config = {
            "input_csv": str(self.input_csv),
            "output_dir": str(self.output_dir)
        }
        
        with patch('master_pipeline.logger') as mock_logger:
            with self.assertRaises(Exception):
                run_pipeline(config)
            
            # Verify error was logged
            mock_logger.error.assert_called()

    def test_extracting_machine_pipeline(self):
        """
        Test the extracting machine pipeline component.
        
        Method being tested: run_extracting_machine_pipeline
        Scenario: Running with valid inputs
        Expected behavior: Calls correct functions in sequence
        """
        from master_pipeline import run_extracting_machine_pipeline
        
        # Mock all the extracting machine components
        with patch('master_pipeline.get_company_by_category') as mock_get_company, \
             patch('master_pipeline.get_bundesanzeiger_html') as mock_get_html, \
             patch('master_pipeline.clean_html') as mock_clean_html, \
             patch('master_pipeline.extract_sachanlagen') as mock_extract, \
             patch('master_pipeline.generate_csv_report') as mock_generate, \
             patch('master_pipeline.merge_csv_with_excel') as mock_merge:
            
            # Setup mock returns
            mock_get_company.return_value = "/path/to/filtered_companies.csv"
            mock_get_html.return_value = "/path/to/html_dir"
            mock_clean_html.return_value = "/path/to/cleaned_html_dir"
            mock_extract.return_value = "/path/to/extracted_dir"
            mock_generate.return_value = "/path/to/report.csv"
            mock_merge.return_value = "/path/to/merged.csv"
            
            # Call the function being tested
            result = run_extracting_machine_pipeline(
                str(self.input_csv),
                str(self.output_dir),
                category="maschinenbauer"
            )
            
            # Verify all components were called in sequence
            mock_get_company.assert_called_once()
            mock_get_html.assert_called_once()
            mock_clean_html.assert_called_once()
            mock_extract.assert_called_once()
            mock_generate.assert_called_once()
            mock_merge.assert_called_once()
            
            # Verify the result is the output of the merge
            self.assertEqual(result, "/path/to/merged.csv")

    def test_webcrawl_pipeline(self):
        """
        Test the webcrawl pipeline component.
        
        Method being tested: run_webcrawl_pipeline
        Scenario: Running with valid inputs
        Expected behavior: Calls correct functions in sequence
        """
        from master_pipeline import run_webcrawl_pipeline
        
        # Mock all the webcrawl components
        with patch('master_pipeline.crawl_domain') as mock_crawl, \
             patch('master_pipeline.extract_llm') as mock_extract, \
             patch('master_pipeline.pluralize_with_llm') as mock_pluralize, \
             patch('master_pipeline.consolidate') as mock_consolidate, \
             patch('master_pipeline.fill_process_type') as mock_fill, \
             patch('master_pipeline.convert_to_csv') as mock_convert:
            
            # Setup mock returns
            mock_crawl.return_value = "/path/to/crawled_dir"
            mock_extract.return_value = "/path/to/extracted_dir"
            mock_pluralize.return_value = "/path/to/pluralized_dir"
            mock_consolidate.return_value = "/path/to/consolidated.json"
            mock_fill.return_value = "/path/to/filled.json"
            mock_convert.return_value = "/path/to/converted.csv"
            
            # Call the function being tested
            result = run_webcrawl_pipeline(
                str(self.input_csv),
                str(self.output_dir)
            )
            
            # Verify all components were called in sequence
            mock_crawl.assert_called_once()
            mock_extract.assert_called_once()
            mock_pluralize.assert_called_once()
            mock_consolidate.assert_called_once()
            mock_fill.assert_called_once()
            mock_convert.assert_called_once()
            
            # Verify the result is the output of the convert
            self.assertEqual(result, "/path/to/converted.csv")

    def test_integration_pipeline(self):
        """
        Test the integration pipeline component.
        
        Method being tested: run_integration_pipeline
        Scenario: Running with valid inputs
        Expected behavior: Calls correct functions in sequence
        """
        from master_pipeline import run_integration_pipeline
        
        # Mock all the integration components
        with patch('master_pipeline.merge_technische_anlagen_with_keywords') as mock_merge, \
             patch('master_pipeline.enrich_data') as mock_enrich:
            
            # Setup mock returns
            mock_merge.return_value = "/path/to/merged.csv"
            mock_enrich.return_value = "/path/to/enriched.csv"
            
            # Call the function being tested
            result = run_integration_pipeline(
                "/path/to/extracting_output.csv",
                "/path/to/webcrawl_output.csv",
                str(self.output_dir)
            )
            
            # Verify all components were called in sequence
            mock_merge.assert_called_once()
            mock_enrich.assert_called_once()
            
            # Verify the result is the output of the enrich
            self.assertEqual(result, "/path/to/enriched.csv")

    def test_main_function(self):
        """
        Test the main function with all components mocked.
        
        Method being tested: main
        Scenario: Running the main function
        Expected behavior: Parses args, sets up logging, and runs pipeline
        """
        with patch('master_pipeline.parse_arguments') as mock_parse, \
             patch('master_pipeline.validate_inputs') as mock_validate, \
             patch('master_pipeline.load_config') as mock_load, \
             patch('master_pipeline.merge_config_with_args') as mock_merge, \
             patch('master_pipeline.setup_logging') as mock_setup, \
             patch('master_pipeline.run_pipeline') as mock_run:
            
            # Setup mock returns
            args = MagicMock()
            args.input_csv = Path(str(self.input_csv))
            args.output_dir = Path(str(self.output_dir))
            args.config_file = Path(str(self.config_file))
            args.category = "maschinenbauer"
            args.verbose = True
            
            mock_parse.return_value = args
            mock_load.return_value = {"default": "config"}
            mock_merge.return_value = {"merged": "config"}
            mock_setup.return_value = MagicMock()
            mock_run.return_value = "/path/to/final_output.csv"
            
            # Import and call the main function
            from master_pipeline import main
            
            # Call the function being tested
            main()
            
            # Verify all functions were called correctly
            mock_parse.assert_called_once()
            mock_validate.assert_called_once()
            mock_load.assert_called_once()
            mock_merge.assert_called_once()
            mock_setup.assert_called_once()
            mock_run.assert_called_once()


if __name__ == '__main__':
    unittest.main()
