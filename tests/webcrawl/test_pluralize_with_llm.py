#!/usr/bin/env python3
"""
Unit tests for the pluralize_with_llm module.

This module tests the functionality for pluralizing German words in JSON files
using a language model, with special handling for compound words.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import os
import logging
import io
import sys
import json
from pathlib import Path

from webcrawl.pluralize_with_llm import (
    process_directory, 
    process_json_file,
    process_file_or_directory,
    clean_compound_words,
    pluralize_with_llm,
    extract_fields_from_entry,
    update_entry_with_pluralized_fields,
    create_pluralization_prompt,
    validate_pluralized_response,
    failed_files, 
    compound_word_stats
)


class TestCleanCompoundWords(unittest.TestCase):
    """Test the compound word cleaning functionality."""
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the compound_word_stats before each test
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []
    
    def test_clean_und_conjunction(self):
        """Test words with 'und' conjunctions are properly cleaned."""
        # Test words with "und" conjunctions
        input_fields = {
            "products": ["Hammer und Meißel", "Schrauben und Muttern", "Normal"]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        # Check the cleaned words
        self.assertEqual(cleaned_fields["products"], ["Meißel", "Muttern", "Normal"])
        
        # Check the modified pairs
        self.assertEqual(len(modified_pairs["products"]), 2)
        self.assertEqual(modified_pairs["products"][0][0], "Hammer und Meißel")
        self.assertEqual(modified_pairs["products"][0][1], "Meißel")
        self.assertEqual(modified_pairs["products"][1][0], "Schrauben und Muttern")
        self.assertEqual(modified_pairs["products"][1][1], "Muttern")
    
    def test_clean_hyphenated_conjunction(self):
        """Test words with hyphenated forms and conjunctions are properly cleaned."""
        input_fields = {
            "machines": ["Saug- und Blasgeräte", "Bohr- und Fräswerkzeuge"]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        # Check the cleaned words
        self.assertEqual(cleaned_fields["machines"], ["Blasgeräte", "Fräswerkzeuge"])
        
        # Check the modified pairs
        self.assertEqual(len(modified_pairs["machines"]), 2)
        self.assertEqual(modified_pairs["machines"][0][0], "Saug- und Blasgeräte")
        self.assertEqual(modified_pairs["machines"][0][1], "Blasgeräte")
    
    def test_clean_comma_separated_values(self):
        """Test words with comma separations are properly split."""
        input_fields = {
            "process_type": ["Pumpen, Ventile, Schläuche", "Filter, Regler", "SingleWord"]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        # Check the cleaned words
        self.assertEqual(len(cleaned_fields["process_type"]), 6)  # 5 split items + 1 single word
        self.assertIn("Pumpen", cleaned_fields["process_type"])
        self.assertIn("Ventile", cleaned_fields["process_type"])
        self.assertIn("Schläuche", cleaned_fields["process_type"])
        self.assertIn("Filter", cleaned_fields["process_type"])
        self.assertIn("Regler", cleaned_fields["process_type"])
        self.assertIn("SingleWord", cleaned_fields["process_type"])
        
        # Check the modified pairs
        self.assertEqual(len(modified_pairs["process_type"]), 2)
        self.assertEqual(modified_pairs["process_type"][0][0], "Pumpen, Ventile, Schläuche")
        self.assertTrue("Split into 3 entries" in modified_pairs["process_type"][0][1])
        self.assertEqual(modified_pairs["process_type"][1][0], "Filter, Regler")
        self.assertTrue("Split into 2 entries" in modified_pairs["process_type"][1][1])
    
    def test_words_that_has_und_inside(self):
        """Test phrases with 'und' in various contexts are properly processed."""
        input_fields = {
            "process_type": [
                "Kundendienst und Wartung kryogener Medien", 
                "Installation und Beratung technischer Systeme",
                "Reinigung und Instandhaltung von Anlagen"
            ]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        # These phrases should be processed to extract the part after "und"
        self.assertEqual(len(cleaned_fields["process_type"]), 3)
        self.assertEqual(cleaned_fields["process_type"][0], "Wartung kryogener Medien")
        self.assertEqual(cleaned_fields["process_type"][1], "Beratung technischer Systeme")
        self.assertEqual(cleaned_fields["process_type"][2], "Instandhaltung von Anlagen")
        
        # Check that modifications were recorded
        self.assertEqual(len(modified_pairs["process_type"]), 3)
        self.assertEqual(modified_pairs["process_type"][0][0], "Kundendienst und Wartung kryogener Medien")
        self.assertEqual(modified_pairs["process_type"][0][1], "Wartung kryogener Medien")
    
    def test_mixed_separators(self):
        """Test words with mixed separators (commas and 'und') are properly processed."""
        input_fields = {
            "products": ["Schrauben, Muttern und Bolzen", "Metall- und Kunststoffteile, Gummiteile"]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
    
        # Check the resulting cleaned words
        self.assertEqual(len(cleaned_fields["products"]), 4)
        self.assertIn("Schrauben", cleaned_fields["products"])
        self.assertIn("Bolzen", cleaned_fields["products"])
        self.assertIn("Kunststoffteile", cleaned_fields["products"])
        self.assertIn("Gummiteile", cleaned_fields["products"])
        
        # Check that appropriate modifications were recorded
        self.assertTrue(len(modified_pairs["products"]) >= 3)
        
        # Check specific modifications
        has_comma_split = False
        has_und_processing = False
        
        for field_name, pairs in modified_pairs.items():
            for orig, modified in pairs:
                if orig == "Schrauben, Muttern und Bolzen" and "Split into" in modified:
                    has_comma_split = True
                if orig == "Muttern und Bolzen" and modified == "Bolzen":
                    has_und_processing = True
                
        self.assertTrue(has_comma_split, "Expected comma splitting of 'Schrauben, Muttern und Bolzen'")
        self.assertTrue(has_und_processing, "Expected processing of 'Muttern und Bolzen' to extract 'Bolzen'")
    
    def test_empty_input(self):
        """Test with empty input fields."""
        input_fields = {
            "products": [],
            "machines": [],
            "process_type": []
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        self.assertEqual(cleaned_fields, {"products": [], "machines": [], "process_type": []})
        self.assertEqual(modified_pairs, {})
    
    def test_no_modifications_needed(self):
        """Test with words that don't need modification."""
        input_fields = {
            "products": ["Schrauben", "Muttern", "Bolzen"]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        self.assertEqual(cleaned_fields, input_fields)
        self.assertEqual(modified_pairs, {})
        
    def test_und_followed_by_hyphen(self):
        """Test phrases with 'und' followed by a hyphen - these should be kept as-is."""
        input_fields = {
            "products": [
                "Leistungstransmissionstools und -händler", 
                "Reinigungsmittel und -geräte",
                "Herstellung und -verkauf von Metallprodukten"
            ]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)
        
        # These phrases should be kept as-is, not modified
        self.assertEqual(len(cleaned_fields["products"]), 3)
        self.assertEqual(cleaned_fields["products"][0], "Leistungstransmissionstools und -händler")
        self.assertEqual(cleaned_fields["products"][1], "Reinigungsmittel und -geräte")
        self.assertEqual(cleaned_fields["products"][2], "Herstellung und -verkauf von Metallprodukten")
        
        # Check that no modifications were recorded
        self.assertEqual(len(modified_pairs), 0)


class TestExtractAndUpdateFields(unittest.TestCase):
    """Test functionality to extract and update entry fields."""
    
    def test_extract_fields_from_entry(self):
        """Test extracting fields from a company entry."""
        entry = {
            "company_name": "Test Company",
            "products": ["Product1", "Product2"],
            "machines": ["Machine1"],
            "process_type": ["Process1", "Process2", "Process3"],
            "other_field": "value"
        }
        
        fields_dict = extract_fields_from_entry(entry)
        
        self.assertIn("products", fields_dict)
        self.assertIn("machines", fields_dict)
        self.assertIn("process_type", fields_dict)
        self.assertEqual(fields_dict["products"], ["Product1", "Product2"])
        self.assertEqual(fields_dict["machines"], ["Machine1"])
        self.assertEqual(fields_dict["process_type"], ["Process1", "Process2", "Process3"])
        self.assertNotIn("other_field", fields_dict)
        self.assertNotIn("company_name", fields_dict)
    
    def test_extract_fields_with_missing_fields(self):
        """Test extracting fields when some are missing."""
        entry = {
            "company_name": "Test Company",
            "products": ["Product1", "Product2"],
            # No machines field
            "process_type": ["Process1"]
        }
        
        fields_dict = extract_fields_from_entry(entry)
        
        self.assertIn("products", fields_dict)
        self.assertIn("process_type", fields_dict)
        self.assertNotIn("machines", fields_dict)
    
    def test_update_entry_with_pluralized_fields(self):
        """Test updating an entry with pluralized fields."""
        original_entry = {
            "company_name": "Test Company",
            "products": ["Product1", "Product2"],
            "machines": ["Machine1"],
            "process_type": ["Process1", "Process2"],
            "other_field": "value"
        }
        
        pluralized_fields = {
            "products": ["Pluralized1", "Pluralized2"],
            "machines": ["PluralizedMachine1"],
            "process_type": ["PluralizedProcess1", "PluralizedProcess2"]
        }
        
        updated_entry = update_entry_with_pluralized_fields(original_entry, pluralized_fields)
        
        # Check that fields were updated
        self.assertEqual(updated_entry["products"], ["Pluralized1", "Pluralized2"])
        self.assertEqual(updated_entry["machines"], ["PluralizedMachine1"])
        self.assertEqual(updated_entry["process_type"], ["PluralizedProcess1", "PluralizedProcess2"])
        
        # Check that other fields were preserved
        self.assertEqual(updated_entry["company_name"], "Test Company")
        self.assertEqual(updated_entry["other_field"], "value")


class TestPluralizeWithLLM(unittest.TestCase):
    """Test the LLM-powered pluralization functionality."""
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the failed_files list
        global failed_files
        failed_files.clear()
        
        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []
    
    def test_create_pluralization_prompt(self):
        """Test creating a proper prompt for the LLM."""
        fields_dict = {
            "products": ["Hammer", "Säge"],
            "machines": ["Bohrmaschine"],
            "process_type": ["Schweißen", "Fräsen"]
        }
        
        prompt = create_pluralization_prompt(fields_dict)
        
        # Check that the prompt contains instructional text
        self.assertIn("translate and pluralize the following", prompt.lower())
        self.assertIn("json object", prompt.lower())
        
        # Check that all fields are in the prompt
        self.assertIn('"products": [', prompt)
        self.assertIn('"machines": [', prompt)
        self.assertIn('"process_type": [', prompt)
        
        # Check that all items are in the prompt
        self.assertIn('"Hammer"', prompt)
        self.assertIn('"Säge"', prompt)
        self.assertIn('"Bohrmaschine"', prompt)
        self.assertIn('"Schweißen"', prompt)
        self.assertIn('"Fräsen"', prompt)
    
    def test_validate_pluralized_response_valid(self):
        """Test validating a correct LLM response."""
        input_fields = {
            "products": ["Hammer", "Säge"],
            "process_type": ["Schweißen"]
        }
        
        output_fields = {
            "products": ["Hämmer", "Sägen"],
            "process_type": ["Schweißarbeiten"]
        }
        
        is_valid, error_msg = validate_pluralized_response(input_fields, output_fields)
        
        self.assertTrue(is_valid)
        self.assertEqual(error_msg, "")
    
    def test_validate_pluralized_response_invalid_missing_field(self):
        """Test validating an LLM response with a missing field."""
        input_fields = {
            "products": ["Hammer", "Säge"],
            "process_type": ["Schweißen"]
        }
        
        output_fields = {
            "products": ["Hämmer", "Sägen"]
            # Missing process_type
        }
        
        is_valid, error_msg = validate_pluralized_response(input_fields, output_fields)
        
        self.assertFalse(is_valid)
        self.assertIn("Missing field", error_msg)
        self.assertIn("process_type", error_msg)
    
    def test_validate_pluralized_response_invalid_word_count(self):
        """Test validating an LLM response with incorrect word count."""
        input_fields = {
            "products": ["Hammer", "Säge"],
            "process_type": ["Schweißen"]
        }
        
        output_fields = {
            "products": ["Hämmer", "Sägen", "Extra word"],  # Extra word
            "process_type": ["Schweißarbeiten"]
        }
        
        is_valid, error_msg = validate_pluralized_response(input_fields, output_fields)
        
        self.assertFalse(is_valid)
        self.assertIn("has 3 words but expected 2", error_msg)
    
    @patch('webcrawl.pluralize_with_llm.completion')
    def test_pluralize_with_llm_successful(self, mock_completion):
        """Test successful pluralization with LLM."""
        # Mock the LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["Pluralized1", "Pluralized2"],
            "machines": ["PluralizedMachine"],
            "process_type": ["PluralizedProcess"]
        })
        mock_completion.return_value = mock_response
        
        # Input fields
        fields_dict = {
            "products": ["Original1", "Original2"],
            "machines": ["OriginalMachine"],
            "process_type": ["OriginalProcess"]
        }
        
        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json")
        
        # Verify result
        self.assertEqual(result["products"], ["Pluralized1", "Pluralized2"])
        self.assertEqual(result["machines"], ["PluralizedMachine"])
        self.assertEqual(result["process_type"], ["PluralizedProcess"])
        
        # Check that completion was called once with expected parameters
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args
        self.assertEqual(kwargs["model"], "gpt-4o-mini")
        self.assertIn("messages", kwargs)
        self.assertEqual(kwargs["temperature"], 0.5)  # First temperature value
    
    @patch('webcrawl.pluralize_with_llm.completion')
    def test_pluralize_with_custom_temperatures(self, mock_completion):
        """Test pluralization with custom temperature values."""
        # Mock the LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["Pluralized1", "Pluralized2"]
        })
        mock_completion.return_value = mock_response
        
        # Input fields
        fields_dict = {
            "products": ["Original1", "Original2"]
        }
        
        # Custom temperatures
        temperatures = [0.2, 0.8]
        
        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json", temperatures)
        
        # Check that completion was called with the first custom temperature
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args
        self.assertEqual(kwargs["temperature"], 0.2)
    
    @patch('webcrawl.pluralize_with_llm.completion', side_effect=Exception("API error"))
    def test_pluralize_with_llm_exception(self, mock_completion):
        """Test handling of exceptions during LLM pluralization."""
        fields_dict = {
            "products": ["Original1", "Original2"]
        }
        
        # Should return original fields on error
        result = pluralize_with_llm(fields_dict, "test_file.json")
        self.assertEqual(result, fields_dict)
        
        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")
    
    @patch('webcrawl.pluralize_with_llm.completion')
    def test_pluralize_with_llm_validation_failure(self, mock_completion):
        """Test handling of validation failures in LLM responses."""
        # Mock the LLM response with incorrect word count
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["Single"]  # Should be 2 words
        })
        mock_completion.return_value = mock_response
        
        # Input fields
        fields_dict = {
            "products": ["Original1", "Original2"]
        }
        
        # Use only one temperature to ensure we hit max retries
        temperatures = [0.5]
        
        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json", temperatures)
        
        # Should return original fields after failing validation
        self.assertEqual(result, fields_dict)
        
        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")


class TestProcessJsonFile(unittest.TestCase):
    """Test processing entire JSON files."""
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the failed_files list
        global failed_files
        failed_files.clear()
        
        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []
    
    @patch('webcrawl.pluralize_with_llm.pluralize_with_llm')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, 
           read_data='[{"company_name": "Test", "products": ["Product1", "Product2"]}]')
    @patch('os.makedirs')
    def test_process_json_file_success(self, mock_makedirs, mock_open, mock_pluralize):
        """Test successfully processing a JSON file."""
        # Mock successful pluralization
        mock_pluralize.return_value = {"products": ["Pluralized1", "Pluralized2"]}
        
        # Call the function
        process_json_file("input.json", "output.json")
        
        # Check that open was called for reading and writing
        mock_open.assert_any_call("input.json", 'r', encoding='utf-8')
        mock_open.assert_any_call("output.json", 'w', encoding='utf-8')
        
        # Check that makedirs was called
        mock_makedirs.assert_called_once()
        
        # Check that pluralize_with_llm was called with correct parameters
        mock_pluralize.assert_called_once()
        args, kwargs = mock_pluralize.call_args
        self.assertEqual(args[0], {"products": ["Product1", "Product2"]})
        self.assertEqual(args[1], "input.json")
    
    @patch('builtins.open', side_effect=Exception("File error"))
    def test_process_json_file_file_error(self, mock_open):
        """Test handling of file errors during JSON processing."""
        process_json_file("input.json", "output.json")
        
        # Check that the file was recorded as failed
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "input.json")
        self.assertEqual(failed_files[0][1], "file_processing_error")
    
    @patch('builtins.open', new_callable=unittest.mock.mock_open, 
           read_data='{"not_an_array": true}')  # Not a JSON array
    def test_process_json_file_invalid_structure(self, mock_open):
        """Test handling of invalid JSON structure."""
        process_json_file("input.json", "output.json")
        
        # Check that the file was recorded as failed
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "input.json")
        self.assertEqual(failed_files[0][1], "invalid_json_structure")


class TestProcessDirectory(unittest.TestCase):
    """Test processing directories of JSON files."""
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the failed_files list
        global failed_files
        failed_files.clear()
        
        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []
        
        # Set up logging to capture log messages
        self.log_output = io.StringIO()
        self.log_handler = logging.StreamHandler(self.log_output)
        logging.getLogger().addHandler(self.log_handler)
        logging.getLogger().setLevel(logging.INFO)
    
    def tearDown(self):
        """Clean up after each test."""
        # Clean up logging
        logging.getLogger().removeHandler(self.log_handler)
        self.log_output.close()
    
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_no_json_files_found(self, mock_listdir, mock_makedirs):
        """Test behavior when no JSON files are found."""
        # Setup
        mock_listdir.return_value = ['file1.txt', 'file2.csv']  # No JSON files
        
        # Execute
        process_directory('input_dir', 'output_dir', temperatures=[0.5, 0.7])
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertIn("No JSON files found in input_dir", self.log_output.getvalue())
    
    @patch('webcrawl.pluralize_with_llm.process_json_file')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_successful_processing(self, mock_listdir, mock_makedirs, mock_process_json_file):
        """Test successful processing of multiple files."""
        # Setup
        mock_listdir.return_value = ['file1.json', 'file2.json', 'file3.txt']
        temperatures = [0.5, 0.7]
        
        # Execute
        process_directory('input_dir', 'output_dir', temperatures)
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertEqual(mock_process_json_file.call_count, 2)  # Should be called twice for 2 JSON files
        
        # Check that process_json_file was called with the correct parameters
        expected_calls = [
            call(os.path.join('input_dir', 'file1.json'), 
                 os.path.join('output_dir', 'file1.json'), 
                 temperatures),
            call(os.path.join('input_dir', 'file2.json'), 
                 os.path.join('output_dir', 'file2.json'), 
                 temperatures)
        ]
        mock_process_json_file.assert_has_calls(expected_calls, any_order=False)
        
        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 2 JSON files to process", log_output)
    
    @patch('webcrawl.pluralize_with_llm.process_json_file')
    @patch('os.makedirs')
    @patch('os.listdir')
    def test_processing_with_failures(self, mock_listdir, mock_makedirs, mock_process_json_file):
        """Test processing with some failures."""
        # Setup
        global failed_files
        mock_listdir.return_value = ['file1.json', 'file2.json', 'file3.json']
        
        # Add some failed files to simulate failures during processing
        failed_files.append((os.path.join('input_dir', 'file1.json'), 'products'))
        failed_files.append((os.path.join('input_dir', 'file2.json'), 'machines'))
        
        # Execute
        process_directory('input_dir', 'output_dir')
        
        # Verify
        mock_makedirs.assert_called_once_with('output_dir', exist_ok=True)
        self.assertEqual(mock_process_json_file.call_count, 3)  # Should be called for all 3 JSON files
        
        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 3 JSON files to process", log_output)
        self.assertIn("FAILURE SUMMARY", log_output)
        self.assertIn("Total files with failures: 2", log_output)
    
    @patch('os.makedirs', side_effect=Exception("Directory creation failed"))
    @patch('os.listdir')
    def test_handles_makedirs_exception(self, mock_listdir, mock_makedirs):
        """Test handling of exceptions during directory creation."""
        # Execute
        process_directory('input_dir', 'output_dir')
        
        # Verify process_json_file was not called
        self.assertIn("Error", self.log_output.getvalue())


class TestIntegrationWithSampleData(unittest.TestCase):
    """Integration tests using the sample data."""
    
    def setUp(self):
        """Set up the test environment before each test."""
        # Path to the sample data file
        self.sample_file_path = str(Path(__file__).parent / 'data' / 'tubetech.de_extracted.json')
        self.output_file_path = str(Path(__file__).parent / 'data' / 'tubetech.de_pluralized.json')
        
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.sample_file_path), exist_ok=True)
        
        # Sample data matching the provided example
        self.sample_data = [
            {
                "company_name": "TubeTech GmbH",
                "company_url": "https://tubetech.de",
                "products": [
                    "Luftkühler",
                    "Luftvorwärmer",
                    "Ersatzwärmetauscher",
                    "Rippenrohre"
                ],
                "machines": [],
                "process_type": [
                    "Schweißarbeiten",
                    "Fräsungen",
                    "Bohrungen",
                    "Montagearbeiten",
                    "Qualitätssicherungen"
                ],
                "lohnfertigung": False,
                "error": False
            }
        ]
        
        # Create the sample file if it doesn't exist
        if not os.path.exists(self.sample_file_path):
            os.makedirs(os.path.dirname(self.sample_file_path), exist_ok=True)
            with open(self.sample_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.sample_data, f, ensure_ascii=False, indent=4)
        
        # Clear the failed_files list
        global failed_files
        failed_files.clear()
        
        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []
    
    def tearDown(self):
        """Clean up after each test."""
        # Remove the output file if it exists
        if os.path.exists(self.output_file_path):
            os.remove(self.output_file_path)
    
    @patch('webcrawl.pluralize_with_llm.pluralize_with_llm')
    def test_process_sample_file(self, mock_pluralize):
        """Test processing the sample file."""
        # Mock the pluralization to return expected values
        mock_pluralize.return_value = {
            "products": [
                "Luftkühler",
                "Luftvorwärmer",
                "Ersatzwärmetauscher",
                "Rippenrohre"
            ],
            "process_type": [
                "Schweißarbeiten",
                "Fräsungen",
                "Bohrungen",
                "Montagearbeiten",
                "Qualitätssicherungen"
            ]
        }
        
        # Process the sample file
        process_json_file(self.sample_file_path, self.output_file_path)
        
        # Check that pluralize_with_llm was called with the correct fields
        mock_pluralize.assert_called_once()
        args, kwargs = mock_pluralize.call_args
        
        # Check the extracted fields match our sample
        self.assertEqual(len(args[0]["products"]), 4)
        self.assertEqual(len(args[0]["process_type"]), 5)
        self.assertNotIn("machines", args[0])  # Empty list, should be ignored
        
        # Check the file path was passed correctly
        self.assertEqual(args[1], self.sample_file_path)


if __name__ == '__main__':
    unittest.main()
