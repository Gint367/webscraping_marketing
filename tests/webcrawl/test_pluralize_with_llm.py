#!/usr/bin/env python3
"""
Unit tests for the pluralize_with_llm module.

This module tests the functionality for pluralizing German words in JSON files
using a language model, with special handling for compound words.
"""

import io
import json
import logging
import os
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, call, patch

from litellm.exceptions import JSONSchemaValidationError

from webcrawl.pluralize_with_llm import (
    clean_compound_words,
    compound_word_stats,
    create_pluralization_prompt,
    extract_fields_from_entry,
    failed_files,
    pluralize_with_llm,
    process_directory,
    process_json_file,
    update_entry_with_pluralized_fields,
    validate_pluralized_response,
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
        input_fields = {"machines": ["Saug- und Blasgeräte", "Bohr- und Fräswerkzeuge"]}
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
            "process_type": [
                "Pumpen, Ventile, Schläuche",
                "Filter, Regler",
                "SingleWord",
            ]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)

        # Check the cleaned words
        self.assertEqual(
            len(cleaned_fields["process_type"]), 6
        )  # 5 split items + 1 single word
        self.assertIn("Pumpen", cleaned_fields["process_type"])
        self.assertIn("Ventile", cleaned_fields["process_type"])
        self.assertIn("Schläuche", cleaned_fields["process_type"])
        self.assertIn("Filter", cleaned_fields["process_type"])
        self.assertIn("Regler", cleaned_fields["process_type"])
        self.assertIn("SingleWord", cleaned_fields["process_type"])

        # Check the modified pairs
        self.assertEqual(len(modified_pairs["process_type"]), 2)
        self.assertEqual(
            modified_pairs["process_type"][0][0], "Pumpen, Ventile, Schläuche"
        )
        self.assertTrue("Split into 3 entries" in modified_pairs["process_type"][0][1])
        self.assertEqual(modified_pairs["process_type"][1][0], "Filter, Regler")
        self.assertTrue("Split into 2 entries" in modified_pairs["process_type"][1][1])

    def test_words_that_has_und_inside(self):
        """Test phrases with 'und' in various contexts are properly processed."""
        input_fields = {
            "process_type": [
                "Kundendienst und Wartung kryogener Medien",
                "Installation und Beratung technischer Systeme",
                "Reinigung und Instandhaltung von Anlagen",
            ]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)

        # These phrases should be processed to extract the part after "und"
        self.assertEqual(len(cleaned_fields["process_type"]), 3)
        self.assertEqual(cleaned_fields["process_type"][0], "Wartung kryogener Medien")
        self.assertEqual(
            cleaned_fields["process_type"][1], "Beratung technischer Systeme"
        )
        self.assertEqual(
            cleaned_fields["process_type"][2], "Instandhaltung von Anlagen"
        )

        # Check that modifications were recorded
        self.assertEqual(len(modified_pairs["process_type"]), 3)
        self.assertEqual(
            modified_pairs["process_type"][0][0],
            "Kundendienst und Wartung kryogener Medien",
        )
        self.assertEqual(
            modified_pairs["process_type"][0][1], "Wartung kryogener Medien"
        )

    def test_mixed_separators(self):
        """Test words with mixed separators (commas and 'und') are properly processed."""
        input_fields = {
            "products": [
                "Schrauben, Muttern und Bolzen",
                "Metall- und Kunststoffteile, Gummiteile",
            ]
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

        self.assertTrue(
            has_comma_split,
            "Expected comma splitting of 'Schrauben, Muttern und Bolzen'",
        )
        self.assertTrue(
            has_und_processing,
            "Expected processing of 'Muttern und Bolzen' to extract 'Bolzen'",
        )

    def test_empty_input(self):
        """Test with empty input fields."""
        input_fields = {"products": [], "machines": [], "process_type": []}
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)

        self.assertEqual(
            cleaned_fields, {"products": [], "machines": [], "process_type": []}
        )
        self.assertEqual(modified_pairs, {})

    def test_no_modifications_needed(self):
        """Test with words that don't need modification."""
        input_fields = {"products": ["Schrauben", "Muttern", "Bolzen"]}
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)

        self.assertEqual(cleaned_fields, input_fields)
        self.assertEqual(modified_pairs, {})

    def test_und_followed_by_hyphen(self):
        """Test phrases with 'und' followed by a hyphen - these should be kept as-is."""
        input_fields = {
            "products": [
                "Leistungstransmissionstools und -händler",
                "Reinigungsmittel und -geräte",
                "Herstellung und -verkauf von Metallprodukten",
            ]
        }
        cleaned_fields, modified_pairs = clean_compound_words(input_fields)

        # These phrases should be kept as-is, not modified
        self.assertEqual(len(cleaned_fields["products"]), 3)
        self.assertEqual(
            cleaned_fields["products"][0], "Leistungstransmissionstools und -händler"
        )
        self.assertEqual(cleaned_fields["products"][1], "Reinigungsmittel und -geräte")
        self.assertEqual(
            cleaned_fields["products"][2],
            "Herstellung und -verkauf von Metallprodukten",
        )

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
            "other_field": "value",
        }

        fields_dict = extract_fields_from_entry(entry)

        self.assertIn("products", fields_dict)
        self.assertIn("machines", fields_dict)
        self.assertIn("process_type", fields_dict)
        self.assertEqual(fields_dict["products"], ["Product1", "Product2"])
        self.assertEqual(fields_dict["machines"], ["Machine1"])
        self.assertEqual(
            fields_dict["process_type"], ["Process1", "Process2", "Process3"]
        )
        self.assertNotIn("other_field", fields_dict)
        self.assertNotIn("company_name", fields_dict)

    def test_extract_fields_with_missing_fields(self):
        """Test extracting fields when some are missing."""
        entry = {
            "company_name": "Test Company",
            "products": ["Product1", "Product2"],
            # No machines field
            "process_type": ["Process1"],
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
            "other_field": "value",
        }

        pluralized_fields = {
            "products": ["Pluralized1", "Pluralized2"],
            "machines": ["PluralizedMachine1"],
            "process_type": ["PluralizedProcess1", "PluralizedProcess2"],
        }

        updated_entry = update_entry_with_pluralized_fields(
            original_entry, pluralized_fields
        )

        # Check that fields were updated
        self.assertEqual(updated_entry["products"], ["Pluralized1", "Pluralized2"])
        self.assertEqual(updated_entry["machines"], ["PluralizedMachine1"])
        self.assertEqual(
            updated_entry["process_type"], ["PluralizedProcess1", "PluralizedProcess2"]
        )

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
            "process_type": ["Schweißen", "Fräsen"],
        }

        prompt = create_pluralization_prompt(fields_dict)

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
        input_fields = {"products": ["Hammer", "Säge"], "process_type": ["Schweißen"]}

        output_fields = {
            "products": ["Hämmer", "Sägen"],
            "process_type": ["Schweißarbeiten"],
        }

        is_valid, error_msg = validate_pluralized_response(input_fields, output_fields)

        self.assertTrue(is_valid)
        self.assertEqual(error_msg, "")

    def test_validate_pluralized_response_invalid_missing_field(self):
        """Test validating an LLM response with a missing field."""
        input_fields = {"products": ["Hammer", "Säge"], "process_type": ["Schweißen"]}

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
        input_fields = {"products": ["Hammer", "Säge"], "process_type": ["Schweißen"]}

        output_fields = {
            "products": ["Hämmer", "Sägen", "Extra word"],  # Extra word
            "process_type": ["Schweißarbeiten"],
        }

        is_valid, error_msg = validate_pluralized_response(input_fields, output_fields)

        self.assertFalse(is_valid)
        self.assertIn("has 3 words but expected 2", error_msg)

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_successful(self, mock_completion):
        """Test successful pluralization with LLM."""
        # Mock the LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(
            {
                "products": ["Pluralized1", "Pluralized2"],
                "machines": ["PluralizedMachine"],
                "process_type": ["PluralizedProcess"],
            }
        )
        mock_completion.return_value = mock_response

        # Input fields
        fields_dict = {
            "products": ["Original1", "Original2"],
            "machines": ["OriginalMachine"],
            "process_type": ["OriginalProcess"],
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
        self.assertIn("messages", kwargs)


    @patch("webcrawl.pluralize_with_llm.completion", side_effect=Exception("API error"))
    def test_pluralize_with_llm_exception(self, mock_completion):
        """Test handling of exceptions during LLM pluralization."""
        fields_dict = {"products": ["Original1", "Original2"]}

        # Should return original fields on error
        result = pluralize_with_llm(fields_dict, "test_file.json")
        self.assertEqual(result, fields_dict)

        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_validation_failure(self, mock_completion):
        """Test handling of validation failures in LLM responses."""
        # Mock the LLM response with incorrect word count
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(
            {
                "products": ["Single"]  # Should be 2 words
            }
        )
        mock_completion.return_value = mock_response

        # Input fields
        fields_dict = {"products": ["Original1", "Original2"]}

        # Use only one temperature to ensure we hit max retries
        temperatures = [0.5]

        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json", temperatures)

        # Should return original fields after failing validation
        self.assertEqual(result, fields_dict)

        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")


class TestModelFallbackFunctionality(unittest.TestCase):
    """Test the new model fallback functionality in pluralize_with_llm."""

    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the failed_files list
        global failed_files
        failed_files.clear()

        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_withCustomModels_usesProvidedModels(self, mock_completion):
        """Test that custom models parameter is correctly used in the function call."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["PluralizedProduct1", "PluralizedProduct2"],
            "machines": ["PluralizedMachine"],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        # Custom models list
        custom_models = [
            "bedrock/us.amazon.nova-lite-v1:0",
            "bedrock/us.amazon.nova-micro-v1:0"
        ]

        fields_dict = {
            "products": ["Product1", "Product2"],
            "machines": ["Machine1"],
            "process_type": []
        }

        # Call the function with custom models
        result = pluralize_with_llm(fields_dict, "test_file.json", models=custom_models)

        # Verify that completion was called with correct models
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that the primary model and fallbacks are set correctly
        self.assertEqual(kwargs["model"], "bedrock/us.amazon.nova-lite-v1:0")
        self.assertEqual(kwargs["fallbacks"], ["bedrock/us.amazon.nova-micro-v1:0"])

        # Verify other parameters
        self.assertEqual(kwargs["temperature"], 0.3)
        self.assertEqual(kwargs["max_tokens"], 1000)
        self.assertEqual(kwargs["num_retries"], 2)
        self.assertEqual(kwargs["timeout"], 45)

        # Verify result
        self.assertEqual(result["products"], ["PluralizedProduct1", "PluralizedProduct2"])
        self.assertEqual(result["machines"], ["PluralizedMachine"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_withDefaultModels_usesDefaultModels(self, mock_completion):
        """Test that default models are used when no models parameter is provided."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["DefaultPluralized1"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["DefaultOriginal1"]}

        # Call the function without custom models
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Verify that completion was called with default models
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that the default primary model is used
        self.assertEqual(kwargs["model"], "bedrock/amazon.nova-pro-v1:0")
        # Check that default fallback is used
        self.assertEqual(kwargs["fallbacks"], ["bedrock/us.amazon.nova-lite-v1:0"])

        # Verify result
        self.assertEqual(result["products"], ["DefaultPluralized1"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_withSingleModel_noFallbacks(self, mock_completion):
        """Test behavior when only one model is provided (no fallbacks)."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["SingleModelResult"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        # Single model list
        single_model = ["bedrock/us.amazon.nova-micro-v1:0"]
        fields_dict = {"products": ["SingleInput"]}

        # Call the function with single model
        result = pluralize_with_llm(fields_dict, "test_file.json", models=single_model)

        # Verify that completion was called with correct parameters
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that the single model is used as primary
        self.assertEqual(kwargs["model"], "bedrock/us.amazon.nova-micro-v1:0")
        # Check that no fallbacks are set
        self.assertEqual(kwargs["fallbacks"], [])

        # Verify result
        self.assertEqual(result["products"], ["SingleModelResult"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_withEmptyModels_usesDefaults(self, mock_completion):
        """Test that default models are used when empty models list is provided."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["EmptyListResult"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["EmptyListInput"]}

        # Call the function with empty models list
        result = pluralize_with_llm(fields_dict, "test_file.json", models=[])

        # Verify that completion was called with fallback to default
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that the default model is used when empty list provided
        self.assertEqual(kwargs["model"], "bedrock/amazon.nova-pro-v1:0")

        # Verify result
        self.assertEqual(result["products"], ["EmptyListResult"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_temperaturesParameter_isIgnored(self, mock_completion):
        """Test that the deprecated temperatures parameter is ignored."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["IgnoredTempResult"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["TempInput"]}
        deprecated_temps = [0.1, 0.5, 0.9]  # Should be ignored

        # Call the function with deprecated temperatures parameter
        result = pluralize_with_llm(fields_dict, "test_file.json", temperatures=deprecated_temps)

        # Verify that completion was called with fixed temperature
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that fixed temperature is used (not from deprecated parameter)
        self.assertEqual(kwargs["temperature"], 0.3)

        # Verify result
        self.assertEqual(result["products"], ["IgnoredTempResult"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_litellmSchemaValidation_enabled(self, mock_completion):
        """Test that LiteLLM JSON schema validation is enabled."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["SchemaValidatedResult"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["SchemaInput"]}

        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Verify that completion was called with schema validation parameters
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args

        # Check that response_format is set to PluralizedFields for schema validation
        from webcrawl.pluralize_with_llm import PluralizedFields
        self.assertEqual(kwargs["response_format"], PluralizedFields)

        # Verify that litellm.enable_json_schema_validation was set to True
        # (This is checked implicitly by the function calling completion with response_format)

        # Verify result
        self.assertEqual(result["products"], ["SchemaValidatedResult"])

    @patch("webcrawl.pluralize_with_llm.completion", side_effect=JSONSchemaValidationError(
        model="bedrock/amazon.nova-pro-v1:0",
        llm_provider="bedrock",
        raw_response='{"invalid": "schema"}',
        schema='{"type": "object"}'
    ))
    def test_pluralize_with_llm_jsonSchemaValidationError_handledGracefully(self, mock_completion):
        """Test handling of JSONSchemaValidationError from LiteLLM."""
        fields_dict = {"products": ["SchemaErrorInput"]}

        # Call the function - should handle the schema validation error
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return original cleaned fields on schema validation error
        self.assertEqual(result, fields_dict)

        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")
        self.assertEqual(failed_files[0][1], "json_schema_validation_error")

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_responseContentNone_handledGracefully(self, mock_completion):
        """Test handling when LLM response content is None."""
        # Mock response with None content
        mock_response = MagicMock()
        mock_response.choices[0].message.content = None
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["NoneContentInput"]}

        # Call the function - should handle None content
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return original cleaned fields on None content
        self.assertEqual(result, fields_dict)

        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_jsonDecodeError_handledGracefully(self, mock_completion):
        """Test handling of JSON decode errors from malformed LLM responses."""
        # Mock response with invalid JSON
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "This is not valid JSON"
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["JsonErrorInput"]}

        # Call the function - should handle JSON decode error
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return original cleaned fields on JSON decode error
        self.assertEqual(result, fields_dict)

        # Should record the failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")
        self.assertEqual(failed_files[0][1], "json_decode_error")

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_validationFailure_returnsOriginalFields(self, mock_completion):
        """Test that validation failures return original fields and log appropriately."""
        # Mock response with validation failure (wrong word count)
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["OnlyOne"],  # Should be 2 words
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["Input1", "Input2"]}

        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return original fields on validation failure
        self.assertEqual(result, fields_dict)

        # Should record the validation failure
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "test_file.json")
        self.assertEqual(failed_files[0][1], "validation_error")

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_successfulResponse_returnsCorrectFields(self, mock_completion):
        """Test that successful responses return correctly processed fields."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["Hämmer", "Sägen"],
            "machines": ["Bohrmaschinen"],
            "process_type": ["Schweißarbeiten", "Fräsarbeiten"]
        })
        mock_completion.return_value = mock_response

        fields_dict = {
            "products": ["Hammer", "Säge"],
            "machines": ["Bohrmaschine"],
            "process_type": ["Schweißarbeit", "Fräsarbeit"]
        }

        # Call the function
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return pluralized fields
        self.assertEqual(result["products"], ["Hämmer", "Sägen"])
        self.assertEqual(result["machines"], ["Bohrmaschinen"])
        self.assertEqual(result["process_type"], ["Schweißarbeiten", "Fräsarbeiten"])

        # Should not record any failures
        self.assertEqual(len(failed_files), 0)

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_pluralize_with_llm_emptyFields_skipsProcessing(self, mock_completion):
        """Test that empty fields are handled without calling LLM."""
        fields_dict = {"products": [], "machines": [], "process_type": []}

        # Call the function with empty fields
        result = pluralize_with_llm(fields_dict, "test_file.json")

        # Should return the same empty fields without calling LLM
        self.assertEqual(result, fields_dict)

        # Should not call completion at all
        mock_completion.assert_not_called()

        # Should not record any failures
        self.assertEqual(len(failed_files), 0)


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

    @patch("webcrawl.pluralize_with_llm.pluralize_with_llm")
    @patch(
        "builtins.open",
        new_callable=mock.mock_open,
        read_data='[{"company_name": "Test", "products": ["Product1", "Product2"]}]',
    )
    @patch("os.makedirs")
    def test_process_json_file_success(self, mock_makedirs, mock_open, mock_pluralize):
        """Test successfully processing a JSON file."""
        # Mock successful pluralization
        mock_pluralize.return_value = {"products": ["Pluralized1", "Pluralized2"]}

        # Call the function
        process_json_file("input.json", "output.json")

        # Check that open was called for reading and writing
        mock_open.assert_any_call("input.json", "r", encoding="utf-8")
        mock_open.assert_any_call("output.json", "w", encoding="utf-8")

        # Check that makedirs was called
        mock_makedirs.assert_called_once()

        # Check that pluralize_with_llm was called with correct parameters
        mock_pluralize.assert_called_once()
        args, kwargs = mock_pluralize.call_args
        self.assertEqual(args[0], {"products": ["Product1", "Product2"]})
        self.assertEqual(args[1], "input.json")
        self.assertEqual(args[1], "input.json")

    @patch("builtins.open", side_effect=Exception("File error"))
    def test_process_json_file_file_error(self, mock_open):
        """Test handling of file errors during JSON processing."""
        # Expect a ValueError to be raised with the proper message
        with self.assertRaises(ValueError) as context:
            process_json_file("input.json", "output.json")

        # Verify the error message
        self.assertEqual(str(context.exception), "Malformed JSON in file: input.json")

        # Check that the file was recorded as failed
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], "input.json")
        self.assertEqual(failed_files[0][1], "file_processing_error")

    @patch(
        "builtins.open", new_callable=mock.mock_open, read_data='{"not_an_array": true}'
    )  # Not a JSON array
    def test_process_json_file_invalid_structure(self, mock_open):
        """Test handling of invalid JSON structure."""
        # Expect a ValueError to be raised with the proper message
        with self.assertRaises(ValueError) as context:
            process_json_file("input.json", "output.json")

        # Verify the error message
        self.assertEqual(
            str(context.exception), "Invalid JSON structure in file: input.json"
        )

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

    @patch("os.makedirs")
    @patch("os.listdir")
    def test_no_json_files_found(self, mock_listdir, mock_makedirs):
        """Test behavior when no JSON files are found."""
        # Setup
        mock_listdir.return_value = ["file1.txt", "file2.csv"]  # No JSON files

        # Execute and verify that FileNotFoundError is raised
        with self.assertRaises(FileNotFoundError):
            process_directory("input_dir", "output_dir", temperatures=[0.5, 0.7])

    @patch("webcrawl.pluralize_with_llm.process_json_file")
    @patch("os.makedirs")
    @patch("os.listdir")
    @patch("os.path.isdir", return_value=True)  # Mock isdir to return True
    @patch(
        "os.environ.get", return_value="True"
    )  # Mock PYTEST_CURRENT_TEST environment variable
    def test_successful_processing(
        self,
        mock_environ_get,
        mock_isdir,
        mock_listdir,
        mock_makedirs,
        mock_process_json_file,
    ):
        """Test successful processing of multiple files."""
        # Setup
        mock_listdir.return_value = ["file1.json", "file2.json", "file3.txt"]
        temperatures = [0.5, 0.7]

        # Execute
        process_directory("input_dir", "output_dir", temperatures)

        # Verify
        mock_makedirs.assert_called_once_with("output_dir", exist_ok=True)
        self.assertEqual(
            mock_process_json_file.call_count, 2
        )  # Should be called twice for 2 JSON files

        # Check that process_json_file was called with the correct parameters
        expected_calls = [
            call(
                os.path.join("input_dir", "file1.json"),
                os.path.join("output_dir", "file1.json"),
                temperatures,
            ),
            call(
                os.path.join("input_dir", "file2.json"),
                os.path.join("output_dir", "file2.json"),
                temperatures,
            ),
        ]
        mock_process_json_file.assert_has_calls(expected_calls, any_order=False)

        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 2 JSON files to process", log_output)

    @patch("webcrawl.pluralize_with_llm.process_json_file")
    @patch("os.makedirs")
    @patch("os.listdir")
    @patch("os.path.isdir", return_value=True)  # Mock isdir to return True
    @patch(
        "os.environ.get", return_value="True"
    )  # Mock PYTEST_CURRENT_TEST environment variable
    def test_processing_with_failures(
        self,
        mock_environ_get,
        mock_isdir,
        mock_listdir,
        mock_makedirs,
        mock_process_json_file,
    ):
        """Test processing with some failures."""
        # Setup
        global failed_files
        mock_listdir.return_value = ["file1.json", "file2.json", "file3.json"]

        # Add some failed files to simulate failures during processing
        failed_files.append((os.path.join("input_dir", "file1.json"), "products"))
        failed_files.append((os.path.join("input_dir", "file2.json"), "machines"))

        # Execute
        process_directory("input_dir", "output_dir")

        # Verify
        mock_makedirs.assert_called_once_with("output_dir", exist_ok=True)
        self.assertEqual(
            mock_process_json_file.call_count, 3
        )  # Should be called for all 3 JSON files

        # Check log messages
        log_output = self.log_output.getvalue()
        self.assertIn("Found 3 JSON files to process", log_output)
        self.assertIn("FAILURE SUMMARY", log_output)
        self.assertIn("Total files with failures: 2", log_output)

    @patch("os.makedirs", side_effect=Exception("Directory creation failed"))
    @patch("os.listdir")
    @patch("os.path.isdir", return_value=True)  # Mock isdir to return True
    @patch(
        "os.environ.get", return_value="True"
    )  # Mock PYTEST_CURRENT_TEST environment variable
    def test_handles_makedirs_exception(
        self, mock_environ_get, mock_isdir, mock_listdir, mock_makedirs
    ):
        """Test handling of exceptions during directory creation."""
        # Execute - we expect the exception to be propagated
        with self.assertRaises(Exception) as context:
            process_directory("input_dir", "output_dir")

        # Verify the specific exception message
        self.assertEqual(str(context.exception), "Directory creation failed")


class TestIntegrationWithSampleData(unittest.TestCase):
    """Integration tests using the sample data."""

    def setUp(self):
        """Set up the test environment before each test."""
        # Path to the sample data file
        self.sample_file_path = str(
            Path(__file__).parent / "data" / "tubetech.de_extracted.json"
        )
        self.output_file_path = str(
            Path(__file__).parent / "data" / "tubetech.de_pluralized.json"
        )

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
                    "Rippenrohre",
                ],
                "machines": [],
                "process_type": [
                    "Schweißarbeiten",
                    "Fräsungen",
                    "Bohrungen",
                    "Montagearbeiten",
                    "Qualitätssicherungen",
                ],
                "lohnfertigung": False,
                "error": False,
            }
        ]

        # Create the sample file if it doesn't exist
        if not os.path.exists(self.sample_file_path):
            os.makedirs(os.path.dirname(self.sample_file_path), exist_ok=True)
            with open(self.sample_file_path, "w", encoding="utf-8") as f:
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

        # Also remove the input file that was created in setUp
        if os.path.exists(self.sample_file_path):
            os.remove(self.sample_file_path)

    @patch("webcrawl.pluralize_with_llm.pluralize_with_llm")
    def test_process_sample_file(self, mock_pluralize):
        """Test processing the sample file."""
        # Mock the pluralization to return expected values
        mock_pluralize.return_value = {
            "products": [
                "Luftkühler",
                "Luftvorwärmer",
                "Ersatzwärmetauscher",
                "Rippenrohre",
            ],
            "process_type": [
                "Schweißarbeiten",
                "Fräsungen",
                "Bohrungen",
                "Montagearbeiten",
                "Qualitätssicherungen",
            ],
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


class TestModelFallbackIntegration(unittest.TestCase):
    """Integration tests for model fallback functionality with higher-level functions."""

    def setUp(self):
        """Set up the test environment before each test."""
        # Clear the failed_files list
        global failed_files
        failed_files.clear()

        # Clear compound_word_stats
        compound_word_stats["files_affected"] = set()
        compound_word_stats["words_modified"] = []

    @patch("webcrawl.pluralize_with_llm.completion")
    @patch("builtins.open", new_callable=mock.mock_open, 
           read_data='[{"company_name": "Test", "products": ["Product1", "Product2"]}]')
    @patch("os.makedirs")
    def test_process_json_file_withModelFallbacks_usesCorrectModels(self, mock_makedirs, mock_open, mock_completion):
        """Test that process_json_file correctly passes through model configurations."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["IntegrationTest1", "IntegrationTest2"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        # Call process_json_file (it should use default models internally)
        process_json_file("input.json", "output.json")

        # Verify that completion was called with default model configuration
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args
        
        # Check that default models are used
        self.assertEqual(kwargs["model"], "bedrock/amazon.nova-pro-v1:0")
        self.assertEqual(kwargs["fallbacks"], ["bedrock/us.amazon.nova-lite-v1:0"])

    def test_backwards_compatibility_temperatureParameter_stillWorks(self):
        """Test that existing code using temperatures parameter still works without breaking."""
        fields_dict = {"products": ["BackwardsTest"]}
        
        # This should not raise an exception even though temperatures is deprecated
        with patch("webcrawl.pluralize_with_llm.completion") as mock_completion:
            mock_response = MagicMock()
            mock_response.choices[0].message.content = json.dumps({
                "products": ["BackwardsPluralized"],
                "machines": [],
                "process_type": []
            })
            mock_completion.return_value = mock_response
            
            # Call with old-style temperatures parameter
            result = pluralize_with_llm(fields_dict, "test.json", temperatures=[0.5, 0.7, 0.9])
            
            # Should still work and return expected result
            self.assertEqual(result["products"], ["BackwardsPluralized"])
            
            # Should use fixed temperature, not from deprecated parameter
            args, kwargs = mock_completion.call_args
            self.assertEqual(kwargs["temperature"], 0.3)

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_default_models_constant_matches_implementation(self, mock_completion):
        """Test that DEFAULT_MODELS constant matches what's actually used."""
        from webcrawl.pluralize_with_llm import DEFAULT_MODELS
        
        # Mock successful response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["ConstantTest"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        fields_dict = {"products": ["TestInput"]}
        
        # Call function without specifying models (should use defaults)
        pluralize_with_llm(fields_dict, "test.json")
        
        # Verify that the DEFAULT_MODELS constant matches actual usage
        args, kwargs = mock_completion.call_args
        self.assertEqual(kwargs["model"], DEFAULT_MODELS[0])
        self.assertEqual(kwargs["fallbacks"], DEFAULT_MODELS[1:])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_nova_model_ids_correct_format(self, mock_completion):
        """Test that Nova model IDs follow the correct format for different regions."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps({
            "products": ["FormatTest"],
            "machines": [],
            "process_type": []
        })
        mock_completion.return_value = mock_response

        # Test with Nova models from different regions
        nova_models = [
            "bedrock/amazon.nova-pro-v1:0",        # Global
            "bedrock/us.amazon.nova-lite-v1:0",    # US region
            "bedrock/us.amazon.nova-micro-v1:0"    # US region
        ]
        
        fields_dict = {"products": ["RegionTest"]}
        
        # Call with specific Nova models
        pluralize_with_llm(fields_dict, "test.json", models=nova_models)
        
        # Verify that the models are passed correctly
        args, kwargs = mock_completion.call_args
        self.assertEqual(kwargs["model"], "bedrock/amazon.nova-pro-v1:0")
        self.assertEqual(kwargs["fallbacks"], ["bedrock/us.amazon.nova-lite-v1:0", "bedrock/us.amazon.nova-micro-v1:0"])

    @patch("webcrawl.pluralize_with_llm.completion")
    def test_error_handling_preserves_file_context(self, mock_completion):
        """Test that error handling correctly preserves file context for debugging."""
        # Mock completion to raise an exception
        mock_completion.side_effect = Exception("Model API error")
        
        fields_dict = {"products": ["ErrorContextTest"]}
        file_path = "/path/to/specific/company_file.json"
        
        # Call function with specific file path
        result = pluralize_with_llm(fields_dict, file_path)
        
        # Should return original fields
        self.assertEqual(result, fields_dict)
        
        # Should record failure with correct file path
        self.assertEqual(len(failed_files), 1)
        self.assertEqual(failed_files[0][0], file_path)
        self.assertIn("products", failed_files[0][1])  # Should include field info
