import unittest
from unittest.mock import patch, MagicMock
import time
import json
from webcrawl.fill_process_type import generate_process_types, extract_category_from_folder, remove_na_words

# Absolute import of the function to test
class TestExtractCategoryFromFolder(unittest.TestCase):
    def test_llm_extracted_pattern_valid_category_returns_category(self):
        folder = "llm_extracted_maschinenbau"
        result = extract_category_from_folder(folder)
        self.assertEqual(result, "maschinenbau")

    def test_pluralized_pattern_valid_category_returns_category(self):
        folder = "pluralized_blechteile"
        result = extract_category_from_folder(folder)
        self.assertEqual(result, "blechteile")

    def test_folder_with_trailing_slash_returns_category(self):
        folder = "pluralized_werkzeughersteller/"
        result = extract_category_from_folder(folder)
        self.assertEqual(result, "werkzeughersteller")

    def test_folder_with_no_match_returns_none(self):
        folder = "random_folder_name"
        result = extract_category_from_folder(folder)
        self.assertIsNone(result)

    def test_folder_with_nested_path_returns_category(self):
        folder = "/some/path/llm_extracted_stahlverarbeitung"
        result = extract_category_from_folder(folder)
        self.assertEqual(result, "stahlverarbeitung")

class TestGenerateProcessTypes(unittest.TestCase):
    def setUp(self):
        # Sample data for tests
        self.sample_products = ["Produkt A", "Produkt B", "Produkt C"]
        self.sample_machines = ["Maschine A", "Maschine B"]
        self.sample_category = "maschinenbau"
        
        # Expected process types
        self.expected_process_types = [
            "Fräsungen", "Bohrungen", "Schweißarbeiten", "Drehprozesse", "Montagearbeiten"
        ]
        
        # Mock response using proper JSON format
        self.mock_successful_response = MagicMock()
        self.mock_successful_response.choices = [
            MagicMock(
                message=MagicMock(
                    content=json.dumps({"process_types": self.expected_process_types})
                )
            )
        ]

    @patch('webcrawl.fill_process_type.completion')
    def test_successful_generation(self, mock_completion):
        """Test process type generation with successful API response"""
        mock_completion.return_value = self.mock_successful_response
        result = generate_process_types(self.sample_products, self.sample_machines, self.sample_category)
        self.assertEqual(result, self.expected_process_types)
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args
        self.assertEqual(kwargs['model'], "bedrock/amazon.nova-pro-v1:0")
        self.assertEqual(kwargs['temperature'], 0.3)
        prompt = kwargs['messages'][0]['content']
        self.assertIn(self.sample_category, prompt)
        for product in self.sample_products:
            self.assertIn(product, prompt)

    @patch('webcrawl.fill_process_type.completion')
    def test_empty_product_list(self, mock_completion):
        """Test behavior with empty product list"""
        result = generate_process_types([], [], self.sample_category)
        self.assertEqual(result, [])
        mock_completion.assert_not_called()

    @patch('webcrawl.fill_process_type.completion')
    @patch('webcrawl.fill_process_type.time.sleep')
    def test_retry_on_error(self, mock_sleep, mock_completion):
        """Test retry behavior when API calls fail"""
        mock_completion.side_effect = [
            Exception("API Rate limit exceeded"),
            self.mock_successful_response
        ]
        result = generate_process_types(self.sample_products, self.sample_machines, self.sample_category)
        self.assertEqual(result, self.expected_process_types)
        self.assertEqual(mock_completion.call_count, 2)
        mock_sleep.assert_called_once()

    @patch('webcrawl.fill_process_type.completion')
    @patch('webcrawl.fill_process_type.time.sleep')
    def test_max_retries_exceeded(self, mock_sleep, mock_completion):
        """Test behavior when max retries are exceeded"""
        mock_completion.side_effect = Exception("API error")
        result = generate_process_types(
            self.sample_products,
            self.sample_machines,
            self.sample_category,
            max_retries=2
        )
        self.assertEqual(result, [])
        self.assertEqual(mock_completion.call_count, 3)  # Initial + 2 retries
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('webcrawl.fill_process_type.completion')
    def test_filter_empty_process_types(self, mock_completion):
        """Test filtering of empty items in process types"""
        response_with_empty = MagicMock()
        response_with_empty.choices = [
            MagicMock(
                message=MagicMock(
                    content=json.dumps({"process_types": ["Fräsungen", "", "Bohrungen", "", "Schweißarbeiten"]})
                )
            )
        ]
        mock_completion.return_value = response_with_empty
        result = generate_process_types(self.sample_products, self.sample_machines, self.sample_category)
        self.assertEqual(result, ["Fräsungen", "Bohrungen", "Schweißarbeiten"])

    @patch('webcrawl.fill_process_type.completion')
    def test_response_parsing(self, mock_completion):
        """Test parsing of different response formats"""
        response_with_formatting = MagicMock()
        response_with_formatting.choices = [
            MagicMock(
                message=MagicMock(
                    content=json.dumps({"process_types": ["  Fräsungen ", "Bohrungen  ", " Schweißarbeiten  "]})
                )
            )
        ]
        mock_completion.return_value = response_with_formatting
        result = generate_process_types(self.sample_products, self.sample_machines, self.sample_category)
        self.assertEqual(result, ["Fräsungen", "Bohrungen", "Schweißarbeiten"])

    @patch('webcrawl.fill_process_type.completion')
    def test_json_validation_error(self, mock_completion):
        """Test handling of JSON validation errors"""
        # Create invalid response
        invalid_response = MagicMock()
        invalid_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="Invalid JSON"
                )
            )
        ]
        mock_completion.return_value = invalid_response
        
        # Call function with minimal retries and delay
        result = generate_process_types(
            self.sample_products,
            self.sample_machines,
            self.sample_category,
            max_retries=0,  # No retries
            base_delay=0  # Minimal delay if retry happens
        )
        
        self.assertEqual(result, [])
        # Verify completion was called exactly once
        mock_completion.assert_called_once()
        
    @patch('webcrawl.fill_process_type.completion')
    @patch('webcrawl.fill_process_type.time.sleep')  # Mock sleep to prevent actual waiting
    def test_json_validation_error_with_retries(self, mock_sleep, mock_completion):
        """Test retry behavior with JSON validation errors"""
        invalid_response = MagicMock()
        invalid_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="Invalid JSON"
                )
            )
        ]
        # Set up multiple failed attempts
        mock_completion.side_effect = [invalid_response] * 3
        
        result = generate_process_types(
            self.sample_products,
            self.sample_machines,
            self.sample_category,
            max_retries=2,
            base_delay=1  # This won't cause actual delay due to mocked sleep
        )
        
        self.assertEqual(result, [])
        self.assertEqual(mock_completion.call_count, 3)  # Initial + 2 retries
        self.assertEqual(mock_sleep.call_count, 2)  # Should be called twice for 2 retries

class TestRemoveNaWords(unittest.TestCase):
    """Unit tests for remove_na_words function."""
    from webcrawl.fill_process_type import remove_na_words
    def test_remove_na_words_positive_removes_na(self):
        process_types = ['Fräsungen', 'na', 'Bohrungen', 'N/A', 'Schweißarbeiten', 'n.a.', 'none']
        result = remove_na_words(process_types)
        self.assertEqual(result, ['Fräsungen', 'Bohrungen', 'Schweißarbeiten'])

    def test_remove_na_words_negative_no_na(self):
        process_types = ['Fräsungen', 'Bohrungen', 'Schweißarbeiten']
        result = remove_na_words(process_types)
        self.assertEqual(result, process_types)

    def test_remove_na_words_empty_list(self):
        process_types = []
        result = remove_na_words(process_types)
        self.assertEqual(result, [])

    def test_remove_na_words_all_na(self):
        process_types = ['na', 'n.a.', 'n/a', 'nicht verfügbar', 'keine', 'none']
        result = remove_na_words(process_types)
        self.assertEqual(result, [])

    def test_remove_na_words_mixed_case_and_whitespace(self):
        process_types = [' NA ', 'N.a.', '  n/A', 'Nicht verfügbar', 'keine ', 'NONE', 'Fräsungen']
        result = remove_na_words(process_types)
        self.assertEqual(result, ['Fräsungen'])

    def test_remove_na_words_word_contains_na_should_not_remove(self):
        """Test that words containing 'na' as a substring are not removed."""
        process_types = ['Fräsungen', 'Manatur', 'Banana', 'Analytik', 'na', 'Montage']
        
        result = remove_na_words(process_types)
        # Only the exact 'na' should be removed
        self.assertEqual(result, ['Fräsungen', 'Manatur', 'Banana', 'Analytik', 'Montage'])

if __name__ == '__main__':
    unittest.main()