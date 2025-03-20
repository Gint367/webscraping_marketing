import unittest
from unittest.mock import patch, MagicMock
import time
import json
from webcrawl.fill_process_type import generate_process_types

# Absolute import of the function to test

class TestGenerateProcessTypes(unittest.TestCase):
    def setUp(self):
        # Sample data for tests
        self.sample_products = ["Produkt A", "Produkt B", "Produkt C"]
        self.sample_category = "maschinenbau"
        
        # Mock response for successful API call
        self.mock_successful_response = MagicMock()
        self.mock_successful_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="Fräsungen, Bohrungen, Schweißarbeiten, Drehprozesse, Montagearbeiten"
                )
            )
        ]
        
        # Expected process types after parsing
        self.expected_process_types = [
            "Fräsungen", "Bohrungen", "Schweißarbeiten", "Drehprozesse", "Montagearbeiten"
        ]

    @patch('webcrawl.fill_process_type.completion')
    def test_successful_generation(self, mock_completion):
        """Test process type generation with successful API response"""
        # Configure mock to return successful response
        mock_completion.return_value = self.mock_successful_response
        
        # Call the function
        result = generate_process_types(self.sample_products, self.sample_category)
        
        # Verify the result contains the expected process types
        self.assertEqual(result, self.expected_process_types)
        
        # Verify completion was called with correct parameters
        mock_completion.assert_called_once()
        args, kwargs = mock_completion.call_args
        
        # Check that the model is correct
        self.assertEqual(kwargs['model'], "bedrock/amazon.nova-pro-v1:0")
        
        # Check that the prompt contains the products and category
        prompt = kwargs['messages'][0]['content']
        self.assertIn(self.sample_category, prompt)
        for product in self.sample_products:
            self.assertIn(product, prompt)

    @patch('webcrawl.fill_process_type.completion')
    def test_empty_product_list(self, mock_completion):
        """Test behavior with empty product list"""
        # The function should return an empty list without calling the API
        result = generate_process_types([], self.sample_category)
        
        # Verify the result is an empty list
        self.assertEqual(result, [])
        
        # Verify completion was not called
        mock_completion.assert_not_called()

    @patch('webcrawl.fill_process_type.completion')
    @patch('webcrawl.fill_process_type.time.sleep')
    def test_retry_on_error(self, mock_sleep, mock_completion):
        """Test retry behavior when API calls fail"""
        # Configure mock to raise an exception on first call and succeed on second
        mock_completion.side_effect = [
            Exception("API Rate limit exceeded"),
            self.mock_successful_response
        ]
        
        # Call the function
        result = generate_process_types(self.sample_products, self.sample_category)
        
        # Verify the result contains the expected process types after retry
        self.assertEqual(result, self.expected_process_types)
        
        # Verify completion was called twice
        self.assertEqual(mock_completion.call_count, 2)
        
        # Verify sleep was called once for the retry
        mock_sleep.assert_called_once()

    @patch('webcrawl.fill_process_type.completion')
    @patch('webcrawl.fill_process_type.time.sleep')
    def test_max_retries_exceeded(self, mock_sleep, mock_completion):
        """Test behavior when max retries are exceeded"""
        # Configure mock to always raise an exception
        mock_completion.side_effect = Exception("API error")
        
        # Call the function with a low max_retries
        result = generate_process_types(
            self.sample_products, 
            self.sample_category,
            max_retries=2
        )
        
        # Verify the result is an empty list after all retries fail
        self.assertEqual(result, [])
        
        # Verify completion was called the expected number of times (initial + retries)
        self.assertEqual(mock_completion.call_count, 3)  # Initial + 2 retries
        
        # Verify sleep was called for each retry
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('webcrawl.fill_process_type.completion')
    def test_filter_empty_process_types(self, mock_completion):
        """Test filtering of empty items in process types"""
        # Mock response with empty items
        response_with_empty = MagicMock()
        response_with_empty.choices = [
            MagicMock(
                message=MagicMock(
                    content="Fräsungen, , Bohrungen,, Schweißarbeiten"
                )
            )
        ]
        mock_completion.return_value = response_with_empty
        
        # Call the function
        result = generate_process_types(self.sample_products, self.sample_category)
        
        # Verify empty items are filtered out
        self.assertEqual(result, ["Fräsungen", "Bohrungen", "Schweißarbeiten"])

    @patch('webcrawl.fill_process_type.completion')
    def test_response_parsing(self, mock_completion):
        """Test parsing of different response formats"""
        # Mock response with extra whitespace and different formats
        response_with_formatting = MagicMock()
        response_with_formatting.choices = [
            MagicMock(
                message=MagicMock(
                    content="  Fräsungen ,Bohrungen  , Schweißarbeiten  "
                )
            )
        ]
        mock_completion.return_value = response_with_formatting
        
        # Call the function
        result = generate_process_types(self.sample_products, self.sample_category)
        
        # Verify whitespace is handled correctly
        self.assertEqual(result, ["Fräsungen", "Bohrungen", "Schweißarbeiten"])

    @patch('webcrawl.fill_process_type.completion')
    def test_custom_retry_parameters(self, mock_completion):
        """Test custom retry parameters"""
        # Configure mock to raise an exception
        mock_completion.side_effect = Exception("API error")
        
        # Call the function with custom retry parameters
        max_retries = 1
        base_delay = 1
        
        # Mock time.sleep to avoid waiting in tests
        with patch('webcrawl.fill_process_type.time.sleep') as mock_sleep:
            result = generate_process_types(
                self.sample_products, 
                self.sample_category,
                max_retries=max_retries,
                base_delay=base_delay
            )
            
            # Verify the function attempted the correct number of retries
            self.assertEqual(mock_completion.call_count, max_retries + 1)
            
            # Verify sleep was called with the correct parameters
            # The first retry should use base_delay * (2^(1-1)) + jitter
            # We can't check the exact value due to random jitter
            mock_sleep.assert_called_once()

if __name__ == '__main__':
    unittest.main()