"""
Unit tests for extract_llm.py automation pipeline integration.
Covers: valid input, invalid input, edge cases, and error conditions.
"""
import os
import sys
import asyncio
import unittest
from unittest.mock import patch
from typing import Any

class TestExtractLlm(unittest.TestCase):
    """Tests for extract_llm.py automation pipeline integration."""

    def setUp(self) -> None:
        self.valid_input_dir = 'tests/automation/webcrawl/data/valid_markdown_dir'
        self.invalid_input_dir = 'tests/automation/webcrawl/data/invalid_markdown_dir'
        self.empty_input_dir = 'tests/automation/webcrawl/data/empty_markdown_dir'
        self.missing_dir = 'tests/automation/webcrawl/data/missing_markdown_dir'
        self.output_dir = 'tests/automation/webcrawl/output/llm_extracted_output'
        os.makedirs(self.valid_input_dir, exist_ok=True)
        os.makedirs(self.invalid_input_dir, exist_ok=True)
        os.makedirs(self.empty_input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        # Create sample valid markdown file
        valid_markdown_file = 'tests/automation/webcrawl/valid_markdown_all_filled.md'
        with open(valid_markdown_file, 'r') as f:
            valid_content = f.read()
        with open(os.path.join(self.valid_input_dir, 'firma_a.md'), 'w') as f:
            f.write(valid_content)
        # Create sample invalid markdown file (irrelevant content)
        with open(os.path.join(self.invalid_input_dir, 'firma_b.md'), 'w') as f:
            f.write('# Firma B\nNo relevant keywords')

    def tearDown(self) -> None:
        for d in [self.valid_input_dir, self.invalid_input_dir, self.empty_input_dir, self.output_dir]:
            if os.path.exists(d):
                for file in os.listdir(d):
                    os.remove(os.path.join(d, file))
                os.rmdir(d)

    def test_main_validInput_createsJsonFiles(self):
        """main_validInput_createsJsonFiles_expectedJsonCreated: Should create JSON files for each company in valid input"""
        import webcrawl.extract_llm as extract_llm
        test_args = [
            "extract_llm.py",
            self.valid_input_dir,
            "--output",
            self.output_dir,
            "--ext",
            ".md"
        ]
        with patch.object(sys, 'argv', test_args):
            asyncio.run(extract_llm.main())
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input")

    def test_main_invalidInput_noRelevantData_createsNoJson(self):
        """main_invalidInput_noRelevantData_createsNoJson_expectedNoOutput: Should not create JSON output for irrelevant input"""
        import webcrawl.extract_llm as extract_llm
        test_args = [
            "extract_llm.py",
            self.invalid_input_dir,
            "--output",
            self.output_dir,
            "--ext",
            ".md"
        ]
        with patch.object(sys, 'argv', test_args):
            asyncio.run(extract_llm.main())
        output_files = os.listdir(self.output_dir)
        self.assertFalse(any(f.endswith('.json') for f in output_files), "JSON output should not be created for irrelevant input")

    def test_main_emptyInput_createsNoJson(self):
        """main_emptyInput_createsNoJson_expectedNoOutput: Should not create JSON output for empty input directory"""
        import webcrawl.extract_llm as extract_llm
        test_args = [
            "extract_llm.py",
            self.empty_input_dir,
            "--output",
            self.output_dir,
            "--ext",
            ".md"
        ]
        with patch.object(sys, 'argv', test_args):
            asyncio.run(extract_llm.main())
        self.assertFalse(os.listdir(self.output_dir), "Output directory should be empty for empty input directory")

    def test_main_missingInputDir_raisesFileNotFoundError(self):
        """main_missingInputDir_raisesFileNotFoundError_expectedException: Should raise FileNotFoundError for missing input directory"""
        import webcrawl.extract_llm as extract_llm
        test_args = [
            "extract_llm.py",
            self.missing_dir,
            "--output",
            self.output_dir,
            "--ext",
            ".md"
        ]
        with patch.object(sys, 'argv', test_args):
            with self.assertRaises(FileNotFoundError):
                asyncio.run(extract_llm.main())

    def test_run_extract_llm_validInput_createsJsonFiles(self):
        """
        run_extract_llm_validInput_createsJsonFiles_expectedJsonCreated: Should create JSON files for each company in valid input when called directly
        """
        import webcrawl.extract_llm as extract_llm
        # Use default strategy
        output_dir = extract_llm.run_extract_llm(
            input_path=self.valid_input_dir,
            output_dir=self.output_dir,
            ext=".md",
            only_recheck=False,
            overwrite=True,
            log_level="INFO",
            llm_strategy=None
        )
        output_files = os.listdir(output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input using run_extract_llm")

    def test_run_extract_llm_customStrategy_validInput(self):
        """
        run_extract_llm_customStrategy_validInput_expectedJsonCreated: Should allow passing a custom LLMExtractionStrategy and create output
        """
        import webcrawl.extract_llm as extract_llm
        from crawl4ai.async_configs import LLMConfig
        from crawl4ai.extraction_strategy import LLMExtractionStrategy
        # Create a custom strategy (same as default, but explicit)
        custom_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(provider="bedrock/amazon.nova-pro-v1:0"),
            extraction_type="schema",
            schema=extract_llm.Company.model_json_schema(),
            instruction=extract_llm.prompt,
            chunk_token_threshold=4096,
            overlap_rate=0.1,
            input_format="markdown",
            apply_chunking=False,
            extra_args={"temperature": 0.7, "max_tokens": 1000},
        )
        output_dir = extract_llm.run_extract_llm(
            input_path=self.valid_input_dir,
            output_dir=self.output_dir,
            ext=".md",
            only_recheck=False,
            overwrite=True,
            log_level="INFO",
            llm_strategy=custom_strategy
        )
        output_files = os.listdir(output_dir)
        self.assertTrue(any(f.endswith('.json') for f in output_files), "No JSON output created for valid input with custom strategy")

if __name__ == '__main__':
    unittest.main()
