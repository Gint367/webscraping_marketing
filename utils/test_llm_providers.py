#!/usr/bin/env python3
"""
LLM Provider Connectivity Test for the Pipeline

This script tests the connectivity and availability of the LLM providers used in the
data extraction and processing pipeline. It should be run before executing the 
master_pipeline.py script to ensure that all the necessary LLM services are accessible.

Usage:
    python test_llm_providers.py [--verbose]
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Dict, List, Optional, Tuple

# Import required libraries for LLM testing
import litellm
from crawl4ai import AsyncWebCrawler, CacheMode
from crawl4ai.async_configs import CrawlerRunConfig, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from litellm import completion
from litellm.exceptions import APIError, JSONSchemaValidationError, RateLimitError
from pydantic import BaseModel, Field


# Configure logging
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configure logging with proper formatting.

    Args:
        log_level (str): The minimum logging level to display as a string (e.g., "INFO", "DEBUG")

    Returns:
        logging.Logger: Configured logger
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create module-specific logger
    logger = logging.getLogger('test_llm_providers')
    logger.setLevel(numeric_level)
    
    # Set log level for HTTPx, which is used by AsyncWebCrawler
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    
    # Set log level for LiteLLM and Botocore
    logging.getLogger("LiteLLM").setLevel(logging.WARNING if numeric_level > logging.DEBUG else logging.DEBUG)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    
    # Enable LiteLLM debug mode if log level is DEBUG
    if numeric_level == logging.DEBUG:
        from litellm._logging import _turn_on_debug
        _turn_on_debug()
        logger.debug("LiteLLM debug mode enabled")
    
    return logger


# Define constants for LLM providers
LLM_PROVIDERS = {
    "litellm": {
        "provider": "bedrock/amazon.nova-pro-v1:0",  # Used in pluralize_with_llm.py
        "max_retries": 3,
        "timeout": 30,  # seconds
    },
    "crawl4ai": {
        "provider": "bedrock/amazon.nova-pro-v1:0",  # Used in extract_llm.py
        "max_retries": 3,
        "timeout": 30,  # seconds
    }
}


class SimpleTestSchema(BaseModel):
    """
    A simple schema for testing LLM responses.
    Used for validating that the LLM can return structured data.
    """
    message: str = Field(..., description="A simple message")
    is_working: bool = Field(..., description="Whether the test was successful")


async def test_crawl4ai_provider(
    provider: str,
    timeout: int = 30,
    max_retries: int = 3,
    logger: Optional[logging.Logger] = None
) -> Tuple[bool, str]:
    """
    Test connectivity to the Crawl4AI LLM provider.

    Args:
        provider (str): The provider string (e.g., "bedrock/amazon.nova-pro-v1:0")
        timeout (int): Maximum time in seconds to wait for a response
        max_retries (int): Number of retries on failure
        logger (Optional[logging.Logger]): Logger instance

    Returns:
        Tuple[bool, str]: (success, message)
    """
    if logger is None:
        logger = logging.getLogger('test_llm_providers')
    
    logger.info(f"Testing Crawl4AI provider: {provider}")
    
    # Simple prompt for testing
    test_prompt = "Please respond with a short confirmation that you are working."
    
    # Configure LLM strategy
    llm_config = LLMConfig(provider=provider)
    
    # Create simple extraction strategy
    llm_strategy = LLMExtractionStrategy(
        llm_config=llm_config,
        extraction_type="raw",  # Use raw extraction for simple test
        instruction=test_prompt,
        chunk_token_threshold=256,  # Small threshold for test
        overlap_rate=0.1,
        input_format="markdown",  # Using markdown format to match file:// URL
        apply_chunking=False,
        extra_args={"temperature": 0.0, "max_tokens": 50},  # Minimal tokens for test
    )
    
    # Configure test
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,  # Don't cache test results
        extraction_strategy=llm_strategy,
    )
    
    # Create a simple test input (just a string)
    test_input = "This is a test input. Please confirm you are operational."
    
    # Track retries
    retries = 0
    start_time = time.time()
    error_message = ""
    
    while retries < max_retries:
        try:
            # Test the provider with a simple extraction
            async with AsyncWebCrawler() as crawler:
                # Create a dummy URL representing our test as a markdown file
                test_url = "https://example.com/"
                
                # Set a timeout for the request
                result = await asyncio.wait_for(
                    crawler.arun(
                        url=test_url,
                        config=config,
                        text_content=test_input  # Pass text directly instead of fetching URL
                    ),
                    timeout=timeout
                )
                
                # Check the result
                if result.success and result.extracted_content:
                    elapsed = time.time() - start_time
                    return True, f"Successfully connected to {provider} (in {elapsed:.2f}s)"
                else:
                    error_message = f"Extraction failed: {getattr(result, 'error_message', 'Unknown error')}"
                    logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
            
        except asyncio.TimeoutError:
            error_message = f"Timeout after {timeout} seconds"
            logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
        
        except Exception as e:
            error_message = str(e)
            logger.warning(f"Attempt {retries+1}/{max_retries}: Error testing Crawl4AI provider: {e}")
        
        # Increment retry counter
        retries += 1
        
        # Wait before retrying (exponential backoff)
        if retries < max_retries:
            wait_time = 2 ** retries
            logger.info(f"Waiting {wait_time}s before retry...")
            await asyncio.sleep(wait_time)
    
    # All retries failed
    return False, f"Failed to connect to {provider} after {max_retries} attempts: {error_message}"


def test_litellm_provider(
    provider: str,
    timeout: int = 30,
    max_retries: int = 3,
    logger: Optional[logging.Logger] = None
) -> Tuple[bool, str]:
    """
    Test connectivity to the LiteLLM provider.

    Args:
        provider (str): The provider string (e.g., "bedrock/amazon.nova-pro-v1:0")
        timeout (int): Maximum time in seconds to wait for a response
        max_retries (int): Number of retries on failure
        logger (Optional[logging.Logger]): Logger instance

    Returns:
        Tuple[bool, str]: (success, message)
    """
    if logger is None:
        logger = logging.getLogger('test_llm_providers')
    
    logger.info(f"Testing LiteLLM provider: {provider}")
    
    # Simple prompt for testing
    test_prompt = "Please respond with a simple JSON structure with 'message' containing 'I am operational' and 'is_working' set to true."
    
    # Enable JSON schema validation for structured output
    litellm.enable_json_schema_validation = True
    
    # Try with Python object first, fall back to JSON parsing if needed
    def extract_content(raw_content):
        """Helper function to extract content whether it's a dict or JSON string"""
        if isinstance(raw_content, dict):
            return raw_content
        if isinstance(raw_content, str):
            try:
                return json.loads(raw_content)
            except json.JSONDecodeError:
                return {"message": raw_content, "is_working": False}
        return {"message": str(raw_content), "is_working": False}
    
    # Track retries
    retries = 0
    start_time = time.time()
    error_message = ""
    
    while retries < max_retries:
        try:
            # Test the provider with a simple completion request
            response = completion(
                model=provider,
                messages=[{"role": "user", "content": test_prompt}],
                temperature=0.1,  # Use deterministic output for testing
                max_tokens=100,  # Minimal tokens for test
                response_format=SimpleTestSchema,  # Use schema for validation
                timeout=timeout  # Set timeout for the request
            )
            
            # Log the raw response for debugging
            if logger.level <= logging.DEBUG:
                logger.debug(f"Raw LiteLLM response: {json.dumps(response, default=str)}")
                if hasattr(response, 'choices') and len(response.choices) > 0:
                    logger.debug(f"Response content: {response.choices[0].message.content}")
                    logger.debug(f"Content type: {type(response.choices[0].message.content)}")
            
            # Check response format and content
            if response and hasattr(response, 'choices') and len(response.choices) > 0:
                # Extract content from response
                try:
                    raw_content = response.choices[0].message.content
                    content = extract_content(raw_content)
                    
                    # Check if the content indicates the LLM is working
                    if content.get('is_working') is True:
                        elapsed = time.time() - start_time
                        return True, f"Successfully connected to {provider} (in {elapsed:.2f}s)"
                    else:
                        error_message = f"Response did not confirm working status: {content}"
                        logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
                except (AttributeError, IndexError) as e:
                    error_message = f"Invalid response structure: {str(e)}"
                    logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
            else:
                error_message = "Empty or invalid response"
                logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
                
        except JSONSchemaValidationError as se:
            error_message = f"Schema validation failed: {str(se).splitlines()[0]}"
            logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
            
        except RateLimitError as rle:
            error_message = f"Rate limit exceeded: {str(rle)}"
            logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
            
        except APIError as ae:
            error_message = f"API error: {str(ae)}"
            logger.warning(f"Attempt {retries+1}/{max_retries}: {error_message}")
            
        except Exception as e:
            error_message = str(e)
            logger.warning(f"Attempt {retries+1}/{max_retries}: Error testing LiteLLM provider: {e}")
        
        # Increment retry counter
        retries += 1
        
        # Wait before retrying (exponential backoff)
        if retries < max_retries:
            wait_time = 2 ** retries
            logger.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    # All retries failed
    return False, f"Failed to connect to {provider} after {max_retries} attempts: {error_message}"


async def run_all_tests(verbose: bool = False) -> Dict[str, List[Tuple[bool, str]]]:
    """
    Run all LLM provider tests and return results.

    Args:
        verbose (bool): Whether to enable verbose logging

    Returns:
        Dict[str, List[Tuple[bool, str]]]: Results for each provider type
    """
    # Set up logging based on verbosity
    log_level = "DEBUG" if verbose else "INFO"
    logger = setup_logging(log_level)
    
    logger.info("Starting LLM provider connectivity tests...")
    
    # Results dictionary
    results = {
        "litellm": [],
        "crawl4ai": []
    }
    
    # Test LiteLLM provider
    litellm_provider = LLM_PROVIDERS["litellm"]["provider"]
    litellm_timeout = LLM_PROVIDERS["litellm"]["timeout"]
    litellm_max_retries = LLM_PROVIDERS["litellm"]["max_retries"]
    
    logger.info(f"Testing LiteLLM provider ({litellm_provider})...")
    litellm_success, litellm_message = test_litellm_provider(
        provider=litellm_provider,
        timeout=litellm_timeout,
        max_retries=litellm_max_retries,
        logger=logger
    )
    results["litellm"].append((litellm_success, litellm_message))
    logger.info(f"LiteLLM test result: {'SUCCESS' if litellm_success else 'FAILURE'} - {litellm_message}")
    
    # Test Crawl4AI provider
    crawl4ai_provider = LLM_PROVIDERS["crawl4ai"]["provider"]
    crawl4ai_timeout = LLM_PROVIDERS["crawl4ai"]["timeout"]
    crawl4ai_max_retries = LLM_PROVIDERS["crawl4ai"]["max_retries"]
    
    logger.info(f"Testing Crawl4AI provider ({crawl4ai_provider})...")
    crawl4ai_success, crawl4ai_message = await test_crawl4ai_provider(
        provider=crawl4ai_provider,
        timeout=crawl4ai_timeout,
        max_retries=crawl4ai_max_retries,
        logger=logger
    )
    results["crawl4ai"].append((crawl4ai_success, crawl4ai_message))
    logger.info(f"Crawl4AI test result: {'SUCCESS' if crawl4ai_success else 'FAILURE'} - {crawl4ai_message}")
    
    return results


def main():
    """
    Main function to run the LLM provider tests from command line.
    """
    parser = argparse.ArgumentParser(
        description='Test connectivity to LLM providers used in the pipeline'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose debug logging'
    )
    args = parser.parse_args()
    
    # Run all tests
    results = asyncio.run(run_all_tests(args.verbose))
    
    # Print summary
    print("\n=== LLM Provider Test Results ===")
    
    all_successful = True
    
    for provider_type, provider_results in results.items():
        for success, message in provider_results:
            status = "✅ SUCCESS" if success else "❌ FAILURE"
            print(f"{provider_type}: {status} - {message}")
            
            if not success:
                all_successful = False
    
    # Exit with appropriate exit code
    if all_successful:
        print("\n✅ All LLM providers are available and working correctly.")
        return 0
    else:
        print("\n❌ One or more LLM providers failed the connectivity test.")
        print("   The master pipeline may not work correctly without all providers.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
