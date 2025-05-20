#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration section module for the Streamlit application.
"""

import logging
import streamlit as st
from typing import Dict, Any

# Set up module level logger
logger = logging.getLogger(__name__)


def display_config_section():
    """
    Displays the UI for configuration settings using Streamlit forms.
    
    This function creates a professional configuration interface with expandable sections
    for different parts of the application configuration.
    
    Returns:
        None
    """
    st.header("2. Configuration")
    st.write("Configure scraping and enrichment parameters.")

    # Initialize config structure if needed
    if "config" not in st.session_state:
        st.session_state["config"] = {}
        
    # Load existing config values from session state
    config = st.session_state["config"]
    
    # Create the main configuration form
    with st.form(key="config_form"):
        # Create tabs for different configuration sections
        tab1, tab2, tab3, tab4 = st.tabs([
            "General Settings", 
            "Web Crawling", 
            "Data Extraction",
            "Advanced Settings"
        ])
        
        # --- TAB 1: GENERAL SETTINGS ---
        with tab1:
            st.subheader("General Parameters")
            
            # Get current values from session state
            prev_category = config.get("category", "maschinenbauer")
            prev_log_level = config.get("log_level", "INFO")
            prev_skip_llm_validation = config.get("skip_llm_validation", False)
            
            # Input fields
            category_input = st.text_input(
                "Category", 
                value=prev_category,
                help="Category of companies (e.g., maschinenbauer, autozulieferer)"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                log_level_input = st.selectbox(
                    "Log Level",
                    options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(prev_log_level)
                    if prev_log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
                    else 1
                )
            
            with col2:
                skip_llm_validation_input = st.checkbox(
                    "Skip LLM Validation", 
                    value=prev_skip_llm_validation,
                    help="Skip validation steps that use LLM"
                )
            
            st.divider()
            
            # Integration settings
            st.subheader("Integration Settings")
            
            # Get current values from session state integration settings
            integration_config = config.get("integration", {})
            prev_data_enrichment = integration_config.get("data_enrichment", True)
            prev_result_filename_template = integration_config.get(
                "result_filename_template", 
                "final_export_{category}_{timestamp}.csv"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                data_enrichment_input = st.checkbox(
                    "Enable Data Enrichment", 
                    value=prev_data_enrichment,
                    help="Process data through enrichment pipeline"
                )
            
            with col2:
                result_filename_template_input = st.text_input(
                    "Result Filename Template", 
                    value=prev_result_filename_template,
                    help="Template for result files. Use {category} and {timestamp} as placeholders."
                )

        # --- TAB 2: WEB CRAWLING ---
        with tab2:
            st.subheader("Web Crawling Configuration")
            
            # Get current crawling values
            prev_depth = config.get("depth", 2)
            
            # Web crawler settings from config.webcrawl.crawler
            webcrawl_config = config.get("webcrawl", {})
            crawler_config = webcrawl_config.get("crawler", {})
            
            prev_browser_type = crawler_config.get("browser_type", "chromium")
            prev_headless = crawler_config.get("headless", True)
            prev_verbose = crawler_config.get("verbose", False)
            prev_check_robots = crawler_config.get("check_robots_txt", True)
            
            # UI elements
            depth_input = st.slider(
                "Crawling Depth", 
                min_value=1, 
                max_value=5, 
                value=prev_depth,
                help="Number of links to follow from the homepage"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                browser_type_input = st.selectbox(
                    "Browser Type",
                    options=["chromium", "firefox", "webkit"],
                    index=["chromium", "firefox", "webkit"].index(prev_browser_type)
                    if prev_browser_type in ["chromium", "firefox", "webkit"]
                    else 0
                )
            
            with col2:
                check_robots_input = st.checkbox(
                    "Respect robots.txt", 
                    value=prev_check_robots,
                    help="Check and follow robots.txt directives"
                )
            
            col1, col2 = st.columns(2)
            with col1:
                headless_input = st.checkbox(
                    "Headless Mode", 
                    value=prev_headless,
                    help="Run browser in headless mode"
                )
            
            with col2:
                verbose_input = st.checkbox(
                    "Verbose Logging", 
                    value=prev_verbose,
                    help="Enable detailed crawler logs"
                )
                
            # Dispatcher settings
            st.divider()
            st.subheader("Crawler Dispatcher Settings")
            
            dispatcher_config = webcrawl_config.get("dispatcher", {})
            prev_memory_threshold = dispatcher_config.get("memory_threshold_percent", 80.0)
            prev_check_interval = dispatcher_config.get("check_interval", 0.5)
            
            col1, col2 = st.columns(2)
            with col1:
                memory_threshold_input = st.slider(
                    "Memory Threshold (%)", 
                    min_value=50.0, 
                    max_value=95.0, 
                    value=float(prev_memory_threshold),
                    step=5.0,
                    help="Memory threshold for crawler throttling"
                )
            
            with col2:
                check_interval_input = st.number_input(
                    "Check Interval (seconds)", 
                    min_value=0.1, 
                    max_value=5.0, 
                    value=float(prev_check_interval),
                    step=0.1,
                    help="Interval between memory checks"
                )

        # --- TAB 3: DATA EXTRACTION ---
        with tab3:
            st.subheader("LLM Configuration")
            
            # Get current values
            prev_llm = config.get("llm_provider", "OpenAI")
            prev_api_key = config.get("api_key", "")
            
            # LLM extraction settings
            llm_extraction_config = webcrawl_config.get("llm_extraction", {})
            # Remove unused variable per linting error
            # prev_provider = llm_extraction_config.get("provider", "OpenAI")
            prev_chunk_threshold = llm_extraction_config.get("chunk_token_threshold", 4096)
            prev_overlap_rate = llm_extraction_config.get("overlap_rate", 0.1)
            prev_temperature = llm_extraction_config.get("temperature", 0.5)
            prev_max_tokens = llm_extraction_config.get("max_tokens", 800)
            
            # UI elements
            col1, col2 = st.columns(2)
            with col1:
                llm_provider_input = st.selectbox(
                    "LLM Provider",
                    ["OpenAI", "Anthropic", "Gemini", "Bedrock", "Mock"],
                    index=["OpenAI", "Anthropic", "Gemini", "Bedrock", "Mock"].index(prev_llm)
                    if prev_llm in ["OpenAI", "Anthropic", "Gemini", "Bedrock", "Mock"]
                    else 0,
                )
            
            with col2:
                api_key_input = st.text_input(
                    "API Key", value=prev_api_key, type="password"
                )
            
            col1, col2 = st.columns(2)
            with col1:
                temperature_input = st.slider(
                    "Temperature", 
                    min_value=0.0, 
                    max_value=1.0, 
                    value=float(prev_temperature),
                    step=0.1,
                    help="Controls randomness in LLM responses"
                )
            
            with col2:
                max_tokens_input = st.slider(
                    "Max Tokens", 
                    min_value=100, 
                    max_value=2000, 
                    value=int(prev_max_tokens),
                    step=100,
                    help="Maximum tokens in LLM response"
                )
            
            col1, col2 = st.columns(2)
            with col1:
                chunk_threshold_input = st.number_input(
                    "Chunk Token Threshold", 
                    min_value=1000, 
                    max_value=8000, 
                    value=int(prev_chunk_threshold),
                    step=100,
                    help="Token threshold for chunking text"
                )
            
            with col2:
                overlap_rate_input = st.slider(
                    "Chunk Overlap Rate", 
                    min_value=0.0, 
                    max_value=0.5, 
                    value=float(prev_overlap_rate),
                    step=0.05,
                    help="Overlap between text chunks"
                )
                
            # Extracting Machine settings
            st.divider()
            st.subheader("Machine Extraction Settings")
            
            extracting_machine_config = config.get("extracting_machine", {})
            prev_search_word = extracting_machine_config.get("search_word", "technische Anlagen")
            prev_max_retries = extracting_machine_config.get("max_retries", 5)
            prev_top_n = extracting_machine_config.get("top_n_machines", 1)
            
            col1, col2 = st.columns(2)
            with col1:
                search_word_input = st.text_input(
                    "Search Term", 
                    value=prev_search_word,
                    help="Key term for machine extraction"
                )
            
            with col2:
                top_n_input = st.number_input(
                    "Top N Machines", 
                    min_value=1, 
                    max_value=20, 
                    value=int(prev_top_n),
                    help="Number of machines to extract"
                )
            
            max_retries_input = st.slider(
                "Max Retries", 
                min_value=1, 
                max_value=10, 
                value=int(prev_max_retries),
                help="Maximum retry attempts"
            )

        # --- TAB 4: ADVANCED SETTINGS ---
        with tab4:
            st.subheader("Advanced Run Configuration")
            
            # Run config settings
            run_config = webcrawl_config.get("run_config", {})
            prev_cache_mode = run_config.get("cache_mode", "BYPASS")
            prev_css_selector = run_config.get("css_selector", "main")
            prev_word_count_threshold = run_config.get("word_count_threshold", 15)
            
            col1, col2 = st.columns(2)
            with col1:
                cache_mode_input = st.selectbox(
                    "Cache Mode",
                    options=["BYPASS", "USE_CACHE", "REFRESH"],
                    index=["BYPASS", "USE_CACHE", "REFRESH"].index(prev_cache_mode)
                    if prev_cache_mode in ["BYPASS", "USE_CACHE", "REFRESH"]
                    else 0,
                    help="Cache strategy for web requests"
                )
            
            with col2:
                css_selector_input = st.text_input(
                    "CSS Selector", 
                    value=prev_css_selector,
                    help="CSS selector for content extraction"
                )
            
            word_count_threshold_input = st.number_input(
                "Word Count Threshold", 
                min_value=5, 
                max_value=50, 
                value=int(prev_word_count_threshold),
                help="Minimum word count for valid content"
            )
            
            st.divider()
            st.subheader("Cleanup Settings")
            
            # General cleanup settings
            prev_cleanup = config.get("cleanup_intermediate_outputs", False)
            prev_verbose_logging = config.get("verbose_logging", False)
            
            col1, col2 = st.columns(2)
            with col1:
                cleanup_input = st.checkbox(
                    "Cleanup Intermediate Outputs", 
                    value=prev_cleanup,
                    help="Remove temporary files after processing"
                )
            
            with col2:
                verbose_logging_input = st.checkbox(
                    "Verbose Logging", 
                    value=prev_verbose_logging,
                    help="Enable detailed logging"
                )

        # Submit button for the form
        submitted = st.form_submit_button("Save Configuration", use_container_width=True)

    # This block executes only when the 'Save Configuration' button is pressed
    if submitted:
        # Update main configuration values
        config["category"] = category_input
        config["log_level"] = log_level_input
        config["skip_llm_validation"] = skip_llm_validation_input
        config["depth"] = depth_input
        config["llm_provider"] = llm_provider_input
        config["api_key"] = api_key_input
        
        # Update integration config
        if "integration" not in config:
            config["integration"] = {}
        config["integration"]["data_enrichment"] = data_enrichment_input
        config["integration"]["result_filename_template"] = result_filename_template_input
        
        # Update webcrawl config
        if "webcrawl" not in config:
            config["webcrawl"] = {}
        
        # Update webcrawl.crawler config
        if "crawler" not in config["webcrawl"]:
            config["webcrawl"]["crawler"] = {}
        config["webcrawl"]["crawler"]["browser_type"] = browser_type_input
        config["webcrawl"]["crawler"]["headless"] = headless_input
        config["webcrawl"]["crawler"]["verbose"] = verbose_input
        config["webcrawl"]["crawler"]["check_robots_txt"] = check_robots_input
        
        # Update webcrawl.dispatcher config
        if "dispatcher" not in config["webcrawl"]:
            config["webcrawl"]["dispatcher"] = {}
        config["webcrawl"]["dispatcher"]["memory_threshold_percent"] = memory_threshold_input
        config["webcrawl"]["dispatcher"]["check_interval"] = check_interval_input
        
        # Update webcrawl.run_config
        if "run_config" not in config["webcrawl"]:
            config["webcrawl"]["run_config"] = {}
        config["webcrawl"]["run_config"]["cache_mode"] = cache_mode_input
        config["webcrawl"]["run_config"]["css_selector"] = css_selector_input
        config["webcrawl"]["run_config"]["word_count_threshold"] = word_count_threshold_input
        
        # Update webcrawl.llm_extraction
        if "llm_extraction" not in config["webcrawl"]:
            config["webcrawl"]["llm_extraction"] = {}
        config["webcrawl"]["llm_extraction"]["provider"] = llm_provider_input
        config["webcrawl"]["llm_extraction"]["chunk_token_threshold"] = chunk_threshold_input
        config["webcrawl"]["llm_extraction"]["overlap_rate"] = overlap_rate_input
        config["webcrawl"]["llm_extraction"]["temperature"] = temperature_input
        config["webcrawl"]["llm_extraction"]["max_tokens"] = max_tokens_input
        
        # Update extracting_machine config
        if "extracting_machine" not in config:
            config["extracting_machine"] = {}
        config["extracting_machine"]["search_word"] = search_word_input
        config["extracting_machine"]["max_retries"] = max_retries_input
        config["extracting_machine"]["top_n_machines"] = top_n_input
        
        # Update general cleanup settings
        config["cleanup_intermediate_outputs"] = cleanup_input
        config["verbose_logging"] = verbose_logging_input
        
        # Update session state with the modified config
        st.session_state["config"] = config

        # Log the configuration update
        logger.info(
            f"Configuration updated via form: Category={config.get('category')}, "
            f"Depth={config.get('depth')}, LLM={config.get('llm_provider')}"
        )
        st.success("Configuration saved successfully!")
        
        # Display a summary of the key settings
        with st.expander("Configuration Summary", expanded=True):
            st.write(f"**Category:** {config.get('category')}")
            st.write(f"**Crawling Depth:** {config.get('depth')}")
            st.write(f"**LLM Provider:** {config.get('llm_provider')}")
            if config.get("api_key"):
                st.write("**API Key:** [Set]")
            else:
                st.write("**API Key:** [Not Set]")
