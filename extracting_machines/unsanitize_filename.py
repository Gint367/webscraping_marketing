#!/usr/bin/env python3
import re

def unsanitize_filename(sanitized_name: str) -> str:
    """
    Attempts to reverse the sanitization process to recover the original string.
    
    Args:
        sanitized_name: A filename that was sanitized using sanitize_filename()
    
    Returns:
        A string that approximates the original name before sanitization
    """
    # Replace underscores with spaces (but not in German Umlaut patterns)
    name = sanitized_name
    
    # First handle the special German Umlaut cases
    # We need to be careful to avoid false positives in normal text
    
    # Improved approach for German Umlaut replacements
    # Process known German words and common patterns first
    german_replacements = [
        ("Goessling", "Gössling"),
        ("Mueller", "Müller"),
        ("Buehler", "Bühler"),
        ("Koenigsberger", "Königsberger"),
        ("Gebrueder", "Gebrüder"),
    ]
    
    for german_word, replacement in german_replacements:
        if german_word in name:
            name = name.replace(german_word, replacement)
    
    # More general pattern replacements
    # Be careful with the order - some need to be processed before others
    patterns = [
        # Common German patterns with word boundaries
        (r'\b([A-Za-z]*?)ae([A-Za-z]*?)\b', r'\1ä\2'),
        (r'\b([A-Za-z]*?)oe([A-Za-z]*?)\b', r'\1ö\2'),
        (r'\b([A-Za-z]*?)ue([A-Za-z]*?)\b', r'\1ü\2'),
        (r'\b([A-Za-z]*?)Ae([A-Za-z]*?)\b', r'\1Ä\2'),
        (r'\b([A-Za-z]*?)Oe([A-Za-z]*?)\b', r'\1Ö\2'),
        (r'\b([A-Za-z]*?)Ue([A-Za-z]*?)\b', r'\1Ü\2'),
        
        # Look for patterns without requiring word boundaries
        (r'([A-Z][a-z]*)oe([a-z]+)', r'\1ö\2'),
        (r'([A-Z][a-z]*)ae([a-z]+)', r'\1ä\2'),
        (r'([A-Z][a-z]*)ue([a-z]+)', r'\1ü\2'),
    ]
    
    # Apply the patterns
    for pattern, replacement in patterns:
        name = re.sub(pattern, replacement, name)
    
    # Special case for sharp s (ss -> ß) - only in certain positions to avoid false positives
    # name = re.sub(r'([a-zäöü])ss([a-zäöü])', r'\1ß\2', name)
    
    # Replace underscores with spaces
    name = name.replace("_", " ")
    
    # Replace "and" with "&" but only where it's likely to be a standalone word
    name = re.sub(r'\band\b', '&', name)
    
    return name

if __name__ == "__main__":
    # Test cases
    test_cases = [
        "Mayer_GmbH_and_Co._KG",
        "Gebrueder_Mueller_AG",
        "Koenigsberger_Strasse",
        "A_and_W_Maschinenbau_GmbH",
        "Buehler_GmbH",
        "Form_+_Test_Seidner_and_Co.",
        "Hans_and_Jos._Kronenberg_GmbH",
        "Goessling_Verwaltungs_-_GmbH"
    ]
    
    for test in test_cases:
        unsanitized = unsanitize_filename(test)
        print(f"Sanitized:   {test}")
        print(f"Unsanitized: {unsanitized}")
        print("-" * 50)
