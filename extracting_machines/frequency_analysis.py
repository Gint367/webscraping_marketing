import json
from pathlib import Path
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import re

def read_json_files(directory):
    json_files = []
    for file in Path(directory).glob('*.json'):
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                json_files.append(data)
            except json.JSONDecodeError:
                print(f"Error reading {file}")
    return json_files

def clean_text(text):
    # Clean and normalize text
    text = text.lower()
    # Replace special characters but keep German umlauts and numbers
    text = re.sub(r'[^a-zäöüß0-9\s]', ' ', text)
    return text.strip()

def extract_headers(data):
    headers_text = []
    for table in data:
        if 'matching_rows' not in table:
            continue
        
        for row in table['matching_rows']:
            # Extract text from all header levels
            for key in row:
                if key.startswith('header'):
                    # Clean and join headers into single strings
                    for header_row in row[key]:
                        if isinstance(header_row, (list, tuple)):
                            header_text = ' '.join(str(h) for h in header_row)
                        else:
                            header_text = str(header_row)
                        cleaned_text = clean_text(header_text)
                        if cleaned_text:  # Only add non-empty strings
                            headers_text.append(cleaned_text)
    return headers_text

def analyze_with_bow(texts):
    if not texts:
        print("Warning: No texts to analyze!")
        return pd.DataFrame()
    
    # Initialize the CountVectorizer with minimal filtering
    vectorizer = CountVectorizer(
        stop_words=['und', 'der', 'die', 'das'],  # Reduced stop words
        token_pattern=r'[a-zäöüß0-9]+',  # Include German chars and numbers
        min_df=1,  # Include all terms
        max_features=100
    )
    
    try:
        # Create bag of words
        X = vectorizer.fit_transform(texts)
        
        # Get feature names (words)
        words = vectorizer.get_feature_names_out()
        
        # Create frequency distribution
        word_freq = pd.DataFrame(
            X.sum(axis=0).T,
            index=words,
            columns=['frequency']
        ).sort_values('frequency', ascending=False)
        
        return word_freq
    
    except ValueError as e:
        print(f"Error in analysis: {e}")
        print("Sample of texts being processed:", texts[:5])
        return pd.DataFrame()

def main():
    directory = "bundesanzeiger_local_data_output_old"
    all_headers = []
    
    # Read all JSON files
    json_files = read_json_files(directory)
    
    # Process each file
    for data in json_files:
        headers = extract_headers(data)
        all_headers.extend(headers)
    
    if not all_headers:
        print("No headers found in the files!")
        return
    
    print(f"Total number of headers found: {len(all_headers)}")
    print("Sample headers:", all_headers[:3])
    
    # Analyze using Bag of Words
    word_frequencies = analyze_with_bow(all_headers)
    
    if not word_frequencies.empty:
        print("\nMost common words in headers (Bag of Words analysis):")
        print("-" * 60)
        print(word_frequencies.head(30))
    else:
        print("Analysis produced no results!")

if __name__ == "__main__":
    main()
