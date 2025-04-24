import csv
import json
import os
import re
import string
from collections import Counter

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download necessary NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('punkt_tab')


def extract_data_from_markdown(file_path):
    """Extract main URL and content text from a markdown file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

        # Extract main URL
        main_url_match = re.search(r'Main URL: (https?://[^\s]+)', content)
        main_url = main_url_match.group(1) if main_url_match else None

        # Extract company name
        company_name_match = re.search(r'Company Name: (.+)', content)
        company_name = company_name_match.group(1) if company_name_match else None

        # Extract all content (simplified approach - getting all text)
        all_text = re.sub(r'\[[^\]]*\]\([^)]*\)', ' ', content)  # Remove markdown links
        all_text = re.sub(r'#+\s', ' ', all_text)  # Remove headers
        all_text = re.sub(r'!\[.*?\]\(.*?\)', ' ', all_text)  # Remove images
        all_text = re.sub(r'```.*?```', ' ', all_text, flags=re.DOTALL)  # Remove code blocks
        all_text = re.sub(r'\*|\||-|>|#', ' ', all_text)  # Remove special markdown chars
        all_text = re.sub(r'\s+', ' ', all_text).strip()  # Normalize whitespace

        return {
            'company_name': company_name,
            'main_url': main_url,
            'text': all_text
        }


def extract_keywords(text, min_length=4, top_n=50):
    """Extract keywords from text, removing stopwords and short words."""
    # Tokenize and clean the text
    tokens = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english') + stopwords.words('german'))

    # Remove punctuation, stopwords, and short words
    words = [word for word in tokens
             if word not in stop_words
             and word not in string.punctuation
             and len(word) >= min_length
             and not word.isdigit()]

    # Count word frequencies
    word_freq = Counter(words)

    # Return the most common words
    return word_freq.most_common(top_n)


def process_markdown_files(directory):
    """Process all markdown files in the given directory."""
    all_data = []
    keyword_analysis = {}

    for filename in os.listdir(directory):
        if filename.endswith('.md'):
            file_path = os.path.join(directory, filename)
            data = extract_data_from_markdown(file_path)

            if data['main_url']:
                all_data.append(data)

                # Analyze keywords for this company
                keywords = extract_keywords(data['text'])
                keyword_analysis[data['main_url']] = {
                    'company_name': data['company_name'],
                    'keywords': keywords
                }

    return all_data, keyword_analysis


def save_to_csv(data, output_file):
    """Save extracted data to CSV file."""
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['company_name', 'main_url', 'text']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in data:
            writer.writerow(item)


def save_to_json(data, output_file):
    """Save extracted data to JSON file."""
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=2)


def save_keyword_analysis(keyword_analysis, output_file):
    """Save keyword analysis results to JSON file."""
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(keyword_analysis, jsonfile, ensure_ascii=False, indent=2)


def main():
    # Directory containing markdown files
    markdown_dir = 'domain_content_aggregated_stahlverarbeitung'

    # Process all markdown files
    data, keyword_analysis = process_markdown_files(markdown_dir)

    # Create output directory if it doesn't exist
    output_dir = '/home/novoai/Documents/scraper/output_keyword analysis'
    os.makedirs(output_dir, exist_ok=True)

    # Save extracted data
    save_to_csv(data, os.path.join(output_dir, 'extracted_data.csv'))
    save_to_json(data, os.path.join(output_dir, 'extracted_data.json'))

    # Save keyword analysis
    save_keyword_analysis(keyword_analysis, os.path.join(output_dir, 'keyword_analysis.json'))

    # Print summary
    print(f"Processed {len(data)} company files")
    print(f"Data saved to {output_dir}/extracted_data.csv and {output_dir}/extracted_data.json")
    print(f"Keyword analysis saved to {output_dir}/keyword_analysis.json")

if __name__ == "__main__":
    main()
