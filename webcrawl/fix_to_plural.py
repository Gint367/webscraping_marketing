import pandas as pd
import numpy as np
from collections import Counter

def pluralize(word, stats_dict, unchanged_words):
    if pd.isna(word) or word == "":
        stats_dict["empty"] += 1
        return ""
    elif word.endswith("ung"):
        stats_dict["ung"] += 1
        return word + "en"
    elif word.endswith("eit") or word.endswith("ion"):
        stats_dict["eit_ion"] += 1
        return word + "en"
    elif word.endswith("e"):
        stats_dict["e"] += 1
        return word + "n"
    elif word.endswith("sprozess"):
        stats_dict["sprozess"] += 1
        return word + "e"
    elif word.endswith("prozess"):
        stats_dict["prozess"] += 1
        return word[:-7] + "prozesse"
    elif word.endswith("bearbeitung"):
        stats_dict["bearbeitung"] += 1
        return word + "en"
    elif word.endswith("arbeit"):
        stats_dict["arbeit"] += 1
        return word + "en"
    elif word.endswith("fertigung"):
        stats_dict["fertigung"] += 1
        return word + "en"
    elif word.endswith("verfahren"):
        stats_dict["verfahren"] += 1
        return word + ""  # Already plural in German
    elif word.endswith("guss"):
        stats_dict["guss"] += 1
        return word[:-4] + "güsse"
    elif word.endswith("schweißen"):
        stats_dict["schweissen"] += 1
        return word  # Already plural form in this context
    elif word.endswith("metallabscheidung"):
        stats_dict["metallabscheidung"] += 1
        return word + "en"
    elif word.endswith("druck"):
        stats_dict["druck"] += 1
        return word[:-5] + "drucke"
    elif word.endswith("technik"):
        stats_dict["technik"] += 1
        return word + "en"
    else:
        stats_dict["else"] += 1
        # Add to list of words that weren't changed
        if isinstance(word, str):
            unchanged_words.append(word)
        return word

def main():
    # Path to the CSV file
    input_file = "/home/novoai/Documents/scraper/consolidated_output/llm_extracted_data.csv"
    output_file = "/home/novoai/Documents/scraper/consolidated_output/llm_extracted_data_pluralized.csv"
    
    # Read the CSV file
    print(f"Reading CSV file: {input_file}")
    df = pd.read_csv(input_file)
    
    # Process columns Prozess_1, Prozess_2, and Prozess_3
    process_columns = ["Prozess_1", "Prozess_2", "Prozess_3"]
    
    # Initialize statistics dictionary
    stats = {
        "total": 0,
        "empty": 0,
        "ung": 0,
        "eit_ion": 0,
        "e": 0,
        "sprozess": 0,
        "prozess": 0,
        "bearbeitung": 0,
        "arbeit": 0,
        "fertigung": 0,
        "verfahren": 0,
        "guss": 0,
        "schweissen": 0,
        "metallabscheidung": 0,
        "druck": 0,
        "technik": 0,
        "else": 0
    }
    
    # List to store words that don't match any rule
    unchanged_words = []
    
    for col in process_columns:
        print(f"Pluralizing {col} column...")
        df[col] = df[col].apply(lambda x: pluralize(x, stats, unchanged_words))
        stats["total"] += len(df[col])
    
    # Write the updated DataFrame to a new CSV file with UTF-8-SIG encoding
    print(f"Writing updated data to: {output_file} with UTF-8-SIG encoding")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print("Pluralization completed successfully!")

    # Print some examples of transformed values
    print("\nExamples of pluralized process terms:")
    for col in process_columns:
        sample = df[[col]].head(10)
        print(f"\n{col} examples:")
        print(sample)
    
    # Print statistics
    print("\nPluralizing Statistics:")
    print(f"Total cells processed: {stats['total']}")
    print(f"Empty cells: {stats['empty']} ({stats['empty']/stats['total']:.2%})")
    
    # Cells that matched specific rules
    matched_rules = stats['total'] - stats['empty'] - stats['else']
    print(f"Cells that matched specific rules: {matched_rules} ({matched_rules/stats['total']:.2%})")
    print(f"Cells that hit the else case: {stats['else']} ({stats['else']/stats['total']:.2%})")
    
    # Detailed statistics for each rule
    print("\nDetailed rule statistics:")
    rules = [k for k in stats.keys() if k not in ["total", "empty", "else"]]
    for rule in rules:
        if stats[rule] > 0:
            print(f"  - {rule}: {stats[rule]} cells ({stats[rule]/stats['total']:.2%})")
    
    # Show words that didn't get changed (if any)
    if unchanged_words:
        print("\nUnchanged words (hit the else case):")
        word_counts = Counter(unchanged_words)
        print(f"Unique unchanged words: {len(word_counts)}")
        
        # Show top 20 most common unchanged words
        print("\nTop 20 most common unchanged words:")
        for word, count in word_counts.most_common(20):
            print(f"  - '{word}': {count} occurrences")
        
        # Write all unchanged words to a file for further analysis
        unchanged_file = "/home/novoai/Documents/scraper/consolidated_output/unchanged_words.txt"
        with open(unchanged_file, "w", encoding='utf-8-sig') as f:
            for word, count in word_counts.most_common():
                f.write(f"{word}: {count}\n")
        print(f"\nAll unchanged words written to: {unchanged_file} with UTF-8-SIG encoding")

if __name__ == "__main__":
    main()
