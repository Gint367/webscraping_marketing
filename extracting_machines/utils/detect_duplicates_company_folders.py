#!/usr/bin/env python3
import argparse
import csv
import os
import re
from collections import defaultdict

from extracting_machines.unsanitize_filename import unsanitize_filename


def check_duplicate_folders(base_dir):
    """
    Check for folders with single-digit numerical suffixes (like _2, _3) that may indicate duplicates.
    Will ignore larger numbers like _2007.

    Args:
        base_dir: Base directory to start the search from

    Returns:
        Dictionary of parent folders containing potential duplicates and their duplicate subfolders
    """
    # Regex pattern to match folder names ending with _X where X is a single digit (0-9)
    pattern = re.compile(r'_([0-9])$')

    # Dictionary to store affected parent folders and their duplicate subfolders
    affected_parents = defaultdict(list)

    # Check if base directory exists
    if not os.path.exists(base_dir):
        print(f"Error: Directory '{base_dir}' not found.")
        return affected_parents

    # Walk through the directory structure
    print(f"Checking for duplicate folders in: {base_dir}")
    print("-" * 80)

    for parent_dir, subdirs, _ in os.walk(base_dir):
        # Skip the base directory itself
        if parent_dir == base_dir:
            continue

        # Get the parent folder name
        os.path.basename(parent_dir)

        # Check each subdirectory for the duplicate pattern
        for subdir in subdirs:
            match = pattern.search(subdir)
            if match:
                suffix_num = match.group(1)
                # Store the parent path and the duplicate subfolder
                affected_parents[parent_dir].append((subdir, suffix_num))

    return affected_parents


def print_stats(affected_parents):
    """
    Print statistics and details about duplicate folders

    Args:
        affected_parents: Dictionary of parent folders containing potential duplicates
    """
    total_affected_parents = len(affected_parents)
    total_duplicate_folders = sum(len(folders) for folders in affected_parents.values())

    # Count parents by number of duplicates
    duplicate_counts = get_duplicate_count_statistics(affected_parents)

    print("\nResults Summary:")
    print("===============")
    print(f"Total parent folders affected: {total_affected_parents}")
    print(f"Total potential duplicate folders: {total_duplicate_folders}")
    print(f"Parent folders with exactly 1 duplicate: {duplicate_counts[1]}")
    print(f"Parent folders with exactly 2 duplicates: {duplicate_counts[2]}")
    print(f"Parent folders with exactly 3 duplicates: {duplicate_counts[3]}")
    print(f"Parent folders with more than 3 duplicates: {duplicate_counts['more_than_3']}")

    if total_affected_parents > 0:
        print("\nAffected Parent Folders:")
        print("=======================")

        for i, (parent, duplicates) in enumerate(affected_parents.items(), 1):
            parent_name = os.path.basename(parent)
            duplicate_info = ", ".join([f"{d[0]} (suffix: _{d[1]})" for d in duplicates])
            print(f"{i}. {parent_name}")
            print(f"   Path: {parent}")
            print(f"   Duplicates: {duplicate_info}")
            print()


def get_duplicate_count_statistics(affected_parents):
    """
    Categorize parent folders by the number of duplicates they have

    Args:
        affected_parents: Dictionary of parent folders containing potential duplicates

    Returns:
        Dictionary with counts of parent folders having exactly 1, 2, 3, or more than 3 duplicates
    """
    stats = {
        1: 0,  # Exactly 1 duplicate
        2: 0,  # Exactly 2 duplicates
        3: 0,  # Exactly 3 duplicates
        'more_than_3': 0  # More than 3 duplicates
    }

    for parent, duplicates in affected_parents.items():
        dup_count = len(duplicates)
        if dup_count == 1:
            stats[1] += 1
        elif dup_count == 2:
            stats[2] += 1
        elif dup_count == 3:
            stats[3] += 1
        else:
            stats['more_than_3'] += 1

    return stats


def create_duplicates_csv(affected_parents):
    """
    Create a CSV file containing just the original company names with duplicates.

    Args:
        affected_parents: Dictionary of parent folders containing potential duplicates
    """
    output_file = "duplicates_company.csv"

    with open(output_file, "w", newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write header - just one column
        csv_writer.writerow(["company name"])

        # Write each original company name
        for parent, _ in affected_parents.items():
            sanitized_company_name = os.path.basename(parent)
            original_company_name = unsanitize_filename(sanitized_company_name)

            # Write only the original company name
            csv_writer.writerow([original_company_name])

    print(f"\nOriginal company names saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Check folders for potential duplicates with single-digit numerical suffixes")
    parser.add_argument("base_dir", nargs="?", default="bundesanzeiger_local_data",
                        help="Base directory to scan (default: bundesanzeiger_local_data)")

    args = parser.parse_args()

    affected_parents = check_duplicate_folders(args.base_dir)
    print_stats(affected_parents)

    # Save results to file if duplicates were found
    if affected_parents:
        output_file = "duplicate_folders_report.txt"
        with open(output_file, "w") as f:
            # Get duplicate counts for report
            duplicate_counts = get_duplicate_count_statistics(affected_parents)

            f.write("Duplicate Folders Report\n")
            f.write("=======================\n\n")
            f.write(f"Base directory: {args.base_dir}\n")
            f.write(f"Total parent folders affected: {len(affected_parents)}\n")
            f.write(f"Total potential duplicate folders: {sum(len(folders) for folders in affected_parents.values())}\n")
            f.write(f"Parent folders with exactly 1 duplicate: {duplicate_counts[1]}\n")
            f.write(f"Parent folders with exactly 2 duplicates: {duplicate_counts[2]}\n")
            f.write(f"Parent folders with exactly 3 duplicates: {duplicate_counts[3]}\n")
            f.write(f"Parent folders with more than 3 duplicates: {duplicate_counts['more_than_3']}\n\n")

            for parent, duplicates in affected_parents.items():
                parent_name = os.path.basename(parent)
                f.write(f"Parent: {parent_name}\n")
                f.write(f"Path: {parent}\n")
                f.write("Duplicates:\n")
                for dup, suffix in duplicates:
                    f.write(f"  - {dup} (suffix: _{suffix})\n")
                f.write("\n")

        print(f"\nDetailed report saved to: {output_file}")

        # Create CSV file with unsanitized company names
        create_duplicates_csv(affected_parents)

if __name__ == "__main__":
    main()
