#!/bin/bash

# Define source and destination folders
source_folder="domain_content_pruned"
destination_folder="domain_content_batch"

# Check if destination folder exists, create if not
mkdir -p "$destination_folder"

# Find the 50 oldest files in the source folder and copy them to the destination folder
find "$source_folder" -type f | sort | head -n 50 | while read file; do
  # Extract the file name
  filename=$(basename "$file")
  
  # Check if the file already exists in the destination folder
  if [ ! -e "$destination_folder/$filename" ]; then
    # If not, copy the file to the destination folder
    cp "$file" "$destination_folder/"
    echo "Copied: $filename"
  else
    echo "Skipped (already exists): $filename"
  fi
done
