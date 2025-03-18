#!/bin/bash

# Script to count the number of folders inside each bundesanzeiger_local_* folder
# without counting their subfolders

echo "Counting folders inside bundesanzeiger_local_* directories (excluding nested subfolders)..."
echo "----------------------------------------"

# Find all directories in current working directory that start with bundesanzeiger_local_
for dir in bundesanzeiger_local_*; do
    # Check if it's a directory
    if [ -d "$dir" ]; then
        # Count direct subfolders (maxdepth 1 to avoid nested folders)
        folder_count=$(find "$dir" -maxdepth 1 -type d | wc -l)
        
        # Subtract 1 to exclude the parent directory itself from the count
        folder_count=$((folder_count - 1))
        
        # Display folder count
        echo "$dir: $folder_count folders (direct subfolders only)"
    fi
done

echo "----------------------------------------"
echo "Each value above represents the number of direct subfolders in each folder (nested subfolders excluded)"
