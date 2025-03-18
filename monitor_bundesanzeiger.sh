#!/bin/bash

# Script to calculate the size of each bundesanzeiger_local_* folder
# without including their subfolder contents

echo "Calculating size of bundesanzeiger_local_* folders (excluding subfolder contents)..."
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
        echo "$dir: $folder_count folders"
    fi
done

echo "----------------------------------------"
