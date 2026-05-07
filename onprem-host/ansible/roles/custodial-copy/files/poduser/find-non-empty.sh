#!/bin/bash

# Find all IO_Metadata.xml files, sorted by modified date (newest first)
files=$(find . -type f -name "IO_Metadata.xml" -printf "%T@ %p\n" | sort -nr | awk '{print $2}')

# Loop through files and find the first non-empty one
for file in $files; do
    if [[ -s "$file" ]]; then  # Check if the file is non-empty
        first_non_empty_date=$(stat --format="%y" "$file" | cut -d' ' -f1)
        echo "First non-empty file date: $first_non_empty_date"
        exit 0
    fi
done

echo "No non-empty files found."
exit 1
