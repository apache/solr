#!/bin/bash
# Script to remove precisionStep property from all XML files
# Usage: ./remove_precision_step.sh [--dry-run]

set -e

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN MODE ==="
    echo "No files will be modified."
    echo ""
fi

# Get the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find all XML files
XML_FILES=$(find "$SCRIPT_DIR" -type f -name "*.xml")

if [[ -z "$XML_FILES" ]]; then
    echo "No XML files found."
    exit 0
fi

FILE_COUNT=$(echo "$XML_FILES" | wc -l | tr -d ' ')
echo "Found $FILE_COUNT XML files."
echo ""

MODIFIED_COUNT=0

while IFS= read -r file; do
    # Check if file contains precisionStep
    if grep -q 'precisionStep="' "$file"; then
        if [[ "$DRY_RUN" == true ]]; then
            echo "[DRY RUN] Would modify: $file"
            # Show what would be removed
            grep -o ' precisionStep="[^"]*"' "$file" | sort -u | sed 's/^/  Found: /'
        else
            # Use sed to remove precisionStep attribute
            # Works on both macOS (BSD sed) and Linux (GNU sed)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS requires empty string after -i
                sed -i '' 's/ precisionStep="[^"]*"//g' "$file"
            else
                # Linux
                sed -i 's/ precisionStep="[^"]*"//g' "$file"
            fi
            echo "Modified: $file"
        fi
        ((MODIFIED_COUNT++))
    fi
done <<< "$XML_FILES"

echo ""
if [[ "$DRY_RUN" == true ]]; then
    echo "Would modify $MODIFIED_COUNT out of $FILE_COUNT files."
    echo ""
    echo "Run without --dry-run to actually modify files."
else
    echo "Modified $MODIFIED_COUNT out of $FILE_COUNT files."
fi