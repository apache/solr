#!/usr/bin/env python3
"""
Script to remove precisionStep property from all XML files in the project.
This handles precisionStep="0", precisionStep="8", or any other numeric value.

Usage: python remove_precision_step.py [--dry-run]
"""

import os
import re
import sys
from pathlib import Path

def remove_precision_step(content):
    """
    Remove precisionStep attribute from XML content.
    Handles various formats with different spacing.
    """
    # Pattern matches precisionStep="<any_value>" with optional surrounding whitespace
    # This will match:
    # - precisionStep="0"
    # - precisionStep="8"
    # - precisionStep="4"
    # etc.
    pattern = r'\s+precisionStep="[^"]*"'
    
    modified_content = re.sub(pattern, '', content)
    return modified_content

def process_file(file_path, dry_run=False):
    """
    Process a single XML file to remove precisionStep attributes.
    
    Returns True if file was modified, False otherwise.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        modified_content = remove_precision_step(original_content)
        
        if original_content != modified_content:
            if dry_run:
                print(f"[DRY RUN] Would modify: {file_path}")
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
                print(f"Modified: {file_path}")
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)
        return False

def find_xml_files(root_dir):
    """
    Recursively find all XML files in the directory.
    """
    xml_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    return xml_files

def main():
    """
    Main function to process all XML files.
    """
    dry_run = '--dry-run' in sys.argv
    
    # Get the script's directory
    script_dir = Path(__file__).parent
    
    if dry_run:
        print("=== DRY RUN MODE ===")
        print("No files will be modified.\n")
    
    # Find all XML files
    xml_files = find_xml_files(script_dir)
    
    if not xml_files:
        print("No XML files found.")
        return
    
    print(f"Found {len(xml_files)} XML files.\n")
    
    modified_count = 0
    
    for xml_file in xml_files:
        if process_file(xml_file, dry_run):
            modified_count += 1
    
    print(f"\n{'Would modify' if dry_run else 'Modified'} {modified_count} out of {len(xml_files)} files.")
    
    if dry_run:
        print("\nRun without --dry-run to actually modify files.")

if __name__ == '__main__':
    main()