#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
import yaml
from pathlib import Path

def print_usage():
    print("Usage: parseContributorsFromChanges.py <version>")
    print("  <version>: Version number (e.g., 9.10.0)")
    print("\nThis script parses all YAML files in changelog/v<version>/ and extracts unique authors.")
    print("Output is a comma-separated list of authors sorted by name.")
    sys.exit(1)

if len(sys.argv) < 2:
    print("Error: Missing required argument <version>")
    print_usage()

version = sys.argv[1]
changelog_dir = Path(f"changelog/v{version}")

if not changelog_dir.exists():
    print(f"Error: Directory '{changelog_dir}' does not exist")
    sys.exit(1)

# Collect all unique authors
authors = set()

# Process all .yml and .yaml files in the changelog directory
yaml_files = list(changelog_dir.glob("*.yml")) + list(changelog_dir.glob("*.yaml"))

if not yaml_files:
    print(f"Warning: No YAML files found in {changelog_dir}")
    sys.exit(0)

for yaml_file in sorted(yaml_files):
    try:
        with open(yaml_file, 'r') as f:
            data = yaml.safe_load(f)
            if data and 'authors' in data:
                author_list = data['authors']
                if isinstance(author_list, list):
                    for author_entry in author_list:
                        if isinstance(author_entry, dict) and 'name' in author_entry:
                            author_name = author_entry['name'].strip()
                            # Filter out solrbot
                            if author_name.lower() != 'solrbot':
                                authors.add(author_name)
    except Exception as e:
        print(f"Warning: Error parsing {yaml_file}: {e}", file=sys.stderr)

# Sort authors by name
sorted_authors = sorted(list(authors))

# Print contributors
for author in sorted_authors:
    print(author)

print('\nThanks to all contributors!: ')
print(', '.join(sorted_authors))