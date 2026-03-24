#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Validates changelog YAML files in changelog/unreleased/ folder.

Checks:
- File is valid YAML
- All top-level keys are valid (title, type, issues, links, important_notes, modules, authors)
- Deprecated keys (merge_requests, configurations) are not used
- Contains required 'title' field (non-empty string)
- Contains required 'type' field (one of: added, changed, fixed, deprecated, removed, dependency_update, security, other)
- Contains required 'authors' field with at least one author
- Each author has a 'name' field (non-empty string)
- Contains either 'links' or 'issues' field (or both)
- If 'issues' is present, it must be an integer not exceeding 17000
- Comment block is removed
"""

import sys
import yaml


def validate_changelog_yaml(file_path):
    """Validate a changelog YAML file."""
    valid_types = ['added', 'changed', 'fixed', 'deprecated', 'removed', 'dependency_update', 'security', 'other']
    valid_keys = ['title', 'type', 'issues', 'links', 'important_notes', 'modules', 'authors']
    deprecated_keys = ['merge_requests', 'configurations']
    not_allowed_text = ['DELETE ALL COMMENTS UP HERE', 'Most such changes are too small']

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
            data = yaml.safe_load(raw_content)

        # Check if file contains a mapping (dictionary)
        if not isinstance(data, dict):
            print(f"::error file={file_path}::File must contain YAML mapping (key-value pairs)")
            return False

        # Check for invalid top-level keys
        for key in data.keys():
            if key not in valid_keys and key not in deprecated_keys:
                print(f"::error file={file_path}::Invalid top-level key '{key}'. Valid keys are: {', '.join(valid_keys)}")
                return False

        # Check for deprecated keys
        for deprecated_key in deprecated_keys:
            if deprecated_key in data:
                print(f"::error file={file_path}::Our project does not use the '{deprecated_key}' yaml key, please remove")
                return False

        # Validate 'title' field
        if 'title' not in data or not data['title']:
            print(f"::error file={file_path}::Missing or empty 'title' field")
            return False

        if not isinstance(data['title'], str) or not data['title'].strip():
            print(f"::error file={file_path}::Field 'title' must be a non-empty string")
            return False

        # Validate 'type' field
        if 'type' not in data or not data['type']:
            print(f"::error file={file_path}::Missing or empty 'type' field")
            return False

        if data['type'] not in valid_types:
            print(f"::error file={file_path}::Invalid 'type': '{data['type']}'. Must be one of: {', '.join(valid_types)}")
            return False

        # Validate 'authors' field
        if 'authors' not in data or not data['authors']:
            print(f"::error file={file_path}::Missing or empty 'authors' field")
            return False

        if not isinstance(data['authors'], list):
            print(f"::error file={file_path}::Field 'authors' must be a list")
            return False

        if len(data['authors']) == 0:
            print(f"::error file={file_path}::Field 'authors' must contain at least one author")
            return False

        # Validate each author
        for i, author in enumerate(data['authors']):
            if not isinstance(author, dict):
                print(f"::error file={file_path}::Author {i} must be a mapping (key-value pairs)")
                return False
            if 'name' not in author or not author['name']:
                print(f"::error file={file_path}::Author {i} missing or empty 'name' field")
                return False
            if not isinstance(author['name'], str) or not author['name'].strip():
                print(f"::error file={file_path}::Author {i} 'name' must be a non-empty string")
                return False

        # Validate that either 'links' or 'issues' exists (or both)
        if 'links' not in data and 'issues' not in data:
            print(f"::error file={file_path}::Must contain either 'links' or 'issues' key (or both)")
            return False

        # Validate 'issues' field if present
        if 'issues' in data:
            if not isinstance(data['issues'], int):
                print(f"::error file={file_path}::Field 'issues' must be an integer")
                return False
            if data['issues'] > 17000:
                print(f"::error file={file_path}::Field 'issues' value {data['issues']} points to a non-existing github PR. Did you intend to reference a JIRA issue, please use 'links'.")
                return False

        # Validate that comments are removed
        for not_allowed in not_allowed_text:
            if not_allowed in raw_content:
                print(f"::error file={file_path}::File still contains commented template text. Please remove the comment block at the top of the file.")
                return False

        # All validations passed
        print(f"✓ {file_path} is valid")
        print(f"  Title: {data['title']}")
        print(f"  Type: {data['type']}")
        print(f"  Authors: {', '.join(a['name'] for a in data['authors'])}")
        return True

    except yaml.YAMLError as e:
        print(f"::error file={file_path}::Invalid YAML: {e}")
        return False
    except Exception as e:
        print(f"::error file={file_path}::Error validating file: {e}")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: validate-changelog-yaml.py <yaml-file>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not validate_changelog_yaml(file_path):
        sys.exit(1)
