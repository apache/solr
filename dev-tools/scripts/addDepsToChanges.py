#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

"""
Script to create changelog YAML entries for solrbot dependency updates
"""
import os
import sys

sys.path.append(os.path.dirname(__file__))
from scriptutil import *

import argparse
import re
import yaml
from pathlib import Path

line_re = re.compile(r"(.*?) (\(branch_\d+x\) )?\(#(\d+)\)$")


class ChangeEntry:
    """
    Represents one dependency change entry.
    Fields:
      - pr_num: string PR number (e.g., '3605')
      - message: cleaned commit message text to be shown in CHANGES (noise removed)
      - author: Git author (e.g., 'solrbot')
    """

    def __init__(self, pr_num: str, message: str, author: str):
        self.pr_num = pr_num
        self.message = message
        self.author = author

    def dep_key(self) -> str:
        """
        Extract a dependency key from the message after 'Update ' and before ' to', '(' or end.
        This is used for de-duplication and sorting. Case-insensitive.
        """
        m = re.search(r"(?i)update\s+(.+?)(?:\s+to\b|\s*\(|$)", self.message)
        if m:
            return m.group(1).strip()
        return self.message.strip()

    def __str__(self) -> str:
        # Keep trailing newline to preserve existing blank-line formatting by update_changes
        return f"* PR#{self.pr_num}: {self.message} ({self.author})\n"

    def to_yaml_dict(self) -> dict:
        """
        Convert to a dictionary suitable for YAML serialization.
        Extracts JIRA IDs from the title and adds them to links.
        """
        # Extract JIRA IDs from the message
        title, jira_links = extract_jira_issues_from_title(self.message)

        # Build links: JIRA issues first, then PR
        links = jira_links.copy()  # Start with JIRA links
        links.append({
            'name': f'PR#{self.pr_num}',
            'url': f'https://github.com/apache/solr/pull/{self.pr_num}'
        })

        return {
            'title': title,
            'type': 'dependency_update',
            'authors': [
                {
                    'name': self.author
                }
            ],
            'links': links
        }

    def yaml_filename(self) -> str:
        """
        Generate a filesystem-safe filename for this entry.
        Format: PR#####-slug.yaml
        Truncates slug on whitespace boundaries, allowing up to 255 chars total.
        """
        # Clean message for slug
        slug = self.message.lower()
        # Replace whitespace with single space
        slug = re.sub(r'\s+', ' ', slug)
        # Remove non-alphanumeric except dashes
        slug = re.sub(r'[^a-z0-9-._ ]', '', slug)

        # Calculate available space for slug
        # Format: "PR" + pr_num + "-" + slug + ".yaml"
        # Typical PR#1234 = 8 chars + "-" = 9 chars, ".yaml" = 5 chars, total overhead = 14 chars
        # Most filesystems limit filenames to 255 chars
        max_filename_length = 255
        overhead = len(f"PR{self.pr_num}-.yaml")
        max_slug_length = max_filename_length - overhead

        # Truncate to max length on word boundaries if necessary
        if len(slug) > max_slug_length:
            # Find the last space within the limit
            truncated = slug[:max_slug_length]
            last_dash = truncated.rfind(' ')
            if last_dash > max_slug_length // 2:  # Keep at least half the available space
                slug = truncated[:last_dash]
            else:
                # If no good word boundary, use hard limit and clean up trailing spaces
                slug = truncated.rstrip(' ')
        else:
            # Remove trailing spaces
            slug = slug.rstrip(' ')

        return f"PR{self.pr_num}-{slug}.yaml"


def get_prev_release_tag(ver):
    """
    Based on a given version, compute the git tag for the "previous" version to calculate changes since.
    For a major version, we want all solrbot commits since last release, i.e. X-1.Y.Z
    For a minor version, we want all solrbot commits since X.Y-1.0
    For a patch version, we want all solrbot commits since X.Y.Z-1
    """
    releases_arr = run('git tag |grep "releases/solr" | cut -c 15-').strip().split("\n")
    releases = list(map(lambda x: Version.parse(x), releases_arr))
    if ver.is_major_release():
        last = releases.pop()
        return "releases/solr/%s" % last.dot
    if ver.is_minor_release():
        return "releases/solr/%s.%s.0" % (ver.major, ver.minor - 1)
    if ver.is_bugfix_release():
        return "releases/solr/%s.%s.%s" % (ver.major, ver.minor, ver.bugfix - 1)
    return None


def read_config():
    parser = argparse.ArgumentParser(description='Adds changelog entries in changelog/ folder')
    parser.add_argument('--version', type=Version.parse, help='Solr version to add changes to', required=True)
    parser.add_argument('--user', default='solrbot', help='Git user to get changes for. Defaults to solrbot')
    newconf = parser.parse_args()
    return newconf


def get_gitlog_lines(user: str, prev_tag: str):
    """
    Run git log and return a list of raw subject lines (%s), newest first.
    """
    output = run('git log --author=' + user + ' --oneline --no-merges --pretty=format:"%s" ' + prev_tag + '..')
    return list(filter(None, output.split("\n")))


def parse_gitlog_lines(lines, author: str):
    """
    Parse raw git log subject lines into ChangeEntry objects.
    - Extract pr_num and message
    - Strip '(branch_Nx)' noise
    - Remove optional leading 'chore(deps): '
    - Normalize 'update dependency X' to 'Update X' (case-insensitive)
    """
    entries = []
    for line in lines:
        match = line_re.search(line)
        if not match:
            print("Skipped un-parsable line: %s" % line)
            continue
        text = match.group(1)
        pr_num = match.group(3)
        # Clean message noise
        msg = text
        msg = re.sub(r"^chore\(deps\):\s*", "", msg, flags=re.IGNORECASE)
        # Normalize 'update dependency' to 'Update '
        msg = re.sub(r"(?i)^update\s+dependenc(y|ies)\s+", "Update ", msg)
        entries.append(ChangeEntry(pr_num=pr_num, message=msg, author=author))
    return entries


def write_changelog_yaml(entries):
    """
    Write each ChangeEntry to a YAML file in changelog/unreleased/
    """
    changelog_dir = Path('changelog/unreleased')

    # Create directory if it doesn't exist
    changelog_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for entry in entries:
        filename = changelog_dir / entry.yaml_filename()
        yaml_data = entry.to_yaml_dict()

        # Write YAML file with proper formatting
        with open(filename, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print(f"Created: {filename}")
        count += 1

    return count


def dedupe_entries(entries):
    """
    De-duplicate dependency update entries (newest first input) by dep_key.
    Keeps the first occurrence for each dependency key.
    """
    seen = set()
    result = []
    for e in entries:
        key = e.dep_key().lower()
        if key not in seen:
            seen.add(key)
            result.append(e)
    return result


def sort_entries(entries):
    """Return a new list sorted alphabetically by dependency key (case-insensitive)."""
    return sorted(entries, key=lambda e: e.dep_key().lower())


def main():
    if not os.path.exists('CHANGELOG.md'):
        sys.exit("Tool must be run from the root of a source checkout.")
    newconf = read_config()
    prev_tag = get_prev_release_tag(newconf.version)
    print("Creating changelog YAML entries for dependency updates since git tag %s" % prev_tag)
    try:
        gitlog_lines = get_gitlog_lines(newconf.user, prev_tag)
        entries = parse_gitlog_lines(gitlog_lines, author=newconf.user)
        if entries:
            deduped = dedupe_entries(entries)
            sorted_entries = sort_entries(deduped)
            count = write_changelog_yaml(sorted_entries)
            print(f"Successfully created {count} changelog YAML entries")
        else:
            print("No changes found for version %s" % newconf.version.dot)
        print("Done")
    except subprocess.CalledProcessError:
        print("Error running git log - check your --version")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nReceived Ctrl-C, exiting early')
