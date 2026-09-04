#!/usr/bin/env python3
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

"""
Migration script to convert Apache Solr's legacy CHANGES.txt file format
to the new logchange YAML-based format.

This script parses the monolithic CHANGES.txt file and generates individual
YAML files for each changelog entry, organized by version.
"""

import os
import re
import sys
import json
import yaml
import html
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Tuple


class ChangeType:
    """Mapping of CHANGES.txt section headings to logchange types."""

    # Section headings that should be skipped entirely (no entries created)
    SKIP_SECTIONS = {
        "Versions of Major Components",
        "Detailed Change List",
        "Upgrading from Solr any prior release",
        "Upgrading from previous Solr versions",
        "System Requirements",
        "Lucene Information",
        "Status",
    }

    # Maps various section heading patterns to logchange types
    HEADING_MAP = {
        # New Features / Additions
        "New Features": "added",
        "Features": "added",
        "New Functionality": "added",

        # Improvements / Changes
        "Improvements": "changed",
        "Enhancements": "changed",
        "Changes": "changed",
        "Improvements / Changes": "changed",

        # Performance / Optimizations
        "Optimizations": "changed",
        "Performance": "changed",
        "Optimization": "changed",

        # Bug Fixes
        "Bug Fixes": "fixed",
        "Bug Fix": "fixed",
        "Bugs": "fixed",

        # Deprecations
        "Deprecations": "deprecated",
        "Deprecation": "deprecated",
        "Deprecation Notices": "deprecated",
        "Deprecation Removals": "removed",  # This is more about removals but was in Deprecations section

        # Removed / Removed Features
        "Removed": "removed",
        "Removal": "removed",
        "Removed Features": "removed",
        "Removals": "removed",

        # Security
        "Security": "security",
        "Security Fixes": "security",

        # Dependency Upgrades
        "Dependency Upgrades": "dependency_update",
        "Dependency Updates": "dependency_update",
        "Dependency Upgrade": "dependency_update",
        "Dependencies": "dependency_update",

        # Build / Infrastructure
        "Build": "other",
        "Build Changes": "other",
        "Build Fixes": "other",

        # Upgrade Notes - special category
        "Upgrade Notes": "upgrade_notes",

        # Other
        "Other Changes": "other",
        "Other": "other",
        "Miscellaneous": "other",
        "Docker": "other",
        "Ref Guide": "other",
        "Documentation": "other",
    }

    @staticmethod
    def get_type(heading: str) -> str:
        """Map a section heading to a logchange type."""
        heading_normalized = heading.strip()
        if heading_normalized in ChangeType.HEADING_MAP:
            return ChangeType.HEADING_MAP[heading_normalized]

        # Fallback: try case-insensitive matching
        for key, value in ChangeType.HEADING_MAP.items():
            if key.lower() == heading_normalized.lower():
                return value

        # Default to "other" if no match found
        print(f"Warning: Unknown section heading '{heading}', defaulting to 'other'", file=sys.stderr)
        return "other"


@dataclass
class Author:
    """Represents a changelog entry author/contributor."""
    name: str
    nick: Optional[str] = None
    url: Optional[str] = None

    def to_dict(self):
        """Convert to dictionary, excluding None values."""
        result = {"name": self.name}
        if self.nick:
            result["nick"] = self.nick
        if self.url:
            result["url"] = self.url
        return result


@dataclass
class Link:
    """Represents a link (JIRA issue or GitHub PR)."""
    name: str
    url: str

    def to_dict(self):
        """Convert to dictionary."""
        return {"name": self.name, "url": self.url}


@dataclass
class ChangeEntry:
    """Represents a single changelog entry."""
    title: str
    change_type: str
    authors: List[Author] = field(default_factory=list)
    links: List[Link] = field(default_factory=list)

    def to_dict(self):
        """Convert to dictionary for YAML serialization."""
        return {
            "title": self.title,
            "type": self.change_type,
            "authors": [author.to_dict() for author in self.authors],
            "links": [link.to_dict() for link in self.links],
        }


class AuthorParser:
    """Parses author/contributor information from entry text."""

    # Pattern to match TRAILING author list at the end of an entry: (Author1, Author2 via Committer)
    # Must be at the very end, possibly with trailing punctuation
    # Strategy: Match from the last '(' that leads to end-of-string pattern matching
    # This regex finds the LAST occurrence of a parenthesized group followed by optional whitespace/punctuation
    # and then end of string
    AUTHOR_PATTERN = re.compile(r'\s+\(([^()]+)\)\s*[.,]?\s*$', re.MULTILINE)

    # Pattern to detect JIRA/GitHub issue references (should be extracted as links, not authors)
    # Matches: SOLR-65, LUCENE-123, INFRA-456, PR#789, PR-789, GITHUB#123
    ISSUE_PATTERN = re.compile(r'^(?:SOLR|LUCENE|INFRA)-\d+$|^PR[#-]\d+$|^GITHUB#\d+$')

    @staticmethod
    def parse_authors(entry_text: str) -> Tuple[str, List[Author]]:
        """
        Extract authors from entry text.

        Returns:
            Tuple of (cleaned_text, list_of_authors)

        Patterns handled:
        - (Author Name)
        - (Author1, Author2)
        - (Author Name via CommitterName)
        - (Author1 via Committer1, Author2 via Committer2)

        Only matches author attribution at the END of the entry text,
        not in the middle of descriptions like (aka Standalone)

        Note: JIRA/GitHub issue IDs found in the author section are NOT added as authors,
        but are preserved in the returned text so IssueExtractor can process them as links.
        """
        # Find ALL matches and use the LAST one (rightmost)
        # This ensures we get the actual author attribution, not mid-text parentheses
        matches = list(AuthorParser.AUTHOR_PATTERN.finditer(entry_text))
        if not matches:
            return entry_text, []

        # Use the last match (rightmost)
        match = matches[-1]

        author_text = match.group(1)
        # Include the space before the parenthesis in what we remove
        cleaned_text = entry_text[:match.start()].rstrip()

        authors = []
        found_issues = []  # Track JIRA issues found in author section

        # Split by comma and slash, which are both used as delimiters in author sections
        # Patterns handled:
        # - "Author1, Author2" (comma delimiter)
        # - "Author1 / Author2" (slash delimiter)
        # - "Author1, Issue1 / Author2" (mixed delimiters)
        # Also aware of "via" keyword: "Author via Committer"
        segments = [seg.strip() for seg in re.split(r'[,/]', author_text)]

        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue

            # Check if this is a JIRA/GitHub issue reference
            if AuthorParser.ISSUE_PATTERN.match(segment):
                # Don't add as author, but remember to add it back to text for IssueExtractor
                found_issues.append(segment)
                continue

            # Handle "via" prefix (standalone or after author name)
            if segment.startswith('via '):
                # Malformed: standalone "via Committer" (comma was added incorrectly)
                # Extract just the committer name
                committer_name = segment[4:].strip()  # Remove "via " prefix
                if committer_name and not AuthorParser.ISSUE_PATTERN.match(committer_name):
                    authors.append(Author(name=committer_name))
            elif ' via ' in segment:
                # Format: "Author via Committer"
                parts = segment.split(' via ')
                author_name = parts[0].strip()
                committer_name = parts[1].strip() if len(parts) > 1 else ""

                # Add author if not an issue ID
                if author_name and not AuthorParser.ISSUE_PATTERN.match(author_name):
                    authors.append(Author(name=author_name))

                # Also add committer (the part after "via") as an author
                if committer_name and not AuthorParser.ISSUE_PATTERN.match(committer_name):
                    authors.append(Author(name=committer_name))
            else:
                # Just an author name (if not an issue ID)
                if not AuthorParser.ISSUE_PATTERN.match(segment):
                    authors.append(Author(name=segment))

        # Add found issues back to the cleaned text so IssueExtractor can find them
        if found_issues:
            cleaned_text = cleaned_text + " " + " ".join(found_issues)

        return cleaned_text, authors


class IssueExtractor:
    """Extracts issue/PR references from entry text."""

    JIRA_ISSUE_PATTERN = re.compile(r'(?:SOLR|LUCENE|INFRA)-(\d+)')
    GITHUB_PR_PATTERN = re.compile(r'(?:GitHub\s*)?#(\d+)')

    @staticmethod
    def extract_issues(entry_text: str) -> List[Link]:
        """Extract JIRA and GitHub issue references."""
        links = []
        seen_issues = set()  # Track seen issues to avoid duplicates

        # Extract SOLR, LUCENE, INFRA issues
        for match in IssueExtractor.JIRA_ISSUE_PATTERN.finditer(entry_text):
            issue_id = match.group(0)  # Full "SOLR-12345" or "LUCENE-12345" format
            if issue_id not in seen_issues:
                url = f"https://issues.apache.org/jira/browse/{issue_id}"
                links.append(Link(name=issue_id, url=url))
                seen_issues.add(issue_id)

        # Extract GitHub PRs in multiple formats:
        # "PR#3758", "PR-2475", "GITHUB#3666"
        github_patterns = [
            (r'PR[#-](\d+)', 'PR#'),  # PR#1234 or PR-1234
            (r'GITHUB#(\d+)', 'GITHUB#'),  # GITHUB#3666
        ]

        for pattern_str, prefix in github_patterns:
            pattern = re.compile(pattern_str)
            for match in pattern.finditer(entry_text):
                pr_num = match.group(1)
                pr_name = f"{prefix}{pr_num}"
                if pr_name not in seen_issues:
                    url = f"https://github.com/apache/solr/pull/{pr_num}"
                    links.append(Link(name=pr_name, url=url))
                    seen_issues.add(pr_name)

        return links


class SlugGenerator:
    """Generates slug-style filenames for YAML files."""

    # Characters that are unsafe in filenames on various filesystems
    # Avoid: < > : " / \ | ? *  and control characters
    # Note: # is safe on most filesystems
    UNSAFE_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')

    @staticmethod
    def generate_slug(issue_id: str, title: str) -> str:
        """
        Generate a slug from issue ID and title.

        Format: ISSUE-12345 short slug or VERSION entry 001 short slug
        Note: Previous slug formats used dashes ("ISSUE-12345-short-slug"), but this script now uses spaces between components (e.g., "ISSUE-12345 short slug").
        Spaces are preferred over dashes for improved readability, better preservation of word boundaries, and to avoid unnecessary character substitutions. This change also ensures that filenames remain filesystem-safe while being more human-friendly.
        Uses the actual issue ID without forcing SOLR- prefix
        Ensures filesystem-safe filenames and respects word boundaries
        Whitespace is preserved as spaces (not converted to dashes)
        """
        # Sanitize issue_id to remove unsafe characters (preserve case and # for readability)
        base_issue = SlugGenerator._sanitize_issue_id(issue_id)

        # Create slug from title: lowercase, preserve spaces, replace only unsafe chars with dash
        title_slug = SlugGenerator._sanitize_filename_part(title)

        # Limit to reasonable length while respecting word boundaries
        # Target max length: 50 chars for slug (leaving room for base_issue and space)
        if len(title_slug) > 50:
            # Find last word/space boundary within 50 chars
            truncated = title_slug[:50]
            # Find the last space within the limit
            last_space = truncated.rfind(' ')
            if last_space > 20:  # Keep at least 20 chars to avoid too-short slugs
                title_slug = truncated[:last_space]
            else:
                # If no good space boundary, try to find a dash (from unsafe chars)
                last_dash = truncated.rfind('-')
                if last_dash > 20:
                    title_slug = truncated[:last_dash]
                else:
                    # If no good boundary, use hard limit and clean up
                    title_slug = truncated.rstrip(' -')

        return f"{base_issue} {title_slug}"

    @staticmethod
    def _sanitize_issue_id(issue_id: str) -> str:
        """
        Sanitize issue ID while preserving uppercase letters and # for readability.
        Examples: SOLR-12345, LUCENE-1234, PR#3758, GITHUB#2408, v9.8.0-entry-001
        """
        # Replace unsafe characters with dash (preserving case)
        sanitized = SlugGenerator.UNSAFE_CHARS_PATTERN.sub('-', issue_id)

        # Replace remaining unsafe characters (but keep letters/numbers/dash/hash/dot)
        sanitized = re.sub(r'[^a-zA-Z0-9.#-]+', '-', sanitized)

        # Replace multiple consecutive dashes with single dash
        sanitized = re.sub(r'-+', '-', sanitized)

        # Strip leading/trailing dashes
        sanitized = sanitized.strip('-')

        return sanitized

    @staticmethod
    def _sanitize_filename_part(text: str) -> str:
        """
        Sanitize text for use in filenames.
        - Convert to lowercase
        - Remove quotes, colons, backticks
        - Replace other unsafe characters with dashes
        - Convert any whitespace to single space
        - Strip leading/trailing spaces and dashes
        """
        # Convert to lowercase
        text = text.lower()

        # Normalize all whitespace to single spaces
        text = re.sub(r'\s+', ' ', text)

        # Remove quotes, colons, backticks entirely (don't replace with dash)
        text = re.sub(r'["\':´`]', '', text)

        # Replace other unsafe characters (from UNSAFE_CHARS_PATTERN) with dash
        # This covers: < > " / \ | ? * and control characters
        # Note: we already removed quotes and colons above
        text = SlugGenerator.UNSAFE_CHARS_PATTERN.sub('-', text)

        # Replace other non-alphanumeric (except space, dash, and dot) with dash
        text = re.sub(r'[^a-z0-9\s.\-]+', '-', text)

        # Replace multiple consecutive dashes with single dash (but preserve spaces)
        text = re.sub(r'-+', '-', text)

        # Remove trailing dashes before we clean up space-dash sequences
        text = text.rstrip('-')

        # Handle " -" and "- " sequences: collapse to single space
        text = re.sub(r'\s*-\s*', ' ', text)

        # Replace multiple consecutive spaces with single space
        text = re.sub(r'\s+', ' ', text)

        # Strip leading/trailing spaces
        text = text.strip(' ')

        return text


class VersionSection:
    """Represents all entries for a specific version."""

    def __init__(self, version: str):
        self.version = version
        self.entries: List[ChangeEntry] = []

    def add_entry(self, entry: ChangeEntry):
        """Add an entry to this version."""
        self.entries.append(entry)

    def get_directory_name(self) -> str:
        """Get the directory name for this version (e.g., 'v10.0.0')."""
        return f"v{self.version}"


class ChangesParser:
    """Main parser for CHANGES.txt file."""

    # Pattern to match version headers: ==================  10.0.0 ==================
    # Also supports pre-release versions: 4.0.0-ALPHA, 4.0.0-BETA, 4.0.0-RC1, etc.
    VERSION_HEADER_PATTERN = re.compile(r'=+\s+([\d.]+(?:-[A-Za-z0-9]+)?)\s+=+')

    # Pattern to match section headers: "Section Name" followed by dashes
    # Matches patterns like "New Features\n---------------------"
    SECTION_HEADER_PATTERN = re.compile(r'^([A-Za-z][A-Za-z0-9\s/&-]*?)\n\s*-+\s*$', re.MULTILINE)

    def __init__(self, changes_file_path: str):
        self.changes_file_path = changes_file_path
        self.versions: List[VersionSection] = []

    def parse(self):
        """Parse the CHANGES.txt file."""
        with open(self.changes_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Split into version sections
        version_matches = list(self.VERSION_HEADER_PATTERN.finditer(content))

        for i, version_match in enumerate(version_matches):
            version = version_match.group(1)
            start_pos = version_match.end()

            # Find the end of this version section (start of next version or EOF)
            if i + 1 < len(version_matches):
                end_pos = version_matches[i + 1].start()
            else:
                end_pos = len(content)

            version_content = content[start_pos:end_pos]
            version_section = self._parse_version_section(version, version_content)
            self.versions.append(version_section)

    def _parse_version_section(self, version: str, content: str) -> VersionSection:
        """Parse all entries within a single version section."""
        version_section = VersionSection(version)

        # Split into subsections (New Features, Bug Fixes, etc.)
        section_matches = list(self.SECTION_HEADER_PATTERN.finditer(content))

        for i, section_match in enumerate(section_matches):
            section_name = section_match.group(1)

            # Skip sections that should not be migrated
            if section_name in ChangeType.SKIP_SECTIONS:
                continue

            section_type = ChangeType.get_type(section_name)

            start_pos = section_match.end()

            # Find the end of this section (start of next section or EOF)
            if i + 1 < len(section_matches):
                end_pos = section_matches[i + 1].start()
            else:
                end_pos = len(content)

            section_content = content[start_pos:end_pos]

            # Parse entries in this section
            entries = self._parse_entries(section_content, section_type)
            for entry in entries:
                version_section.add_entry(entry)

        return version_section

    def _parse_entries(self, section_content: str, change_type: str) -> List[ChangeEntry]:
        """Parse individual entries within a section.

        Handles both:
        - Bulleted entries: * text
        - Numbered entries: 1. text, 2. text, etc. (older format)
        """
        entries = []

        # First try to split by bulleted entries (* prefix)
        bulleted_pattern = re.compile(r'^\*\s+', re.MULTILINE)
        bulleted_entries = bulleted_pattern.split(section_content)

        if len(bulleted_entries) > 1:
            # Has bulleted entries
            for entry_text in bulleted_entries[1:]:  # Skip first empty split
                entry_text = entry_text.strip()
                if not entry_text or entry_text == "(No changes)":
                    continue
                entry = self._parse_single_entry(entry_text, change_type)
                if entry:
                    entries.append(entry)
        else:
            # No bulleted entries, try numbered entries (old format: "1. text", "2. text", etc.)
            numbered_pattern = re.compile(r'^\s{0,2}\d+\.\s+', re.MULTILINE)
            if numbered_pattern.search(section_content):
                # Has numbered entries
                numbered_entries = numbered_pattern.split(section_content)
                for entry_text in numbered_entries[1:]:  # Skip first empty split
                    entry_text = entry_text.strip()
                    if not entry_text:
                        continue
                    entry = self._parse_single_entry(entry_text, change_type)
                    if entry:
                        entries.append(entry)
            else:
                # No standard entries found, try as paragraph
                entry_text = section_content.strip()
                if entry_text and entry_text != "(No changes)":
                    entry = self._parse_single_entry(entry_text, change_type)
                    if entry:
                        entries.append(entry)

        return entries

    def _parse_single_entry(self, entry_text: str, change_type: str) -> Optional[ChangeEntry]:
        """Parse a single entry into a ChangeEntry object."""
        # Extract authors
        description, authors = AuthorParser.parse_authors(entry_text)

        # Extract issues/PRs
        links = IssueExtractor.extract_issues(description)

        # Remove all issue/PR IDs from the description text
        # Handle multiple formats of issue references at the beginning:

        # 1. Remove leading issues with mixed projects: "LUCENE-3323,SOLR-2659,LUCENE-3329,SOLR-2666: description"
        description = re.sub(r'^(?:(?:SOLR|LUCENE|INFRA)-\d+(?:\s*[,:]?\s*)?)+:\s*', '', description)

        # 2. Remove SOLR-specific issues: "SOLR-12345: description" or "SOLR-12345, SOLR-12346: description"
        description = re.sub(r'^(?:SOLR-\d+(?:\s*,\s*SOLR-\d+)*\s*[:,]?\s*)+', '', description)

        # 3. Remove PR references: "PR#123: description" or "GITHUB#456: description"
        description = re.sub(r'^(?:(?:PR|GITHUB)#\d+(?:\s*,\s*(?:PR|GITHUB)#\d+)*\s*[:,]?\s*)+', '', description)

        # 4. Remove parenthesized issue lists at start: "(SOLR-123, SOLR-456)"
        description = re.sub(r'^\s*\((?:SOLR-\d+(?:\s*,\s*)?)+\)\s*', '', description)
        description = re.sub(r'^\s*\((?:(?:SOLR|LUCENE|INFRA)-\d+(?:\s*,\s*)?)+\)\s*', '', description)

        # 5. Remove any remaining leading issue references
        description = re.sub(r'^[\s,;]*(?:SOLR-\d+|LUCENE-\d+|INFRA-\d+|PR#\d+|GITHUB#\d+)[\s,:;]*', '', description)
        while re.match(r'^[\s,;]*(?:SOLR-\d+|LUCENE-\d+|INFRA-\d+|PR#\d+|GITHUB#\d+)', description):
            description = re.sub(r'^[\s,;]*(?:SOLR-\d+|LUCENE-\d+|INFRA-\d+|PR#\d+|GITHUB#\d+)[\s,:;]*', '', description)

        description = description.strip()

        # Normalize whitespace: collapse multiple newlines/spaces into single spaces
        # This joins multi-line formatted text into a single coherent paragraph
        description = re.sub(r'\s+', ' ', description)

        # Escape HTML angle brackets to prevent markdown rendering issues
        # Only escape < and > to avoid breaking markdown links and quotes
        description = description.replace('<', '&lt;').replace('>', '&gt;')

        if not description:
            return None

        return ChangeEntry(
            title=description,
            change_type=change_type,
            authors=authors,
            links=links,
        )


class YamlWriter:
    """Writes ChangeEntry objects to YAML files."""

    @staticmethod
    def write_entry(entry: ChangeEntry, slug: str, output_dir: Path):
        """Write a single entry to a YAML file."""
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{slug}.yml"
        filepath = output_dir / filename

        # Convert entry to dictionary and write as YAML
        entry_dict = entry.to_dict()

        with open(filepath, 'w', encoding='utf-8') as f:
            # Use custom YAML dumper for better formatting
            yaml.dump(
                entry_dict,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                width=80  # Line width for better readability
            )

        return filepath


class ReleaseDate:
    """Fetches and manages release dates from Apache projects JSON."""

    @staticmethod
    def fetch_release_dates_and_latest() -> tuple:
        """
        Fetch release dates from Apache projects JSON and identify latest version.

        Returns:
            Tuple of (version_dates_dict, latest_version_string)
            Example: ({'9.9.0': '2025-07-24', ...}, '9.9.0')
        """
        import urllib.request
        from packaging import version as pkg_version

        version_dates = {}
        latest_version = None
        latest_version_obj = None

        url = "https://projects.apache.org/json/projects/solr.json"

        try:
            response = urllib.request.urlopen(url, timeout=10)
            data = json.loads(response.read().decode('utf-8'))

            releases = data.get('release', [])
            for release in releases:
                ver = release.get('revision')
                created = release.get('created')

                if ver and created:
                    version_dates[ver] = created

                    # Track the latest (highest) version
                    try:
                        ver_obj = pkg_version.parse(ver)
                        if latest_version_obj is None or ver_obj > latest_version_obj:
                            latest_version_obj = ver_obj
                            latest_version = ver
                    except Exception:
                        # Skip invalid version strings
                        pass
        except Exception as e:
            print(f"Warning: Could not fetch release dates: {e}", file=sys.stderr)

        return version_dates, latest_version


class VersionWriter:
    """Handles version enumeration, comparison, and release-date.txt writing."""

    def __init__(self, changes_file_path: str, changelog_dir: str):
        self.changes_file_path = changes_file_path
        self.changelog_dir = Path(changelog_dir)
        self.parser = ChangesParser(changes_file_path)

        # Fetch release dates from Apache projects JSON
        version_dates_raw, _ = ReleaseDate.fetch_release_dates_and_latest()

        # Normalize version keys for consistent lookup (e.g., "3.1" -> "3.1.0")
        self.version_dates = {}
        for version, date in version_dates_raw.items():
            normalized = self._normalize_version(version)
            # Keep the first occurrence (most canonical form)
            if normalized not in self.version_dates:
                self.version_dates[normalized] = date

    def run(self):
        """Execute version comparison and release-date.txt writing."""
        print("Parsing CHANGES.txt for versions...")
        self.parser.parse()

        # Extract versions from CHANGES.txt
        changes_versions = set(vs.version for vs in self.parser.versions)
        print(f"Found {len(changes_versions)} versions in CHANGES.txt")

        # Get existing version folders
        existing_folders = self.get_existing_version_folders()
        print(f"Found {len(existing_folders)} existing version folders in changelog/")

        # Get versions from solr.json (which is what ReleaseDate fetches)
        solr_json_versions = set(self.version_dates.keys())
        print(f"Found {len(solr_json_versions)} versions in solr.json\n")

        # Build normalized version mappings for matching (supports semver like 3.1 == 3.1.0)
        changes_normalized = {self._normalize_version(v): v for v in changes_versions}
        existing_normalized = {self._normalize_version(v): v for v in existing_folders}
        solr_normalized = {self._normalize_version(v): v for v in solr_json_versions}

        # Combine all normalized versions
        all_normalized = sorted(set(changes_normalized.keys()) | set(solr_normalized.keys()) | set(existing_normalized.keys()),
                               key=self._version_sort_key)

        # Print comparison report
        self._print_comparison_report(all_normalized, changes_normalized, solr_normalized, existing_normalized)

        # Write release-date.txt for existing folders
        self._write_release_dates(existing_normalized)

    def get_existing_version_folders(self) -> set:
        """Get all existing vX.Y.Z folders in changelog/."""
        if not self.changelog_dir.exists():
            return set()

        folders = set()
        for item in self.changelog_dir.iterdir():
            if item.is_dir() and item.name.startswith('v') and item.name[1:].replace('.', '').isdigit():
                # Extract version without 'v' prefix
                version = item.name[1:]
                folders.add(version)

        return folders

    @staticmethod
    def _normalize_version(version: str) -> str:
        """
        Normalize incomplete version strings to X.Y.Z format.
        Complete versions (3+ numeric parts) are left unchanged.
        Incomplete versions are padded with zeros.
        Pre-release versions (e.g., 4.0.0-ALPHA) are handled correctly.

        Supports semantic versioning where "3.1" matches "3.1.0".
        But keeps distinct versions separate: 3.6.0, 3.6.1, 3.6.2 are NOT normalized to the same value.

        Examples:
        - "3.1" -> "3.1.0" (2 parts, pad to 3)
        - "3" -> "3.0.0" (1 part, pad to 3)
        - "3.1.0" -> "3.1.0" (3 parts, unchanged)
        - "3.6.1" -> "3.6.1" (3 parts, unchanged)
        - "3.6.2" -> "3.6.2" (3 parts, unchanged - NOT collapsed!)
        - "4.0.0-ALPHA" -> "4.0.0-ALPHA" (pre-release, unchanged)
        - "4.0-ALPHA" -> "4.0.0-ALPHA" (incomplete pre-release, pad to 3 numeric parts)
        - "4.0.0-ALPHA.0" -> "4.0.0-ALPHA" (remove spurious .0 from pre-release)
        - "3.1.0.0" -> "3.1.0.0" (4 parts, unchanged)
        """
        # Check if this is a pre-release version (contains dash)
        if '-' in version:
            # Split on the dash to separate numeric version from pre-release identifier
            base_version, prerelease = version.split('-', 1)
            base_parts = base_version.split('.')

            # Pad the base version to 3 parts
            while len(base_parts) < 3:
                base_parts.append('0')

            # Take only first 3 numeric parts, then rejoin with pre-release identifier
            # This prevents "4.0.0-ALPHA.0" from being added
            normalized_base = '.'.join(base_parts[:3])
            return f"{normalized_base}-{prerelease}"
        else:
            # Non-pre-release version - use original logic
            parts = version.split('.')

            # If already 3+ parts, return as-is (complete version)
            if len(parts) >= 3:
                return version

            # If less than 3 parts, pad with zeros to make it 3 parts
            while len(parts) < 3:
                parts.append('0')
            return '.'.join(parts)

    def _version_sort_key(self, version: str) -> tuple:
        """Convert version string to sortable tuple for proper ordering."""
        try:
            from packaging import version as pkg_version
            return (pkg_version.parse(version),)
        except Exception:
            return (version,)

    def _print_comparison_report(self, all_normalized_versions: list, changes_normalized: dict,
                                solr_normalized: dict, existing_normalized: dict):
        """
        Print a comparison report of versions across sources.

        Args:
            all_normalized_versions: List of normalized versions to display
            changes_normalized: Dict mapping normalized version -> original version from CHANGES.txt
            solr_normalized: Dict mapping normalized version -> original version from solr.json
            existing_normalized: Dict mapping normalized version -> original version from folders
        """
        print("=" * 100)
        print(f"{'Normalized':<15} | {'CHANGES.txt':<15} | {'solr.json':<15} | {'Folder':<15} | {'Release Date':<20}")
        print("-" * 100)

        for norm_version in all_normalized_versions:
            in_changes = "✓" if norm_version in changes_normalized else " "
            in_solr_json = "✓" if norm_version in solr_normalized else " "
            has_folder = "✓" if norm_version in existing_normalized else " "

            # Get original version strings for display
            orig_changes = changes_normalized.get(norm_version, "")
            orig_solr = solr_normalized.get(norm_version, "")
            orig_folder = existing_normalized.get(norm_version, "")

            # Get release date using normalized version (all version_dates keys are normalized)
            release_date = self.version_dates.get(norm_version, "(no date)")

            # Format original versions as "orig" if different from normalized
            changes_str = f"{orig_changes}" if orig_changes and orig_changes != norm_version else ""
            solr_str = f"{orig_solr}" if orig_solr and orig_solr != norm_version else ""
            folder_str = f"{orig_folder}" if orig_folder and orig_folder != norm_version else ""

            print(f"{norm_version:<15} | {in_changes} {changes_str:<13} | {in_solr_json} {solr_str:<13} | {has_folder} {folder_str:<13} | {release_date:<20}")

        print("=" * 100)

    def _write_release_dates(self, existing_normalized: dict):
        """
        Write release-date.txt files for existing version folders that don't have them.

        Args:
            existing_normalized: Dict mapping normalized version -> original folder version string
        """
        written_count = 0
        skipped_count = 0

        print("\nWriting release-date.txt files:")
        for norm_version in sorted(existing_normalized.keys(), key=self._version_sort_key):
            orig_folder_version = existing_normalized[norm_version]
            version_dir = self.changelog_dir / f"v{orig_folder_version}"
            release_date_file = version_dir / "release-date.txt"

            # Get release date using normalized version (all version_dates keys are normalized)
            release_date = self.version_dates.get(norm_version)

            if release_date:
                if release_date_file.exists():
                    existing_content = release_date_file.read_text().strip()
                    if existing_content == release_date:
                        print(f"  ✓ {orig_folder_version}: already has release-date.txt")
                    else:
                        print(f"  ⚠ {orig_folder_version}: already has release-date.txt with different date ({existing_content})")
                    skipped_count += 1
                else:
                    with open(release_date_file, 'w', encoding='utf-8') as f:
                        f.write(release_date + '\n')
                    version_display = f"{orig_folder_version} (normalized: {norm_version})" if orig_folder_version != norm_version else orig_folder_version
                    print(f"  ✓ {version_display}: wrote release-date.txt ({release_date})")
                    written_count += 1
            else:
                version_display = f"{orig_folder_version} (normalized: {norm_version})" if orig_folder_version != norm_version else orig_folder_version
                print(f"  ⚠ {version_display}: no date found in solr.json")
                skipped_count += 1

        print(f"\nSummary: {written_count} files written, {skipped_count} skipped/existing")


class MigrationRunner:
    """Orchestrates the complete migration process."""

    def __init__(self, changes_file_path: str, output_base_dir: str, last_released_version: Optional[str] = None):
        self.changes_file_path = changes_file_path
        self.output_base_dir = Path(output_base_dir)
        self.parser = ChangesParser(changes_file_path)

        # Fetch release dates and latest version
        self.version_dates, detected_latest = ReleaseDate.fetch_release_dates_and_latest()

        # Use provided version or detected latest
        self.last_released_version = last_released_version or detected_latest

        if self.last_released_version:
            print(f"Latest released version: {self.last_released_version}", file=sys.stderr)

        self.stats = {
            'versions_processed': 0,
            'entries_migrated': 0,
            'entries_skipped': 0,
            'files_created': 0,
            'release_dates_written': 0,
            'unreleased_entries': 0,
        }

    def run(self):
        """Execute the migration."""
        print(f"Parsing CHANGES.txt from: {self.changes_file_path}")
        self.parser.parse()

        print(f"Found {len(self.parser.versions)} versions")

        for version_section in self.parser.versions:
            self._process_version(version_section)

        self._print_summary()

    def _process_version(self, version_section: VersionSection):
        """Process all entries for a single version."""
        from packaging import version as pkg_version

        # Determine if this version should go to unreleased folder
        is_unreleased = False
        if self.last_released_version:
            try:
                current_ver = pkg_version.parse(version_section.version)
                latest_ver = pkg_version.parse(self.last_released_version)
                is_unreleased = current_ver > latest_ver
            except Exception:
                # If parsing fails, treat as unreleased (conservative approach)
                is_unreleased = True

        # Route to appropriate directory
        if is_unreleased:
            version_dir = self.output_base_dir / "unreleased"
            print(f"\nProcessing version {version_section.version} (unreleased):")
            self.stats['unreleased_entries'] += len(version_section.entries)
        else:
            version_dir = self.output_base_dir / version_section.get_directory_name()
            print(f"\nProcessing version {version_section.version}:")

        print(f"  Found {len(version_section.entries)} entries")

        # Write release-date.txt if we have a date for this version (only for released versions)
        if not is_unreleased and version_section.version in self.version_dates:
            release_date = self.version_dates[version_section.version]
            release_date_file = version_dir / "release-date.txt"
            version_dir.mkdir(parents=True, exist_ok=True)

            with open(release_date_file, 'w', encoding='utf-8') as f:
                f.write(release_date + '\n')

            self.stats['release_dates_written'] += 1
            print(f"  Release date: {release_date}")

        entry_counter = 0  # For entries without explicit issue IDs

        for entry in version_section.entries:
            # Find primary issue ID from links
            issue_id = None
            for link in entry.links:
                if link.name.startswith('SOLR-'):
                    issue_id = link.name
                    break

            if not issue_id:
                # If no SOLR issue found, try to use other JIRA/PR formats
                for link in entry.links:
                    if link.name.startswith(('LUCENE-', 'INFRA-', 'PR#', 'GITHUB#')):
                        issue_id = link.name
                        break

            if not issue_id:
                # No standard issue/PR found, generate a synthetic ID
                # Use format: unknown-001, unknown-002, etc.
                entry_counter += 1
                synthetic_id = f"unknown-{entry_counter:03d}"
                issue_id = synthetic_id

            # Generate slug and write YAML
            slug = SlugGenerator.generate_slug(issue_id, entry.title)
            filepath = YamlWriter.write_entry(entry, slug, version_dir)

            print(f"    ✓ {slug}.yml")
            self.stats['entries_migrated'] += 1
            self.stats['files_created'] += 1

        self.stats['versions_processed'] += 1

    def _print_summary(self):
        """Print migration summary."""
        print("\n" + "="*60)
        print("Migration Summary:")
        print(f"  Versions processed:    {self.stats['versions_processed']}")
        print(f"  Entries migrated:      {self.stats['entries_migrated']}")
        print(f"  Entries skipped:       {self.stats['entries_skipped']}")
        print(f"  Files created:         {self.stats['files_created']}")
        print(f"  Release dates written: {self.stats['release_dates_written']}")
        if self.stats['unreleased_entries'] > 0:
            print(f"  Unreleased entries:    {self.stats['unreleased_entries']}")
        print("="*60)


class StdinProcessor:
    """Process individual changelog entries from stdin and output YAML to stdout."""

    @staticmethod
    def process():
        """
        Read from stdin, parse individual changelog entries, and output YAML.

        Ignores headers and nested structure.
        Outputs YAML entries separated by '----' YAML separator.
        """
        import sys

        # Read all lines from stdin
        lines = sys.stdin.readlines()

        entries_yaml = []
        i = 0

        while i < len(lines):
            line = lines[i]

            # Skip empty lines and header lines (lines with only dashes or equals)
            if not line.strip() or re.match(r'^[-=\s]+$', line):
                i += 1
                continue

            # Check if this line starts a changelog entry (bullet point)
            if line.strip().startswith('*') or line.strip().startswith('-'):
                # Collect the full entry (may span multiple lines)
                entry_text = line.strip()[1:].strip()  # Remove bullet and leading spaces

                # Continue reading continuation lines
                i += 1
                while i < len(lines):
                    next_line = lines[i]
                    # If the next line is another entry or empty, stop collecting
                    if (next_line.strip().startswith('*') or
                        next_line.strip().startswith('-') or
                        re.match(r'^[-=\s]+$', next_line) or
                        not next_line.strip()):
                        break
                    # Add to entry text
                    entry_text += ' ' + next_line.strip()
                    i += 1

                # Parse the entry to a ChangeEntry
                entry = EntryParser.parse_entry_line(entry_text)
                if entry:
                    # Serialize to YAML
                    yaml_dict = {
                        'title': entry.title,
                        'type': entry.change_type,
                    }
                    if entry.authors:
                        yaml_dict['authors'] = [{'name': a.name} for a in entry.authors]
                    if entry.links:
                        yaml_dict['links'] = [
                            {'name': link.name, 'url': link.url}
                            for link in entry.links
                        ]

                    yaml_str = yaml.dump(yaml_dict, default_flow_style=False, sort_keys=False, allow_unicode=True)
                    entries_yaml.append(yaml_str.rstrip())
            else:
                i += 1

        # Output entries separated by YAML separators
        for i, yaml_entry in enumerate(entries_yaml):
            if i > 0:
                print('----')
            print(yaml_entry, end='')
            if yaml_entry and not yaml_entry.endswith('\n'):
                print()


class EntryParser:
    """Parse a single changelog entry line."""

    @staticmethod
    def parse_entry_line(text: str) -> Optional[ChangeEntry]:
        """
        Parse a single changelog entry line.

        Format: [ISSUE-ID: ]description (author1) (author2) ...
        """
        if not text.strip():
            return None

        # Extract issue links
        links = IssueExtractor.extract_issues(text)

        # Remove issue IDs from text
        for link in links:
            # Remove markdown link format [ID](url)
            text = re.sub(rf'\[{re.escape(link.name)}\]\([^)]+\)', '', text)
            # Remove plain text issue IDs
            text = re.sub(rf'{re.escape(link.name)}\s*:?\s*', '', text)

        text = text.strip()

        # Extract authors
        text, authors = AuthorParser.parse_authors(text)
        text = text.strip()

        # Escape HTML angle brackets
        text = text.replace('<', '&lt;').replace('>', '&gt;')

        if not text:
            return None

        # Default to 'other' type
        change_type = 'other'

        return ChangeEntry(
            title=text,
            change_type=change_type,
            authors=authors,
            links=links,
        )


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate Apache Solr CHANGES.txt to logchange YAML format"
    )
    parser.add_argument(
        "changes_file",
        help="Path to the CHANGES.txt file to migrate. Use '-' to read individual changelog entries from stdin and output YAML to stdout"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="changelog",
        help="Directory to write changelog/ structure (default: ./changelog)"
    )
    parser.add_argument(
        "--last-released",
        help="Last released version (e.g., 9.9.0). Versions newer than this go to unreleased/. "
             "If not specified, fetches from Apache projects JSON."
    )
    parser.add_argument(
        "--write-versions",
        action="store_true",
        help="Parse CHANGES.txt to enumerate versions, compare with solr.json, and write release-date.txt files to existing changelog folders"
    )

    args = parser.parse_args()

    # Handle stdin/stdout mode
    if args.changes_file == '-':
        StdinProcessor.process()
        return

    if not os.path.exists(args.changes_file):
        print(f"Error: CHANGES.txt file not found: {args.changes_file}", file=sys.stderr)
        sys.exit(1)

    # Handle --write-versions mode
    if args.write_versions:
        writer = VersionWriter(args.changes_file, args.output_dir)
        writer.run()
        return

    # Standard migration mode
    runner = MigrationRunner(args.changes_file, args.output_dir, args.last_released)
    runner.run()


if __name__ == "__main__":
    main()
