#!/usr/bin/env python3
"""
Generate changelog YAML entry for Renovate dependency update PRs.

This script parses the PR title to extract dependency information,
then generates a changelog YAML file in changelog/unreleased/ with the proper
naming convention and content structure.

Usage:
    python3 generate-renovate-changelog.py --pr-number 1234 --pr-title "Update org.apache.httpcomponents to v1.2.3"
"""

import argparse
import os
import re
import sys
import yaml
from pathlib import Path
from typing import Optional, Tuple


def sanitize_slug(text: str, max_length: int = 50) -> str:
    """
    Sanitize text to create a valid filename slug.

    - Convert to lowercase
    - Replace dots, colons, slashes with dashes
    - Replace other special chars with dashes
    - Preserve word boundaries
    - Truncate to max_length while preserving word boundaries
    """
    # Convert to lowercase
    text = text.lower()

    # Replace colons, slashes, dots with dashes
    text = re.sub(r'[:/.]+', '-', text)

    # Replace other special characters with dashes
    text = re.sub(r'[^a-z0-9\s-]', '-', text)

    # Replace spaces with dashes
    text = re.sub(r'\s+', '-', text)

    # Replace multiple dashes with single dash
    text = re.sub(r'-+', '-', text)

    # Remove leading/trailing dashes
    text = text.strip('-')

    # Truncate to max_length at word boundary
    if len(text) > max_length:
        text = text[:max_length]
        # Find last dash and truncate there
        last_dash = text.rfind('-')
        if last_dash > 0:
            text = text[:last_dash]
        text = text.rstrip('-')

    return text


def parse_pr_title(title: str) -> Tuple[str, Optional[str]]:
    """
    Parse Renovate PR title to extract dependency name and version.

    Handles patterns like:
    - "Update dependency org.junit.jupiter:junit-jupiter to v6"
    - "Update dependency com.jayway.jsonpath:json-path to v2.10.0"
    - "Update netty to v4.2.6.Final"
    - "Update apache.kafka to v3.9.1"
    - "Update actions/checkout action to v5"

    Returns:
        Tuple of (title_for_changelog, dependency_slug_for_filename)

    Note: The slug excludes the version number so the filename remains stable
          across version updates.
    """

    # Pattern 1: "Update dependency {group}:{artifact} to {version}"
    match = re.match(r'Update dependency (.+?) to (.+?)(?:\s*$|\s*\(|$)', title)
    if match:
        dep_name = match.group(1)
        version = match.group(2).strip()
        changelog_title = f"Update {dep_name} to {version}"
        # Slug contains only the dependency name, not the version
        slug = sanitize_slug(f"Update {dep_name}")
        return changelog_title, slug

    # Pattern 2: "Update {owner}/{action} action to {version}"
    match = re.match(r'Update ([a-z0-9-]+/[a-z0-9-]+) action to (.+?)(?:\s*$|\s*\(|$)', title)
    if match:
        action = match.group(1)
        version = match.group(2).strip()
        changelog_title = f"Update {action} action to {version}"
        # Slug contains only the action name, not the version
        slug = sanitize_slug(f"Update {action} action")
        return changelog_title, slug

    # Pattern 3: "Update {package} to {version}" (short form)
    match = re.match(r'Update ([a-z0-9\-_.]+) to (.+?)(?:\s*$|\s*\(|$)', title)
    if match:
        package = match.group(1)
        version = match.group(2).strip()
        changelog_title = f"Update {package} to {version}"
        # Slug contains only the package name, not the version
        slug = sanitize_slug(f"Update {package}")
        return changelog_title, slug

    # Fallback: use title as-is if no pattern matches
    return title, sanitize_slug(title)


def generate_changelog_entry(
    pr_number: int,
    changelog_title: str,
    pr_url: str = None
) -> dict:
    """Generate the changelog YAML entry dict."""

    if pr_url is None:
        pr_url = f"https://github.com/apache/solr/pull/{pr_number}"

    return {
        'title': changelog_title,
        'type': 'dependency_update',
        'authors': [
            {'name': 'solrbot'}
        ],
        'links': [
            {
                'name': f'PR#{pr_number}',
                'url': pr_url
            }
        ]
    }


def find_existing_changelog_file(pr_number: int, changelog_dir: str = 'changelog/unreleased') -> Optional[str]:
    """Find existing changelog file for this PR, returns path if exists."""
    pattern = f"PR#{pr_number}-*.yml"
    path = Path(changelog_dir)

    if not path.exists():
        return None

    for file in path.glob(f"PR#{pr_number}-*.yml"):
        return str(file)

    return None


def should_update_changelog(existing_file: str, new_title: str) -> bool:
    """
    Check if we need to update the changelog file.

    Updates if the title has changed (version was bumped).
    """
    if not existing_file or not Path(existing_file).exists():
        return False

    try:
        with open(existing_file, 'r') as f:
            content = yaml.safe_load(f)

        existing_title = content.get('title', '')
        return existing_title != new_title
    except Exception as e:
        print(f"Warning: Could not read existing file {existing_file}: {e}", file=sys.stderr)
        return False


def write_changelog_file(filename: str, entry: dict, changelog_dir: str = 'changelog/unreleased') -> None:
    """Write the changelog YAML file."""
    path = Path(changelog_dir)
    path.mkdir(parents=True, exist_ok=True)

    filepath = path / filename

    # Use YAML dumper that preserves order and formatting
    with open(filepath, 'w') as f:
        yaml.dump(
            entry,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True
        )

    print(f"Created/updated changelog file: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate changelog entry for Renovate PR',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate-renovate-changelog.py --pr-number 1234 --pr-title "Update org.apache.httpcomponents to v1.2.3"
  python3 generate-renovate-changelog.py --pr-number 3751 --pr-title "Update dependency com.microsoft.onnxruntime:onnxruntime to v1.23.1"
        """
    )

    parser.add_argument(
        '--pr-number',
        type=int,
        required=True,
        help='GitHub PR number'
    )
    parser.add_argument(
        '--pr-title',
        required=True,
        help='GitHub PR title (from the Renovate bot)'
    )
    parser.add_argument(
        '--changelog-dir',
        default='changelog/unreleased',
        help='Directory for changelog files (default: changelog/unreleased)'
    )

    args = parser.parse_args()

    # Parse the PR title
    changelog_title, slug = parse_pr_title(args.pr_title)

    # Generate filename
    filename = f"PR#{args.pr_number}-{slug}.yml"

    # Check if file already exists
    existing_file = find_existing_changelog_file(args.pr_number, args.changelog_dir)

    # Generate the new entry
    entry = generate_changelog_entry(args.pr_number, changelog_title)

    # Decide if we need to write
    should_write = True
    if existing_file and not should_update_changelog(existing_file, changelog_title):
        print(f"Changelog entry already up-to-date: {existing_file}")
        should_write = False

    if should_write:
        write_changelog_file(filename, entry, args.changelog_dir)
        return 0
    else:
        return 0


if __name__ == '__main__':
    sys.exit(main())
