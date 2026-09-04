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

#!/usr/bin/env python3
"""
Validate Solr changelog structure across branches.

This tool helps release managers validate that the changelog folder structure
and CHANGELOG.md file are correct across the four active development branches:
- main (next major/minor release, e.g., 11.0.0)
- branch_10x (stable branch, e.g., 10.1.0 next release)
- branch_10_0 (release branch, e.g., 10.0.0, 10.0.1)
- branch_9x (previous stable, e.g. 9.11.0)
- branch_9_x (previous bugfix, e.g., 9.9.0)

It checks that:
1. Git status is clean (no uncommitted changes)
2. All changelog/vX.Y.Z folders are identical across branches
3. Released files don't exist in the 'unreleased' folder
4. Generates a report showing features scheduled for each branch
5. Checks for possible duplicate JIRA issues across yml files
"""

import os
import sys
import re
import json
import yaml
import shutil
import subprocess
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

@dataclass
class BranchInfo:
    """Information about a branch."""
    name: str
    version: str
    is_main: bool = False
    is_stable: bool = False
    is_release: bool = False
    is_previous_major_bugfix: bool = False  # e.g., branch_9_9 (9.9.0)
    is_previous_major_stable: bool = False  # e.g., branch_9x (9.8.0)
    changelog_path: Optional[Path] = None
    unreleased_files: Set[str] = field(default_factory=set)
    versioned_folders: Dict[str, Set[str]] = field(default_factory=dict)
    has_changelog_folder: bool = True  # False if changelog folder doesn't exist yet
    duplicate_issues: Dict[str, List[str]] = field(default_factory=dict)  # Maps issue ID -> list of files
    new_count: int = 0  # Count of features new to this version (first appearing in this branch)
    not_in_newer: Set[str] = field(default_factory=set)  # Files in unreleased that don't appear in any newer branch


class ChangelogValidator:
    """Main validator for Solr changelog structure."""

    def __init__(
        self,
        git_root: Optional[Path] = None,
        work_dir: Optional[Path] = None,
        report_file: Optional[Path] = None,
        changelog_file: Optional[Path] = None,
        fetch_remote: bool = False,
        report_format: str = "md",
        skip_sync_check: bool = False,
        check_duplicates: bool = False,
    ):
        """Initialize the validator.

        Args:
            git_root: Root of git repository (auto-detected if not provided)
            work_dir: Working directory for temporary branches (default: auto in /tmp)
            report_file: File to write validation report to (default: stdout)
            changelog_file: File to write generated CHANGELOG.md to. (default: none)
            fetch_remote: If True, fetch from remote.
            report_format: Report format ("md" for Markdown or "json" for JSON)
            skip_sync_check: If True, skip git branch sync check
            check_duplicates: If True, check for duplicate JIRA issues (default: False)
        """
        if git_root is None:
            git_root = self._find_git_root()
        self.git_root = git_root
        self.changelog_root = git_root / "changelog"
        self.build_gradle = git_root / "build.gradle"
        self.changelog_md = git_root / "CHANGELOG.md"
        self.report_file = report_file
        self.changelog_file = changelog_file
        self.work_dir = work_dir
        self.fetch_remote = fetch_remote
        self.report_format = report_format
        self.skip_sync_check = skip_sync_check
        self.check_duplicates = check_duplicates
        self.branches = {}
        self.remote_branches = set()
        self.errors = []
        self.warnings = []
        self.info_messages = []
        self.current_branch = None
        self.temp_branch = None

    @staticmethod
    def _find_git_root() -> Path:
        """Find the git root directory."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"],
                capture_output=True,
                text=True,
                check=True
            )
            return Path(result.stdout.strip())
        except subprocess.CalledProcessError:
            print("Error: Not in a git repository")
            sys.exit(1)

    def run_git(self, args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a git command."""
        return subprocess.run(
            ["git"] + args,
            cwd=self.git_root,
            capture_output=True,
            text=True,
            check=check
        )

    def validate_git_status(self) -> bool:
        """Verify that git status is clean with no uncommitted changes."""
        self.info_messages.append("Checking git status...")
        result = self.run_git(["status", "--porcelain"], check=False)

        if result.stdout.strip():
            self.errors.append(
                "Git status is not clean. Please commit or stash all changes:\n" +
                result.stdout
            )
            return False

        # Store current branch for later restoration
        result = self.run_git(["rev-parse", "--abbrev-ref", "HEAD"])
        self.current_branch = result.stdout.strip()

        self.info_messages.append("✓ Git status is clean")
        return True

    def _find_apache_remote(self) -> Optional[str]:
        """Find the official Apache Solr remote (matching 'apache' and 'solr' in URL)."""
        result = self.run_git(["remote", "-v"], check=False)
        if result.returncode != 0:
            return None

        for parts in (line.split() for line in result.stdout.strip().split("\n") if line):
            if len(parts) >= 2 and "apache" in parts[1].lower() and "solr" in parts[1].lower():
                return parts[0]
        return None

    def _get_remote_branches(self, remote: str) -> set:
        """Get list of available branches from remote."""
        result = self.run_git(["ls-remote", "--heads", remote], check=False)
        if result.returncode != 0:
            return set()

        return {parts[1].replace("refs/heads/", "") for line in result.stdout.strip().split("\n")
                if (parts := line.split()) and len(parts) >= 2 and parts[1].startswith("refs/heads/")}

    def validate_branches_up_to_date(self) -> bool:
        """Validate remote branches are available.

        By default (fetch_remote=False): Uses cached remote-tracking branches from last fetch
        If fetch_remote=True: Fetches fresh list from Apache remote
        """
        apache_remote = self._find_apache_remote()
        if not apache_remote:
            self.errors.append(
                "Could not find Apache Solr remote (matching 'apache/solr'). "
                "Please ensure you have the official remote configured."
            )
            return False

        if self.fetch_remote:
            # Fetch fresh data from remote
            self._log_and_print("Fetching fresh branch list from Apache remote...")
            self._log_and_print(f"  Found Apache remote: {apache_remote}")

            self._log_and_print(f"  Fetching from {apache_remote}...", flush=True)
            result = self.run_git(["fetch", apache_remote], check=False)
            if result.returncode != 0:
                self.errors.append(
                    f"Failed to fetch from {apache_remote}: {result.stderr}"
                )
                return False
            print("  ✓ Fetch complete")

            remote_branches = self._get_remote_branches(apache_remote)
            if not remote_branches:
                self.errors.append(
                    f"Could not retrieve branch list from {apache_remote}"
                )
                return False

            # Store the fetched remote branches for use in discover_branches()
            self.remote_branches = remote_branches
        else:
            # Use cached remote-tracking branches
            self._log_and_print("Using cached remote-tracking branches (run with --fetch-remote to update)")

        self._log_and_print(f"  Found Apache remote: {apache_remote}")
        return True

    def parse_version_from_build_gradle(self, branch: str) -> Optional[str]:
        """Parse baseVersion from build.gradle on a specific branch.

        Tries to read from local branch first, then from remote if available.
        In offline mode, also tries remote-tracking branches (e.g., origin/branch_X_Y).
        """
        gradle_path = self.build_gradle.relative_to(self.git_root)
        content = self._get_file_from_branch(branch, str(gradle_path))

        if not content:
            self.warnings.append(f"Could not read build.gradle from branch {branch}")
            return None

        match = re.search(r"String\s+baseVersion\s*=\s*['\"]([^'\"]+)['\"]", content)
        if match:
            return match.group(1)

        self.warnings.append(f"Could not find baseVersion in build.gradle on branch {branch}")
        return None

    @staticmethod
    def _extract_version(name: str) -> int:
        """Extract major version number from branch name (e.g., 10 from branch_10_0)."""
        if m := re.search(r"branch_(\d+)", name):
            return int(m.group(1))
        return -1

    @staticmethod
    def _extract_branch_version_tuple(name: str) -> tuple:
        """Extract full version from branch name as tuple for comparison.

        Examples:
        - branch_9_9 -> (9, 9)
        - branch_9_1 -> (9, 1)
        - branch_10_0 -> (10, 0)
        """
        if m := re.search(r"branch_(\d+)_(\d+)", name):
            return (int(m.group(1)), int(m.group(2)))
        return (-1, -1)

    @staticmethod
    def _parse_version_string(version: str) -> tuple:
        """Convert version string to sortable tuple (e.g., '9.9.1' -> (9, 9, 1))."""
        return tuple(int(p) for p in version.split("."))

    def _log_and_print(self, msg: str, flush: bool = False) -> None:
        """Log a message and print it to stdout."""
        self.info_messages.append(msg)
        print(msg, flush=flush)

    def _format_error_for_display(self, error) -> str:
        """Format an error for display. Handles both strings and dict objects."""
        if isinstance(error, dict):
            return json.dumps(error, indent=2)
        return str(error)

    def _git_ref_output(self, cmd: List[str], branch: str, rel_path: str) -> Optional[str]:
        """Execute git command on a branch ref, trying local then remote. Helper for file/tree operations."""
        result = self.run_git([*cmd, f"{branch}:{rel_path}"], check=False)
        if result.returncode != 0 and (remote := self._find_apache_remote()):
            result = self.run_git([*cmd, f"{remote}/{branch}:{rel_path}"], check=False)
        return result.stdout if result.returncode == 0 else None

    def _get_file_from_branch(self, branch: str, rel_path: str) -> Optional[str]:
        """Read a file from a branch, trying local first, then remote."""
        return self._git_ref_output(["show"], branch, rel_path)

    def _get_tree_from_branch(self, branch: str, rel_path: str) -> Optional[str]:
        """List tree contents from a branch, trying local first, then remote."""
        return self._git_ref_output(["ls-tree", "-r", "--name-only"], branch, rel_path)

    def discover_branches(self) -> bool:
        """Discover available branches and determine their types."""
        self._log_and_print("Discovering branches...")

        # Get branch list (cached or fetched)
        if not self.fetch_remote:
            result = self.run_git(["branch", "-r"], check=False)
            if result.returncode != 0:
                self.errors.append(f"Failed to list branches: {result.stderr}")
                return False
            branches = sorted(set(b.split("/", 1)[1] for line in result.stdout.split("\n")
                                 if (b := line.strip()) and not b.startswith("HEAD")))
            msg = f"  Found {len(branches)} branches (cached remote)"
        else:
            if not self.remote_branches:
                self.errors.append("Remote branches not discovered. Run with remote validation first.")
                return False
            branches = sorted(self.remote_branches)
            msg = f"  Found {len(branches)} branches from remote"

        self._log_and_print(msg)

        # Categorize and validate branches
        main_b, stable_b, release_b, feature_b = self._categorize_branches(branches)
        if not all([main_b, stable_b, release_b]):
            missing = [k for k, v in [("main", main_b), ("stable (branch_*x)", stable_b),
                                      ("release (branch_*_0)", release_b)] if not v]
            location = "in cached remote" if not self.fetch_remote else "on fetched remote"
            self.errors.append(f"Missing branches {location}: {', '.join(missing)}")
            return False

        # Get current versions
        stable = max(stable_b, key=self._extract_version)
        release = max(release_b, key=self._extract_version)
        prev_major_stable, prev_major_bugfix = self._find_previous_major_branches(
            stable_b, feature_b, self._extract_branch_version_tuple(release))

        # Register branches
        configs = [(main_b, True, False, False, False, False),
                   (stable, False, True, False, False, False),
                   (release, False, False, True, False, False)]
        if prev_major_bugfix:
          self._log_and_print("Not using previous major")
            #configs.append((prev_major_bugfix, False, False, False, True, False))
        if prev_major_stable:
            configs.append((prev_major_stable, False, False, False, False, True))

        return self._register_branches(configs)

    def validate_branches_in_sync(self) -> bool:
        """Validate that all discovered branches are up to date with their remote tracking branches."""
        if self.skip_sync_check:
            self._log_and_print("Skipping branch sync check (--skip-sync-check enabled)")
            return True

        self._log_and_print("Validating that all branches are in sync with remote...")

        out_of_sync = []
        for branch_name in self.branches.keys():
            # Check if branch has a tracking branch
            result = self.run_git(
                ["rev-list", "--left-right", f"{branch_name}...origin/{branch_name}"],
                check=False
            )

            if result.returncode != 0:
                # Branch might not have a remote tracking branch, try to find it
                continue

            # If there's any output, the branch is not in sync
            # Format: commits only in left side (local) are prefixed with <
            # commits only in right side (remote) are prefixed with >
            lines = result.stdout.strip().split("\n")
            local_only = [l for l in lines if l.startswith("<")]
            remote_only = [l for l in lines if l.startswith(">")]

            if local_only or remote_only:
                local_count = len(local_only)
                remote_count = len(remote_only)
                out_of_sync.append(f"{branch_name} ({local_count} local, {remote_count} remote)")

        if out_of_sync:
            self.errors.append(
                f"The following branches are not in sync with remote:\n  "
                + "\n  ".join(out_of_sync) +
                "\nPlease run 'git pull' on these branches to update them, or use --skip-sync-check to ignore this check (for testing only)."
            )
            return False

        self._log_and_print("  ✓ All branches are in sync with remote")
        return True

    def get_branch_changelog_structure(self, branch: str) -> Tuple[Set[str], Dict[str, Set[str]]]:
        """Get changelog structure for a specific branch.

        Tries local branch first, then remote if available.
        In offline mode, also tries remote-tracking branches (e.g., origin/branch_X_Y).
        """
        unreleased = set()
        versioned = defaultdict(set)
        changelog_rel_path = self.changelog_root.relative_to(self.git_root)

        output = self._get_tree_from_branch(branch, str(changelog_rel_path))
        if not output:
            # Only warn if this is a branch that should have changelog
            # (not expected to warn for older branches without changelog folder yet)
            return unreleased, dict(versioned)

        for line in output.strip().split("\n"):
            if not line:
                continue
            # Extract relative path from changelog root
            parts = line.split("/")
            if len(parts) < 2:
                continue

            folder = parts[0]
            filename = "/".join(parts[1:])

            # Only include YAML files, skip metadata files like version-summary.md
            if not filename.endswith(('.yml', '.yaml')):
                continue

            if folder == "unreleased":
                unreleased.add(filename)
            elif re.match(r"v\d+\.\d+\.\d+", folder):
                versioned[folder].add(filename)

        return unreleased, dict(versioned)

    def load_branch_data(self) -> bool:
        """Load changelog data for all branches."""
        self._log_and_print("Loading changelog data for all branches...")
        for name, info in self.branches.items():
            info.unreleased_files, info.versioned_folders = self.get_branch_changelog_structure(name)
            info.has_changelog_folder = bool(info.unreleased_files or info.versioned_folders)
            detail = (f"{len(info.unreleased_files)} unreleased, {len(info.versioned_folders)} versioned"
                     if info.has_changelog_folder else "(no changelog folder yet)")
            self._log_and_print(f"  {name}: {detail}")
        return True

    def _extract_issues_from_file(self, file_content: str) -> Set[str]:
        """Extract JIRA and GitHub issue IDs from a changelog YAML file.

        Returns a set of issue identifiers (e.g., 'SOLR-12345', 'GITHUB-PR-789').
        """
        issues = set()
        try:
            data = yaml.safe_load(file_content)
            if data and isinstance(data, dict):
                # Look for links section with issue references
                links = data.get('links', [])
                if isinstance(links, list):
                    for link in links:
                        if isinstance(link, dict):
                            name = link.get('name', '').strip()
                            if name:
                                # Extract just the issue ID part (e.g., "SOLR-17961" from "SOLR-17961")
                                # or "GITHUB-PR-123" from issue names
                                match = re.search(r'(SOLR-\d+|GITHUB-PR-\d+)', name)
                                if match:
                                    issues.add(match.group(1))
        except Exception:
            # If YAML parsing fails, silently continue
            pass
        return issues

    def detect_duplicate_issues(self) -> bool:
        """Detect duplicate JIRA/GitHub issue references within each branch.

        Returns False if duplicates are found (adds warnings), True otherwise.
        """
        self._log_and_print("Detecting duplicate issues within each branch...")

        has_duplicates = False

        for branch_name, branch_info in self.branches.items():
            if not branch_info.has_changelog_folder:
                continue

            # Collect all issues and their files for the unreleased section
            issue_to_files = defaultdict(list)

            for filename in branch_info.unreleased_files:
                # Get the file content
                file_content = self._get_file_from_branch(branch_name, f"changelog/unreleased/{filename}")
                if file_content:
                    issues = self._extract_issues_from_file(file_content)
                    for issue in issues:
                        issue_to_files[issue].append(filename)

            # Find duplicates
            duplicates = {issue: files for issue, files in issue_to_files.items() if len(files) > 1}

            if duplicates:
                has_duplicates = True
                branch_info.duplicate_issues = duplicates

                # Create warning messages
                for issue, files in sorted(duplicates.items()):
                    files_str = ", ".join(sorted(files))
                    msg = f"Branch {branch_name}: Issue {issue} appears in multiple files: {files_str}"
                    self.warnings.append(msg)
                    self._log_and_print(f"  ⚠ {msg}")

        if not has_duplicates:
            self._log_and_print("  ✓ No duplicate issues found")

        return not has_duplicates

    def _log_validation_result(self, errors_before: int, success_msg: str) -> None:
        """Log validation result based on error count."""
        if len(self.errors) == errors_before:
            self.info_messages.append(f"  ✓ {success_msg}")
        else:
            self.info_messages.append("  ✗ Validation failed - see errors above")

    def _run_validation_step(self, step_func) -> bool:
        """Run a validation step and report failure."""
        if not step_func():
            self.print_report(None)
            return False
        return True

    def _generate_error_only_report(self) -> str:
        """Generate a simple report with only errors and warnings."""
        report_lines = []
        if self.errors:
            report_lines.append("ERRORS:")
            report_lines.extend(f"  ✗ {self._format_error_for_display(e)}" for e in self.errors)
        if self.warnings:
            report_lines.append("\nWARNINGS:")
            report_lines.extend(f"  ⚠ {w}" for w in self.warnings)
        return "\n".join(report_lines)

    def validate_versioned_folders_identical(self) -> bool:
        """Verify that all changelog/vX.Y.Z folders are identical across branches."""
        self.info_messages.append("Validating versioned folders are identical across branches...")

        all_folders = set().union(*(info.versioned_folders.keys() for info in self.branches.values()))
        if not all_folders:
            self.info_messages.append("  No versioned folders found")
            return True

        errors_before = len(self.errors)

        for folder in sorted(all_folders):
            contents_by_branch = {b: info.versioned_folders.get(folder)
                                 for b, info in self.branches.items() if folder in info.versioned_folders}

            # Check if folder exists on all branches
            if len(contents_by_branch) != len(self.branches):
                missing_branches = set(self.branches.keys()) - set(contents_by_branch.keys())
                error_obj = {
                    "folder": folder,
                    "missing_on_branches": sorted(missing_branches)
                }
                self.errors.append(error_obj)
                continue

            # Find union of all files and check for differences
            all_files = set().union(*contents_by_branch.values())

            # Build file-centric diffs: which branches have each file
            diffs = {}
            all_branches = sorted(contents_by_branch.keys())
            for file in sorted(all_files):
                branches_with_file = sorted([b for b, contents in contents_by_branch.items() if file in contents])
                # Only include files that don't exist in all branches
                if len(branches_with_file) != len(contents_by_branch):
                    branches_without_file = sorted([b for b in all_branches if b not in branches_with_file])
                    diffs[file] = {
                        "present_in": branches_with_file,
                        "missing_in": branches_without_file
                    }

            # If there are any differences, create structured error
            if diffs:
                error_obj = {
                    "folder": folder,
                    "diffs": diffs
                }
                self.errors.append(error_obj)

        self._log_validation_result(errors_before, f"All {len(all_folders)} versioned folders are identical")
        return True

    def validate_no_released_in_unreleased(self) -> bool:
        """Verify that no YAML changelog files from released versions exist in unreleased folder."""
        self.info_messages.append("Validating that released files don't exist in unreleased folder...")
        errors_before = len(self.errors)

        for branch, info in self.branches.items():
            released = set().union(*info.versioned_folders.values())
            # Filter to only check YAML/YML changelog entry files
            unreleased_yaml = {f for f in info.unreleased_files if f.endswith(('.yml', '.yaml'))}
            released_yaml = {f for f in released if f.endswith(('.yml', '.yaml'))}
            if conflicts := (unreleased_yaml & released_yaml):
                self.errors.append(f"Branch {branch}: Files in both unreleased and released: {conflicts}")

        self._log_validation_result(errors_before, "No released files found in unreleased folder")
        return len(self.errors) == errors_before

    def _get_branch_by_type(self, **kwargs) -> BranchInfo:
        """Helper to retrieve a branch by its type flags."""
        return next(i for i in self.branches.values()
                   if all(getattr(i, k) == v for k, v in kwargs.items()))

    def _map_analysis_to_branches(self, analysis: Optional[Dict]) -> Dict[str, tuple]:
        """Map analysis keys to branch info. Returns {branch_name: (analysis_key, analysis_data)}."""
        if not analysis:
            return {}
        key_to_flags = {
            "release": {"is_release": True},
            "stable": {"is_stable": True},
            "main": {"is_main": True},
            "previous_major_bugfix": {"is_previous_major_bugfix": True},
            "previous_major_stable": {"is_previous_major_stable": True},
        }
        return {(b := self._get_branch_by_type(**key_to_flags[k])).name: (k, analysis[k])
                for k in analysis.keys() if k in key_to_flags and (b := self._get_branch_by_type(**key_to_flags[k]))}

    def _get_branch_configs_for_report(self, analysis: Dict) -> List[tuple]:
        """Build branch configs for report. Returns list of (display_name, key, label) tuples sorted by version."""
        branch_configs = [
            ("Release Branch", "release", "Features scheduled:"),
            ("Stable Branch", "stable", "Additional features (not in release):"),
            ("Main Branch", "main", "Main-only features:"),
        ]
        if "previous_major_bugfix" in analysis:
            branch_configs.append(("Previous Major Bugfix Branch", "previous_major_bugfix", "Features (not in release):"))
        if "previous_major_stable" in analysis:
            branch_configs.append(("Previous Major Stable Branch", "previous_major_stable", "Features (not in release):"))
        branch_configs.sort(key=lambda cfg: self._parse_version_string(analysis[cfg[1]]['version']))
        return branch_configs

    def _categorize_branches(self, branches: List[str]) -> tuple:
        """Categorize branches by type patterns. Returns (main, stable, release, feature) lists."""
        return (next((b for b in branches if b == "main"), None),
                [b for b in branches if re.match(r"branch_\d+x$", b)],
                [b for b in branches if re.match(r"branch_\d+_0$", b)],
                [b for b in branches if re.match(r"branch_\d+_[1-9]\d*$", b)])

    def _find_previous_major_branches(self, stable_b: List[str], feature_b: List[str], release_version: tuple) -> tuple:
        """Find previous major stable and bugfix branches. Returns (prev_major_stable, prev_major_bugfix)."""
        older_stable = [b for b in stable_b if self._extract_version(b) < release_version[0]]
        prev_major_stable = max(older_stable, key=self._extract_version) if older_stable else None

        older_features = [b for b in feature_b if self._extract_branch_version_tuple(b)[0] < release_version[0]]
        prev_major_bugfix = max(older_features, key=self._extract_branch_version_tuple) if older_features else None

        return prev_major_stable, prev_major_bugfix

    def _register_branches(self, configs: List[tuple]) -> bool:
        """Register discovered branches. Returns True on success, False if version parsing fails."""
        for name, is_main, is_stable, is_release, is_prev_bugfix, is_prev_stable in configs:
            version = self.parse_version_from_build_gradle(name)
            if not version:
                self.errors.append(f"Could not parse version for branch {name}")
                return False
            self.branches[name] = BranchInfo(
                name=name, version=version, is_main=is_main, is_stable=is_stable, is_release=is_release,
                is_previous_major_bugfix=is_prev_bugfix, is_previous_major_stable=is_prev_stable,
                changelog_path=self.changelog_root,
            )
            self.info_messages.append(f"  {name}: version {version}")
        return True

    def analyze_feature_distribution(self) -> Dict:
        """Analyze which features are scheduled for each branch."""
        self.info_messages.append("Analyzing feature distribution...")

        release_info = self._get_branch_by_type(is_release=True)
        stable_info = self._get_branch_by_type(is_stable=True)
        main_info = self._get_branch_by_type(is_main=True)
        prev_bugfix_info = self._get_branch_by_type(is_previous_major_bugfix=True) if any(b.is_previous_major_bugfix for b in self.branches.values()) else None
        prev_stable_info = self._get_branch_by_type(is_previous_major_stable=True) if any(b.is_previous_major_stable for b in self.branches.values()) else None

        # Calculate feature sets - ordered from oldest to newest branch
        # Each branch shows only files that first appear in that branch
        prev_stable_only = (prev_stable_info.unreleased_files) if prev_stable_info else set()
        prev_bugfix_only = (prev_bugfix_info.unreleased_files - prev_stable_only) if prev_bugfix_info else set()
        release_features = release_info.unreleased_files - prev_stable_only - prev_bugfix_only
        stable_only = stable_info.unreleased_files - prev_stable_only - prev_bugfix_only - release_features
        main_only = main_info.unreleased_files - prev_stable_only - prev_bugfix_only - release_features - stable_only

        # Calculate files not in any newer versions
        # For each branch, find files that don't appear in any newer branch's unreleased files
        all_newer_files = {}
        newer_cumulative = set()

        # Build cumulative sets from newest to oldest
        for branch_info in reversed(sorted(self.branches.values(), key=lambda b: self._parse_version_string(b.version))):
            newer_cumulative = newer_cumulative | branch_info.unreleased_files
            all_newer_files[branch_info.name] = newer_cumulative.copy()

        # Calculate not_in_newer for each branch (skip main branch since it's always the newest)
        for branch_info in self.branches.values():
            if branch_info.is_main:
                # Main branch is always newest, so it will never have files not in newer versions
                branch_info.not_in_newer = set()
                continue

            # Get all files from newer branches (excluding current branch's own files from newer set)
            newer_files = set()
            for other_info in self.branches.values():
                if self._parse_version_string(other_info.version) > self._parse_version_string(branch_info.version):
                    newer_files |= other_info.unreleased_files

            # Files in this branch's unreleased that don't appear in any newer branch
            branch_info.not_in_newer = branch_info.unreleased_files - newer_files

        # Build analysis dictionary
        def build_entry(info: BranchInfo, features: Set[str]) -> Dict:
            return {"version": info.version, **({"count": len(features), "files": sorted(features)} if info.has_changelog_folder else {"has_changelog_folder": False})}

        analysis = {
            "release": build_entry(release_info, release_features),
            "stable": build_entry(stable_info, stable_only),
            "main": build_entry(main_info, main_only),
        }
        if prev_bugfix_info:
            analysis["previous_major_bugfix"] = build_entry(prev_bugfix_info, prev_bugfix_only)
        if prev_stable_info:
            analysis["previous_major_stable"] = build_entry(prev_stable_info, prev_stable_only)

        # Log summary
        for key, label in [("release", "Release"), ("stable", "Stable"), ("main", "Main"),
                          ("previous_major_bugfix", "Previous Major Bugfix"), ("previous_major_stable", "Previous Major Stable")]:
            if key in analysis:
                version = analysis[key]["version"]
                if "has_changelog_folder" in analysis[key] and not analysis[key]["has_changelog_folder"]:
                    self.info_messages.append(f"  {label} branch ({version}): (no changelog folder yet)")
                else:
                    count = analysis[key].get("count", 0)
                    self.info_messages.append(f"  {label} branch ({version}): {count} features")

        return analysis

    def _copy_files_to_snapshot(self, branch_info: BranchInfo, files: Set[str], snapshot_dir: Path) -> None:
        """Helper to copy files from a branch to a snapshot directory."""
        changelog_rel = self.changelog_root.relative_to(self.git_root)
        for file in files:
            result = self.run_git(
                ["show", f"{branch_info.name}:{changelog_rel}/unreleased/{file}"],
                check=False
            )
            if result.returncode == 0:
                (snapshot_dir / file).write_text(result.stdout)
            else:
                self.warnings.append(f"Could not retrieve unreleased/{file} from {branch_info.name}")

    def create_temp_branch_with_changelog(self, analysis: Dict) -> Optional[str]:
        """Create temporary git branch with merged changelog for generation."""
        self.info_messages.append("Creating temporary branch for changelog generation...")

        try:
            # Generate a unique branch name
            self.temp_branch = f"__changelog-validation-{os.getpid()}__"

            # Create the temp branch from current branch
            result = self.run_git(["checkout", "-b", self.temp_branch], check=False)
            if result.returncode != 0:
                self.errors.append(f"Failed to create temporary branch: {result.stderr}")
                return None

            self.info_messages.append(f"  Created temporary branch: {self.temp_branch}")

            # Get branch info
            release_info = self._get_branch_by_type(is_release=True)
            stable_info = self._get_branch_by_type(is_stable=True)
            main_info = self._get_branch_by_type(is_main=True)

            # Check if previous major branches exist
            has_prev_bugfix = any(b.is_previous_major_bugfix for b in self.branches.values())
            prev_bugfix_info = self._get_branch_by_type(is_previous_major_bugfix=True) if has_prev_bugfix else None
            has_prev_stable = any(b.is_previous_major_stable for b in self.branches.values())
            prev_stable_info = self._get_branch_by_type(is_previous_major_stable=True) if has_prev_stable else None

            # Prepare changelog folder structure
            changelog_dir = self.changelog_root

            # Clear existing unreleased folder
            unreleased_dir = changelog_dir / "unreleased"
            if unreleased_dir.exists():
                shutil.rmtree(unreleased_dir)
            unreleased_dir.mkdir(parents=True, exist_ok=True)

            # Create and prepare snapshot folders
            release_features = release_info.unreleased_files
            stable_features = stable_info.unreleased_files - release_features
            main_features = main_info.unreleased_files - release_features - stable_features

            snapshots = {
                changelog_dir / f"v{release_info.version}-SNAPSHOT": (release_info, release_features),
                changelog_dir / f"v{stable_info.version}-SNAPSHOT": (stable_info, stable_features),
                changelog_dir / f"v{main_info.version}-SNAPSHOT": (main_info, main_features),
            }
            if prev_bugfix_info and prev_bugfix_info.has_changelog_folder:
                snapshots[changelog_dir / f"v{prev_bugfix_info.version}-SNAPSHOT"] = (prev_bugfix_info, prev_bugfix_info.unreleased_files)
            if prev_stable_info and prev_stable_info.has_changelog_folder:
                snapshots[changelog_dir / f"v{prev_stable_info.version}-SNAPSHOT"] = (prev_stable_info, prev_stable_info.unreleased_files)

            # Create snapshot directories and copy files
            for snapshot_path, (branch_info, files) in snapshots.items():
                if snapshot_path.exists():
                    shutil.rmtree(snapshot_path)
                snapshot_path.mkdir(parents=True, exist_ok=True)
                if branch_info and files:
                    self._copy_files_to_snapshot(branch_info, files, snapshot_path)

            self.info_messages.append(f"  ✓ Prepared changelog structure in temporary branch")
            return self.temp_branch

        except Exception as e:
            self.errors.append(f"Failed to create temporary branch: {e}")
            return None

    def generate_changelog_preview(self, temp_branch: str) -> Optional[str]:
        """Generate CHANGELOG.md preview using gradle task."""
        self.info_messages.append("Generating changelog preview...")
        try:
            # Run logchangeGenerate task
            result = subprocess.run(
                ["./gradlew", "logchangeGenerate"],
                cwd=self.git_root,
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode != 0:
                self.warnings.append(f"logchangeGenerate task failed: {result.stderr}")
                return None

            # Read the generated CHANGELOG.md
            if self.changelog_md.exists():
                preview = re.sub(r'\[unreleased\]\s*\n-+\s*\n\s*\n', '', self.changelog_md.read_text())
                self.info_messages.append("  ✓ Generated changelog preview")
                return preview
            else:
                self.warnings.append("CHANGELOG.md not generated")
                return None

        except subprocess.TimeoutExpired:
            self.warnings.append("Changelog generation timed out")
            return None
        except Exception as e:
            self.warnings.append(f"Could not generate changelog preview: {e}")
            return None

    def _print_git_status(self, message: str, git_result: subprocess.CompletedProcess) -> None:
        """Helper to print git command status."""
        if git_result.returncode != 0:
            error_msg = f"{message}: {git_result.stderr}"
            print(f"  ✗ {error_msg}")
            self.warnings.append(error_msg)

    def cleanup_temp_branch(self):
        """Clean up temporary branch and restore original branch."""
        if not self.temp_branch:
            return

        print(f"\nCleaning up temporary branch: {self.temp_branch}")

        # Restore original branch
        if self.current_branch:
            #print(f"  Restoring branch: {self.current_branch}")
            result = self.run_git(["checkout", self.current_branch], check=False)
            self._print_git_status(f"Restored branch: {self.current_branch}", result)
        else:
            print("  Warning: Could not determine original branch")

        # Delete temporary branch
        #print(f"  Deleting temporary branch: {self.temp_branch}")
        result = self.run_git(["branch", "-D", self.temp_branch], check=False)
        self._print_git_status(f"Deleted temporary branch: {self.temp_branch}", result)

        # Clean up working directory
        #print(f"  Cleaning up working directory")
        result = self.run_git(["reset", "--hard"], check=False)
        if result.returncode == 0:
            result = self.run_git(["clean", "-fd"], check=False)
        self._print_git_status("Cleaned up working directory", result)

    @staticmethod
    def _get_branch_type(branch_info: BranchInfo) -> str:
        """Get human-readable branch type abbreviation."""
        type_map = [
            (lambda b: b.is_previous_major_bugfix, "prev_bug"),
            (lambda b: b.is_previous_major_stable, "prev_sta"),
            (lambda b: b.is_release, "release"),
            (lambda b: b.is_stable, "stable"),
        ]
        return next((t for check, t in type_map if check(branch_info)), "main")

    def generate_report(self, analysis: Dict) -> str:
        """Generate validation report in Markdown format."""
        # Build mapping of branch names to analysis data
        analysis_by_branch = self._map_analysis_to_branches(analysis)

        # Generate branch information table
        branches_table_rows = []
        for i in sorted(self.branches.values(), key=lambda b: self._parse_version_string(b.version)):
            btype = self._get_branch_type(i)
            new_count = ""
            not_in_newer_count = ""
            if i.name in analysis_by_branch:
                _, analysis_data = analysis_by_branch[i.name]
                new_count = str(analysis_data.get("count", ""))

            if i.has_changelog_folder:
                not_in_newer_count = str(len(i.not_in_newer))
                row = f"| {i.name:15} | {btype:8} | {i.version:7} | {len(i.unreleased_files):>10} | {new_count:>6} | {not_in_newer_count:>13} |"
            else:
                row = f"| {i.name:15} | {btype:8} | {i.version:7} | {'N/A':>10} | {'N/A':>6} | {'N/A':>13} |"
            branches_table_rows.append(row)

        report = f"""# Solr Changelog Validation Report

## Repository Status
- **Git root:** `{self.git_root}`

## Branch Information

| Branch          | Type     | Version | Unreleased |    New | Not in Newer  |
|-----------------|----------|---------|------------|--------|---------------|
{chr(10).join(branches_table_rows)}

## Feature Distribution
"""

        branch_configs = self._get_branch_configs_for_report(analysis)

        for branch_name, key, label in branch_configs:
            d = analysis[key]
            if "has_changelog_folder" in d and not d["has_changelog_folder"]:
                report += f"\n### {branch_name} (v{d['version']})\n- (no changelog folder yet)\n"
            else:
                report += f"\n### {branch_name} (v{d['version']})\n- **{label}** {d['count']}\n"
                if d['files']:
                    files_str = "\n".join(f"  - `{f}`" for f in d['files'][:5])
                    if len(d['files']) > 5:
                        files_str += f"\n  - ... and {len(d['files']) - 5} more"
                    report += files_str + "\n"

        # Add duplicate issues section if found
        has_duplicates = any(info.duplicate_issues for info in self.branches.values())
        if has_duplicates:
            report += "\n## Duplicate Issues\n"
            for branch_info in sorted(self.branches.values(), key=lambda b: self._parse_version_string(b.version)):
                if branch_info.duplicate_issues:
                    report += f"\n### {branch_info.name} (v{branch_info.version})\n"
                    for issue, files in sorted(branch_info.duplicate_issues.items()):
                        files_str = ", ".join(f"`{f}`" for f in sorted(files))
                        report += f"- Issue **{issue}** appears in: {files_str}\n"

        report += "\n## Validation Results\n"
        if self.errors:
            report += f"\n### ✗ {len(self.errors)} Error(s) Found\n"
            for i, e in enumerate(self.errors, 1):
                report += f"\n**Error {i}:**\n```json\n{self._format_error_for_display(e)}\n```\n"
        else:
            report += "\n### ✓ All Validations Passed\n"

        if self.warnings:
            report += f"\n### ⚠ {len(self.warnings)} Warning(s)\n"
            for w in self.warnings:
                report += f"- {w}\n"

        return report

    def run(self) -> bool:
        """Run the complete validation."""
        print("\nStarting Solr changelog validation...\n")

        try:
            # Step 1: Check git status
            if not self._run_validation_step(self.validate_git_status):
                return False

            # Step 2: Check if branches are up to date with remote (before discovery)
            if not self._run_validation_step(self.validate_branches_up_to_date):
                return False

            # Step 3: Discover branches (uses remote or local branch list)
            if not self._run_validation_step(self.discover_branches):
                return False

            # Step 3.5: Validate all discovered branches are in sync with remote
            if not self._run_validation_step(self.validate_branches_in_sync):
                return False

            # Step 4: Load branch data
            if not self._run_validation_step(self.load_branch_data):
                return False

            # Step 5: Validate versioned folders
            self.validate_versioned_folders_identical()

            # Step 6: Validate no released files in unreleased
            self.validate_no_released_in_unreleased()

            # Step 7: Detect duplicate issues (warnings, not errors) - only if enabled
            if self.check_duplicates:
                self.detect_duplicate_issues()

            # Step 8: Analyze feature distribution
            analysis = self.analyze_feature_distribution()

            # Step 9: Create temporary branch and generate changelog
            temp_branch = self.create_temp_branch_with_changelog(analysis)
            changelog_preview = None

            if temp_branch:
                changelog_preview = self.generate_changelog_preview(temp_branch)

            # Step 10: Generate and print report
            self.print_report(analysis, changelog_preview)

            # Return success if no errors
            success = len(self.errors) == 0

            return success

        finally:
            # Always cleanup temp branch
            self.cleanup_temp_branch()

    def _generate_json_report(self, analysis: Optional[Dict] = None) -> str:
        """Generate validation report in JSON format."""
        analysis_by_branch = self._map_analysis_to_branches(analysis)
        report_data = {
            "success": len(self.errors) == 0,
            "errors": self.errors,
            "warnings": self.warnings,
            "branch_report": {}
        }

        # Add branch information sorted by version in ascending order
        sorted_branches = sorted(self.branches.values(), key=lambda b: self._parse_version_string(b.version))
        for info in sorted_branches:
            branch_entry = {"version": info.version}

            # Add unreleased count and files if changelog folder exists
            if info.has_changelog_folder:
                branch_entry["unreleased_count"] = len(info.unreleased_files)
                # Don't include all unreleased files in JSON, keep it clean
            else:
                branch_entry["has_changelog_folder"] = False

            # Add feature distribution info if available for this branch
            if info.name in analysis_by_branch:
                analysis_key, analysis_data = analysis_by_branch[info.name]
                branch_entry["id"] = analysis_key
                if "count" in analysis_data:
                    branch_entry["new_count"] = analysis_data["count"]
                if "files" in analysis_data and info.has_changelog_folder:
                    branch_entry["new"] = analysis_data["files"]

            # Add files not in any newer versions
            if info.has_changelog_folder and info.not_in_newer:
                branch_entry["not_in_newer_count"] = len(info.not_in_newer)
                branch_entry["not_in_newer"] = sorted(info.not_in_newer)

            # Add duplicate issues if found for this branch
            if info.duplicate_issues:
                branch_entry["duplicate_issues"] = {
                    issue: sorted(files) for issue, files in info.duplicate_issues.items()
                }

            report_data["branch_report"][info.name] = branch_entry

        return json.dumps(report_data, indent=2)

    def print_report(self, analysis: Optional[Dict] = None, changelog_preview: Optional[str] = None):
        """Print/write the validation report.

        If report_file is set, writes to that file. Otherwise prints to stdout.
        If changelog_file is set, also writes the generated CHANGELOG.md to that file.

        Note: Info messages are printed live during validation, not repeated here.
        """
        # Generate report based on format
        if self.report_format == "json":
            report = self._generate_json_report(analysis)
        elif analysis:
            report = self.generate_report(analysis)
        else:
            report = self._generate_error_only_report()

        # Output report to file or stdout
        if self.report_file:
            self.report_file.write_text(report)
            # Always print errors to stdout so user is alerted even when writing to file
            if self.errors:
                print("ERRORS:")
                for error in self.errors:
                    print(f"  ✗ {self._format_error_for_display(error)}")
            if self.warnings:
                print("WARNINGS:")
                for warning in self.warnings:
                    print(f"  ⚠ {warning}")
            print(f"Report written to: {self.report_file}")
        else:
            print(report)

        # Write changelog preview if requested
        if changelog_preview and self.changelog_file:
            self.changelog_file.write_text(changelog_preview)
            print(f"Changelog written to: {self.changelog_file}")


def main():
    """Main entry point with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Validate Solr changelog structure across branches",
    )

    parser.add_argument(
        "-r", "--report-file",
        type=Path,
        help="File to write report to (default: stdout)",
        metavar="PATH",
    )

    parser.add_argument(
        "-c", "--changelog-file",
        type=Path,
        help="File to write generated CHANGELOG.md preview to",
        metavar="PATH",
    )

    parser.add_argument(
        "-w", "--work-dir",
        type=Path,
        help="Working directory (default TEMP dir)",
        metavar="PATH",
    )

    parser.add_argument(
        "--fetch-remote",
        action="store_true",
        help="Fetch fresh branch list from remote",
    )

    parser.add_argument(
        "-f", "--format",
        choices=["md", "json"],
        default="md",
        help="Report output format (default: md)",
    )

    parser.add_argument(
        "--skip-sync-check",
        action="store_true",
        help="Skip branch in sync validation",
    )

    parser.add_argument(
        "--check-duplicates",
        action="store_true",
        help="Check for duplicate JIRA issues",
    )

    args = parser.parse_args()

    # Create validator with provided options
    validator = ChangelogValidator(
        report_file=args.report_file,
        changelog_file=args.changelog_file,
        work_dir=args.work_dir,
        fetch_remote=args.fetch_remote,
        report_format=args.format,
        skip_sync_check=args.skip_sync_check,
        check_duplicates=args.check_duplicates,
    )

    success = validator.run()
    # JSON format always exits with 0, Markdown exits with 1 on errors
    if args.format == "json":
        sys.exit(0)
    else:
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
