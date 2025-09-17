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
Script to add solrbot changes lines to CHANGES.txt
"""
import os
import sys

sys.path.append(os.path.dirname(__file__))
from scriptutil import *

import argparse
import re

line_re = re.compile(r"(.*?) (\(branch_\d+x\) )?\(#(\d+)\)$")


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
    parser = argparse.ArgumentParser(description='Adds dependency changes section to CHANGES.txt.')
    parser.add_argument('--version', type=Version.parse, help='Solr version to add changes to', required=True)
    parser.add_argument('--user', default='solrbot', help='Git user to get changes for. Defaults to solrbot')
    newconf = parser.parse_args()
    return newconf


def gitlog_to_changes(line, user="solrbot"):
    """
    Converts a git log formatted line ending in (#<pr-num) into a CHANGES style line.
    Strips any '(branch_Nx)' text from the commit message, as that is not needed in CHANGES.txt
    """
    match = line_re.search(line)

    if match:
        text = match.group(1)
        pr_num = match.group(3)
        return "* PR#%s: %s (%s)\n" % (pr_num, text, user)
    else:
        print("Skipped un-parsable line: %s" % line)
        return ""


def update_changes(filename, version, changes_lines):
    """
    Edits CHANGES.txt in-place
    """
    buffer = []
    found_ver = False
    found_header = False
    checked_no_changes = False
    appended = False
    with open(filename) as f:
        version_re = re.compile(r' %s ===' % (version))
        header_re = re.compile(r'^Dependency Upgrades')
        header_line_re = re.compile(r'^----')
        for line in f:
            if not found_ver:
                buffer.append(line)
                if version_re.search(line):
                    found_ver = True
                continue
            if not found_header:
                buffer.append(line)
                if header_re.search(line):
                    found_header = True
                continue
            if not appended:
                if header_line_re.search(line):
                    buffer.append(line)
                    appended = True
                    for change_line in changes_lines:
                        buffer.append(change_line)
                        buffer.append("\n")
                    continue
                else:
                    print("Mismatch in CHANGES.txt, expected '----' line after header, got: %s" % line)
                    exit(1)
            if not checked_no_changes:
                checked_no_changes = True
                if re.compile(r'^\(No changes\)').search(line):
                    continue
            buffer.append(line)
    if appended:
        with open(filename, 'w') as f:
            f.write(''.join(buffer))
    else:
        if not found_ver:
            print("Did not find version %s in CHANGES.txt" % version.dot)
        elif not found_header:
            print("Did not find header 'Dependency Upgrades' under version %s in CHANGES.txt" % version.dot)
        exit(1)


def dedupe(changes_lines):
    """
    De-duplicate dependency update lines.
    Strategy: keep only the latest (newest) update for a given dependency key.
    The input list is assumed to be in reverse chronological order (newest first),
    as produced by `git log` without `--reverse`.

    We extract a dependency key from the message part between "Update " and
    either " to", " (" or end-of-string. If that can't be found, we fall back
    to using the whole message to avoid accidental merges.
    """
    seen = set()
    deduped = []

    # Regex to capture the message portion inside the CHANGES line
    # Example line: "* PR#1600: Update io.netty:* to v4.3 (solrbot)"
    msg_re = re.compile(r"^\* PR#\d+: (.*) \([^)]+\)\s*$")
    # From the message, extract the dependency part after "Update "
    dep_re = re.compile(r"[Uu]pdate( dependency)?\s+(.+?)(?:\s+to\b|\s*\(|$)")

    for line in changes_lines:
        msg_match = msg_re.match(line.strip())
        message = msg_match.group(1) if msg_match else line.strip()
        # Normalize: drop optional leading 'chore(deps): ' prefix which may or may not be present
        normalized = re.sub(r"^chore\(deps\):\s*", "", message)

        dep_match = dep_re.search(normalized)
        if dep_match:
            key = dep_match.group(2).strip()
        else:
            # Fallback: use entire message (normalized) as the key to avoid collapsing unrelated lines
            key = normalized

        if key not in seen:
            seen.add(key)
            deduped.append(line)
        # else: skip older updates for the same dependency key

    # Finally, sort lines alphabetically by the dependency name (case-insensitive),
    # ignoring the leading '* PR#1234:' part (via msg_re) and an optional leading
    # 'chore(deps): ' prefix in the message. Also normalize both
    # 'Update foo' and 'Update dependency foo' to just 'foo' when sorting.
    def _sort_key(l):
        m = msg_re.match(l.strip())
        msg = m.group(1) if m else l.strip()
        normalized_msg = re.sub(r"^chore\(deps\):\s*", "", msg)
        dep_match_sort = dep_re.search(normalized_msg)
        if dep_match_sort:
            return dep_match_sort.group(2).strip().lower()
        return normalized_msg.lower()
    deduped.sort(key=_sort_key)
    return deduped


def main():
    if not os.path.exists('solr/CHANGES.txt'):
        sys.exit("Tool must be run from the root of a source checkout.")
    newconf = read_config()
    prev_tag = get_prev_release_tag(newconf.version)
    print("Adding dependency updates since git tag %s" % prev_tag)
    try:
        gitlog_lines = run(
            'git log --author=' + newconf.user + ' --oneline --no-merges --pretty=format:"%s" ' + prev_tag + '..').split(
            "\n")
        changes_lines = list(map(lambda l: gitlog_to_changes(l, newconf.user), list(filter(None, gitlog_lines))))
        if changes_lines and len(changes_lines) > 0:
            update_changes('solr/CHANGES.txt', newconf.version, dedupe(changes_lines))
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
