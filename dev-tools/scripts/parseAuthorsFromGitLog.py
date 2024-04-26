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
Script to parse authors and co-authors from git log output
"""
import os
import sys

sys.path.append(os.path.dirname(__file__))
from scriptutil import *

import argparse
import re

line_re = re.compile(r"(.*?) \(#(\d+)\)$")


def get_prev_release_tag(ver):
    """
    Based on a given version, compute the git tag for the "previous" version to calculate changes since.
    For a major version, we want all commits since last release, i.e. X-1.Y.Z
    For a minor version, we want all commits since X.Y-1.0
    For a patch version, we want all commits since X.Y.Z-1
    TODO: This is duplicated in addDepsToChanges.py
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
    parser = argparse.ArgumentParser(description='Parse authors and co-authors from git log output.')
    parser.add_argument('--version', type=Version.parse, help='Solr version to select commits for', required=True)
    newconf = parser.parse_args()
    return newconf


def parse_authors(log):
    """
    Parse git log and extract authors and co-authors.

    Args:
    - log: String containing the git log.

    Returns:
    - Dictionary containing author names as keys and number of contributions as values.
    """
    authors = set()
    author_names = {}

    # Split the log into individual commits based on the start of each commit.
    commits = re.split(r'\ncommit [a-f0-9]', log)

    def add_to_author_names(author_line):
        name = author_line.split(' <')[0].strip()
        if name in author_names:
            author_names[name] += 1
        else:
            author_names[name] = 1

    for commit in commits:
        # Split the commit message into lines.
        lines = commit.split('\n')

        # Get the author from the first line starting with "Author:".
        author_line = next((line for line in lines if line.lower().startswith('author:')), None)
        if author_line:
            author = author_line.split(':')[1].strip()
            authors.add(author)
            add_to_author_names(author)

        # Get the co-authors from lines starting with "Co-authored-by:".
        co_author_lines = [line for line in lines if line.lower().startswith('    co-authored-by:')]
        for line in co_author_lines:
            co_author = line.split(':')[1].strip()
            authors.add(co_author)
            add_to_author_names(co_author)

    return author_names


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    if not os.path.exists('solr/CHANGES.txt'):
        sys.exit("Tool must be run from the root of a source checkout.")
    newconf = read_config()
    prev_tag = get_prev_release_tag(newconf.version)
    eprint("Selecting git log since %s" % prev_tag)
    try:
        gitlog = run(
            'git log --no-merges ' + prev_tag + '..')
        authors = parse_authors(gitlog)
        # Sort authos by number of contributions
        authors = [f"{k} ({v})" for k, v in sorted(authors.items(), key=lambda item: item[1], reverse=True)]
        if len(authors) > 0:
            print("\n".join(authors))
        else:
            epring("No authors found")
    except subprocess.CalledProcessError:
        print("Error running git log - check your --version")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nReceived Ctrl-C, exiting early')
