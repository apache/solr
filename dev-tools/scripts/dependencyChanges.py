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
Summarizes changes to the third-party dependencies Solr ships, by diffing the
jar checksum files in solr/licenses/ between two git refs (by default: the
previous release tag and HEAD).  Jars that moved between the same two versions
are listed together, since they are typically from the same project.

Prints the summary to stdout, or with --write, writes changelog YAML entries.

This is a stopgap until Solr publishes an SBOM per release, which would be the
better source to diff.
"""

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))
from scriptutil import Version, find_current_version

LICENSES_DIR = 'solr/licenses'
DEFAULT_OUTPUT_DIR = 'changelog/unreleased'

# A version starts at the last '-'-delimited segment that looks like a dotted number.
# e.g. log4j-1.2-api-2.25.3, zstd-jni-1.5.6-10, guava-33.4.8-jre
VERSION_SEG_RE = re.compile(r'^\d+\.\d')
# Classifiers that distinguish separate jars of the same artifact and version
CLASSIFIER_RE = re.compile(r'-(tests?|linux-[\w-]+|osx-[\w-]+|windows-[\w-]+)$')
# Test artifacts sharing a LICENSE file with shipped ones (e.g. lucene-test-framework)
TEST_ARTIFACT_RE = re.compile(r'-(tests?|testing|test-framework)$')


def git(*args):
  return subprocess.run(['git'] + list(args), capture_output=True, text=True, check=True).stdout


def parse_jar(jar_name):
  """Returns (artifact, version) for a jar name without the '.jar' extension."""
  classifier = ''
  m = CLASSIFIER_RE.search(jar_name)
  if m:
    classifier = m.group(0)
    jar_name = jar_name[:m.start()]
  segs = jar_name.split('-')
  idx = max((i for i, s in enumerate(segs) if i > 0 and VERSION_SEG_RE.match(s)), default=None)
  if idx is None:
    idx = max((i for i, s in enumerate(segs) if i > 0 and s[:1].isdigit()), default=len(segs))
  return '-'.join(segs[:idx]) + classifier, '-'.join(segs[idx:])


def read_licenses_dir(ref):
  """Returns (artifact -> version, set of LICENSE file prefixes) at the given ref."""
  jars = {}
  license_prefixes = set()
  for name in git('ls-tree', '--name-only', f'{ref}:{LICENSES_DIR}').split():
    if name.endswith('.jar.sha1'):
      artifact, version = parse_jar(name[:-len('.jar.sha1')])
      jars[artifact] = version
    else:
      m = re.match(r'(.+)-LICENSE-.+\.txt$', name)
      if m:
        license_prefixes.add(m.group(1))
  return jars, license_prefixes


def has_license(artifact, license_prefixes):
  """Whether some '-'-delimited prefix of the artifact has a LICENSE file (mirrors jar-checks.gradle)."""
  name = CLASSIFIER_RE.sub('', artifact)
  while True:
    if name in license_prefixes:
      return True
    prefix = re.sub(r'-[^-]+$', '', name)
    if prefix == name:
      return False
    name = prefix


def find_previous_release_tag(current):
  cur = Version.parse(current)
  best = None
  for tag in git('tag', '-l', 'releases/solr/*').split():
    m = re.fullmatch(r'releases/solr/(\d+)\.(\d+)\.(\d+)', tag)
    if not m:
      continue
    v = tuple(int(x) for x in m.groups())
    if v < (cur.major, cur.minor, cur.bugfix) and (best is None or v > best[0]):
      best = (v, tag)
  if best is None:
    sys.exit(f'No release tag found before {current}; pass --from')
  return best[1]


def describe_artifacts(artifacts):
  """e.g. 'asm, asm-tree', or 'jetty-* (23 jars)' when all share the first name segment."""
  if len(artifacts) > 3:
    first = {a.split('-')[0] for a in artifacts}
    if len(first) == 1:
      return f'{first.pop()}-* ({len(artifacts)} jars)'
  return ', '.join(artifacts)


CATEGORIES = ('added', 'upgraded', 'removed')


def compute_changes(from_ref, to_ref):
  """Returns category ('added', 'upgraded', 'removed') -> sorted list of human-readable changes."""
  old_jars, old_license_prefixes = read_licenses_dir(from_ref)
  new_jars, new_license_prefixes = read_licenses_dir(to_ref)

  # Only jars with a LICENSE file count: *.sha1 files also cover jars we don't ship (e.g. test
  # dependencies), whereas LICENSE files are only required for shipped ones (since SOLR-15465; older
  # releases have them for non-shipped jars too, making removals from such a release unreliable).
  def shipped(artifact, license_prefixes):
    return has_license(artifact, license_prefixes) and not TEST_ARTIFACT_RE.search(artifact)

  by_transition = defaultdict(list)  # (old_version or None, new_version or None) -> artifacts
  for artifact in old_jars.keys() | new_jars.keys():
    old, new = old_jars.get(artifact), new_jars.get(artifact)
    if old == new:
      continue
    if shipped(artifact, new_license_prefixes if new else old_license_prefixes):
      by_transition[(old, new)].append(artifact)

  changes = {c: [] for c in CATEGORIES}
  for (old, new), artifacts in by_transition.items():
    names = describe_artifacts(sorted(artifacts, key=str.lower))
    if old is None:
      changes['added'].append(f'{names} {new}')
    elif new is None:
      changes['removed'].append(f'{names} {old}')
    else:
      changes['upgraded'].append(f'{names} {old} → {new}')
  return {c: sorted(lines, key=str.lower) for c, lines in changes.items()}


def to_yaml(title, version):
  def quote(s):
    return "'" + s.replace("'", "''") + "'"
  return (f'# Generated by dev-tools/scripts/dependencyChanges.py\n'
          f'title: {quote(title)}\n'
          f'type: dependency_update\n'
          f'authors:\n'
          f'  - name: various contributors\n'
          f'links:\n'
          f'  - name: solr/licenses\n'
          f'    url: https://github.com/apache/solr/tree/releases/solr/{version}/solr/licenses\n')


def main():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--from', dest='from_ref',
                      help='Git ref to compare from (default: the release tag preceding the current version)')
  parser.add_argument('--to', dest='to_ref', default='HEAD', help='Git ref to compare to (default: HEAD)')
  parser.add_argument('--write', nargs='?', const=DEFAULT_OUTPUT_DIR, metavar='DIR',
                      help='Write changelog YAML entries (one per category) instead of printing'
                           f' (default dir: {DEFAULT_OUTPUT_DIR})')
  args = parser.parse_args()

  version = find_current_version()
  from_ref = args.from_ref or find_previous_release_tag(version)
  changes = compute_changes(from_ref, args.to_ref)
  since = from_ref.rsplit('/', 1)[-1]

  if not args.write:
    print(f'Dependency changes from {from_ref} to {args.to_ref}:')
    for category in CATEGORIES:
      print(f'{category.capitalize()}: ' + ('; '.join(changes[category]) or '(none)'))
    return

  for i, category in enumerate(CATEGORIES, 1):  # numbered so the changelog lists them in this order
    path = os.path.join(args.write, f'dependency-changes-{i}-{category}.yml')
    if changes[category]:
      title = f'Third-party dependencies {category} since {since}: ' + '; '.join(changes[category])
      with open(path, 'w', encoding='utf-8') as f:
        f.write(to_yaml(title, version))
      print(f'Wrote {len(changes[category])} {category} dependency changes to {path}')
    elif os.path.exists(path):
      os.remove(path)
      print(f'Removed {path}; no {category} dependency changes')


if __name__ == '__main__':
  main()
