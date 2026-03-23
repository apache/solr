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
Standalone smoke-test for Solr Maven artifacts: builds the test-external-client project
against locally published Solr artifacts to verify that solr-solrj and solr-test-framework
can be consumed from their published POMs.

This test can be run independently of the full smokeTestRelease.py suite.
It uses the test-external-client project checked into the repository root, which has
both Maven (pom.xml) and Gradle (build.gradle.kts) build files.

Usage examples:

  # Test against a local Maven repository (produced by "gradlew mavenToLocalFolder"):
  python3 checkMavenBuild.py --maven-dir build/maven-local 10.0.0

  # Test against a release-candidate Maven staging URL (artifacts are downloaded):
  python3 checkMavenBuild.py --url https://dist.apache.org/repos/dist/dev/solr/solr-10.0.0-RC1-rev-abc1234/maven 10.0.0

Requirements: Maven (mvn), Docker, or Gradle (gradlew) must be available.
"""

import argparse
import os
import sys
import tempfile
import textwrap

# Import utilities from smokeTestRelease located in the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import smokeTestRelease


def parse_config():
  epilog = textwrap.dedent('''
    Examples:
      # Use locally published Maven artifacts (run "gradlew mavenToLocalFolder" first):
      python3 checkMavenBuild.py --maven-dir build/maven-local 10.0.0

      # Download Maven artifacts from a release-candidate staging URL:
      python3 checkMavenBuild.py --url https://dist.apache.org/repos/dist/dev/solr/solr-10.0.0-RC1-rev-abc1234/maven 10.0.0
  ''')
  parser = argparse.ArgumentParser(
    description='Verify Solr Maven artifacts can be consumed by the test-external-client project.',
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter)

  parser.add_argument('version', metavar='X.Y.Z',
                      help='Solr version to test (e.g. 10.0.0)')

  source = parser.add_mutually_exclusive_group(required=True)
  source.add_argument('--maven-dir', metavar='DIR',
                      help='Local Maven repository directory whose root contains '
                           'org/apache/solr/ (e.g. the output of "gradlew mavenToLocalFolder")')
  source.add_argument('--url', metavar='URL',
                      help='URL of the Maven repository to test against '
                           '(artifacts will be downloaded into --tmp-dir)')

  parser.add_argument('--tmp-dir', metavar='DIR',
                      help='Temporary directory for downloaded artifacts and log files '
                           '(default: auto-generated under /tmp)')

  parser.add_argument('--java-home', metavar='DIR',
                      help='Path to a Java 21+ home directory (default: uses the JAVA_HOME '
                           'environment variable)')

  return parser.parse_args()


def main():
  c = parse_config()

  if c.tmp_dir:
    tmp_dir = os.path.abspath(c.tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)
  else:
    tmp_dir = tempfile.mkdtemp(prefix='solr_maven_check_%s_' % c.version)

  print('Using tmp dir: %s' % tmp_dir)

  java_home = c.java_home or os.environ.get('JAVA_HOME')

  if c.maven_dir:
    maven_dir = os.path.abspath(c.maven_dir)
    print('Using local Maven repository: %s' % maven_dir)
  else:
    # Download Maven artifacts from the given URL
    maven_dir = os.path.join(tmp_dir, 'maven')
    target_dir = os.path.join(maven_dir, 'org', 'apache', 'solr')
    os.makedirs(target_dir, exist_ok=True)
    artifacts_url = c.url.rstrip('/') + '/org/apache/solr/'
    print('Downloading Maven artifacts from %s' % artifacts_url)
    smokeTestRelease.crawl([], artifacts_url, target_dir)
    print()

  smokeTestRelease.testMavenBuild(maven_dir, tmp_dir, c.version, javaHome=java_home)

  print('\nSUCCESS!')


if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Keyboard interrupt...exiting')

