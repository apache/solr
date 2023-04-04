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

import os
import sys
sys.path.append(os.path.dirname(__file__))
from scriptutil import *

import argparse
import re
from configparser import ConfigParser, ExtendedInterpolation
from textwrap import dedent

def update_changes(filename, new_version, init_changes, headers):
  print('  adding new section to %s...' % filename, end='', flush=True)
  matcher = re.compile(r'\d+\.\d+\.\d+\s+===')
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    match = new_version.previous_dot_matcher.search(line)
    if match is not None:
      buffer.append(line.replace(match.group(0), new_version.dot))
      buffer.append(init_changes)
      for header in headers:
        buffer.append('%s\n---------------------\n(No changes)\n\n' % header)
    buffer.append(line)
    return match is not None
     
  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def update_solrversion_class(new_version):
  filename = 'solr/core/src/java/org/apache/solr/util/SolrVersion.java'
  print('  changing version to %s...' % new_version.dot, end='', flush=True)
  constant_prefix = 'public static final String LATEST_STRING = "(.*?)"'
  matcher = re.compile(constant_prefix)

  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    buffer.append(line.replace(match.group(1), new_version.dot))
    return True
  
  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def update_build_version(new_version):
  print('  changing baseVersion...', end='', flush=True)
  filename = 'build.gradle'
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    buffer.append('  String baseVersion = \'' + new_version.dot + '\'\n')
    return True 

  changed = update_file(filename, version_prop_re, edit)
  print('done' if changed else 'uptodate')


def onerror(x):
  raise x

def update_example_solrconfigs(new_version):
  print('  updating example solrconfig.xml files with lucene version %s' % new_version)
  matcher = re.compile('<luceneMatchVersion>(.*?)</luceneMatchVersion>')

  paths = ['solr/server/solr/configsets', 'solr/example']
  for path in paths:
    print("   Patching configset folder %s" % path)
    if not os.path.isdir(path):
      raise RuntimeError("Can't locate configset dir (layout change?) : " + path)
    for root,dirs,files in os.walk(path, onerror=onerror):
      for f in files:
        if f == 'solrconfig.xml':
          update_solrconfig(os.path.join(root, f), matcher, new_version)

def update_solrconfig(filename, matcher, new_version):
  print('    %s...' % filename, end='', flush=True)
  def edit(buffer, match, line):
    if new_version in line:
      return None
    match = matcher.search(line)
    if match is None:
      return False
    buffer.append(line.replace(match.group(1), new_version))
    return True

  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def check_solr_version_class_tests():
  print('  checking solr version tests...', end='', flush=True)
  run('./gradlew -p solr/core test --tests TestSolrVersion')
  print('ok')

def check_lucene_match_version_tests():
  print('  checking solr version tests...', end='', flush=True)
  run('./gradlew -p solr/core test --tests TestLuceneMatchVersion')
  print('ok')

def read_config(current_version, current_lucene_version):
  parser = argparse.ArgumentParser(description='Add a new version to CHANGES, to Version.java, build.gradle and solrconfig.xml files')
  parser.add_argument('version', type=Version.parse, help='New Solr version')
  parser.add_argument('-l', dest='lucene_version', type=Version.parse, help='Optional lucene version. By default will read versions.props')
  newconf = parser.parse_args()
  if not newconf.lucene_version:
    newconf.lucene_version = current_lucene_version
  print('Using lucene_version %s' % current_lucene_version)

  newconf.branch_type = find_branch_type()
  newconf.is_latest_version = newconf.version.on_or_after(current_version)

  print ("branch_type is %s " % newconf.branch_type)

  return newconf

# Hack ConfigParser, designed to parse INI files, to parse & interpolate Java .properties files
def parse_properties_file(filename):
  contents = open(filename, encoding='ISO-8859-1').read().replace('%', '%%') # Escape interpolation metachar
  parser = ConfigParser(interpolation=ExtendedInterpolation())               # Handle ${property-name} interpolation
  parser.read_string("[DUMMY_SECTION]\n" + contents)                         # Add required section
  return dict(parser.items('DUMMY_SECTION'))

def get_solr_init_changes():
  return ''
  
def main():
  if not os.path.exists('build.gradle'):
    sys.exit("Tool must be run from the root of a source checkout.")
  current_version = Version.parse(find_current_version())
  current_lucene_version = Version.parse(find_current_lucene_version())
  newconf = read_config(current_version, current_lucene_version)
  is_bugfix = newconf.version.is_bugfix_release()

  print('\nAdding new version %s' % newconf.version)
  update_changes('solr/CHANGES.txt', newconf.version, get_solr_init_changes(),
                 ['Bug Fixes', 'Dependency Upgrades'] if is_bugfix else ['New Features', 'Improvements', 'Optimizations', 'Bug Fixes', 'Dependency Upgrades', 'Other Changes'])

  if newconf.is_latest_version:
    print('\nAdded version is latest version, updating...')
    update_build_version(newconf.version)
    update_solrversion_class(newconf.version)
    if newconf.version.is_major_release or newconf.version.is_minor_release():
      # Update solrconfig.xml with new <luceneMatchVersion>major.minor</luceneMatchVersion> for major/minor releases
      update_example_solrconfigs("%d.%d" % (newconf.lucene_version.major, newconf.lucene_version.minor))

    print('\nTesting changes')
    check_solr_version_class_tests()
    check_lucene_match_version_tests()
  else:
    print('\nNot updating build.gradle, SolrVersion or solrconfig.xml since version added (%s) is not latest version' % newconf.version)

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')
