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
from textwrap import dedent

def update_build(file_path, search_re, replace_line):
  print('adding contrib into %s' % file_path)
  matcher = re.compile(search_re)

  def edit(buffer, match, line):
    if replace_line in line:
      return None
    match = matcher.search(line)
    if match is not None:
      buffer.append(replace_line)
    buffer.append(line)
    return match is not None

  changed = update_file(file_path, matcher, edit)
  print('done' if changed else 'uptodate')


def read_config():
  parser = argparse.ArgumentParser(description='Scaffold new contrib module')
  parser.add_argument("name")
  parser.add_argument("full_name")
  parser.add_argument("description")
  newconf = parser.parse_args()
  return newconf


def get_readme_skel(conrib_name):
  return dedent('''Apache Solr %s
=====================================

Introduction
------------
TBD

Getting Started
---------------
TBD
''' % conrib_name)

def get_license_header():
  return '''/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'''

def get_build_gradle(description):
  return dedent('''apply plugin: 'java-library'

description = '%s'

configurations.all {
  exclude group: 'log4j', module: 'log4j'
  exclude group: 'org.slf4j', module: 'slf4j-log4j12'
  exclude group: 'org.apache.yetus', module: 'audience-annotations'
  // be conservative on what's added here.  Affects *all* configs, including internal ones.
}

dependencies {
  implementation project(':solr:core')
  implementation 'org.slf4j:slf4j-api'
  
  testImplementation project(':solr:test-framework')
}
''' % description)

def get_overview_tpl(name):
  return dedent('''<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<html>
<body>
Apache Solr Search Server: %s
</body>
</html>
''' % name)

def scaffold_folder(contrib_name, contrib_full_name, contrib_folder, contrib_description):
  print("\nScaffolding folder %s" % contrib_folder)
  os.makedirs(contrib_folder)
  readme = os.path.join(contrib_folder, 'README.md')
  with open(readme, 'w') as fp:
    fp.write(get_readme_skel(contrib_full_name))
  build = os.path.join(contrib_folder, 'build.gradle')
  with open(build, 'w') as fp:
    fp.write (get_license_header())
    fp.write (get_build_gradle(contrib_description))
  src_java_folder = os.path.join(contrib_folder, 'src', 'java')
  os.makedirs(src_java_folder)
  overview = os.path.join(src_java_folder, 'overview.html')
  with open(overview, 'w') as fp:
    fp.write (get_overview_tpl(contrib_full_name))

  os.makedirs(os.path.join(contrib_folder, 'src', 'resources'))
  os.makedirs(os.path.join(contrib_folder, 'src', 'test-files'))
  os.makedirs(os.path.join(contrib_folder, 'src', 'test'))

  update_build(os.path.join('solr', 'packaging', 'build.gradle'),
               r':solr:contrib:extraction',
               '   ":solr:contrib:%s",\n' % contrib_name)
  update_build(os.path.join('gradle', 'maven', 'defaults-maven.gradle'),
               r':solr:contrib:extraction',
               '        ":solr:contrib:%s",\n' % contrib_name)
  update_build(os.path.join('settings.gradle'),
               r'include "solr:contrib:extraction"',
               'include "solr:contrib:%s"\n' % contrib_name)

def main():
  conf = read_config()
  contrib_name = conf.name

  contrib_folder = os.path.join('solr', 'contrib', contrib_name)
  scaffold_folder(contrib_name, conf.full_name, contrib_folder, conf.description)


if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')
