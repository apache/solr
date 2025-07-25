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

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import json
from enum import Enum


class Version(object):
  def __init__(self, major, minor, bugfix, prerelease):
    self.major = major
    self.minor = minor
    self.bugfix = bugfix
    self.prerelease = prerelease
    self.previous_dot_matcher = self.make_previous_matcher()
    self.dot = '%d.%d.%d' % (self.major, self.minor, self.bugfix) 
    self.constant = 'LUCENE_%d_%d_%d' % (self.major, self.minor, self.bugfix)

  @classmethod
  def parse(cls, value):
    match = re.search(r'(\d+)\.(\d+).(\d+)(.1|.2)?', value) 
    if match is None:
      raise argparse.ArgumentTypeError('Version argument must be of format x.y.z(.1|.2)?')
    parts = [int(v) for v in match.groups()[:-1]]
    parts.append({ None: 0, '.1': 1, '.2': 2 }[match.groups()[-1]])
    return Version(*parts)

  def __str__(self):
    return self.dot

  def make_previous_matcher(self, prefix='', suffix='', sep='\\.'):
    if self.is_bugfix_release():
      pattern = '%s%s%s%s%d' % (self.major, sep, self.minor, sep, self.bugfix - 1)
    elif self.is_minor_release():
      pattern = '%s%s%d%s\\d+' % (self.major, sep, self.minor - 1, sep)
    else:
      pattern = '%d%s\\d+%s\\d+' % (self.major - 1, sep, sep)

    return re.compile(prefix + '(' + pattern + ')' + suffix)

  def is_bugfix_release(self):
    return self.bugfix != 0

  def is_minor_release(self):
    return self.bugfix == 0 and self.minor != 0

  def is_major_release(self):
    return self.bugfix == 0 and self.minor == 0

  def on_or_after(self, other):
    return (self.major > other.major or self.major == other.major and
           (self.minor > other.minor or self.minor == other.minor and
           (self.bugfix > other.bugfix or self.bugfix == other.bugfix and
           self.prerelease >= other.prerelease)))

  def gt(self, other):
    return (self.major > other.major or
           (self.major == other.major and self.minor > other.minor) or
           (self.major == other.major and self.minor == other.minor and self.bugfix > other.bugfix))

  def is_back_compat_with(self, other):
    if not self.on_or_after(other):
      raise Exception('Back compat check disallowed for newer version: %s < %s' % (self, other))
    return other.major + 1 >= self.major


class CommitterPgp():
    """
    A class to resolve a commiter's committer's PGP key from ASF records.
    The class looks up the committer's ASF id in a json file downloaded from
    https://whimsy.apache.org/public/public_ldap_people.json
    and if the committer has a PGP key, it's fingerprint is made available.
    """
    def __init__(self, asf_id, json_content = None):
        self.asf_id = asf_id
        self.fingerprint = None
        self.ldap_url = 'https://whimsy.apache.org/public/public_ldap_people.json'
        self.fingerprint = None
        self.fingerprint_short = None
        if json_content:
            self.ldap_json = json_content
        else:
            self.ldap_json = self.load_ldap()
        self.resolve()


    def load_ldap(self):
        try:
            with urllib.request.urlopen(self.ldap_url) as f:
                return json.load(f)
        except urllib.error.HTTPError as e:
            raise Exception(f'Failed to load {self.ldap_url}: {e}')


    def resolve(self):
        """ Resolve the PGP key fingerprint for the committer's ASF id """
        try:
            self.fingerprint = self.ldap_json['people'][self.asf_id]['key_fingerprints'][0].replace(" ", "").upper()
            self.fingerprint_short = self.fingerprint[-8:]
        except KeyError:
            raise Exception(f'No PGP key found for {self.asf_id}')


    def get_fingerprint(self):
        return self.fingerprint


    def get_short_fingerprint(self):
        return self.fingerprint_short


def run(cmd, cwd=None):
  try:
    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, cwd=cwd)
  except subprocess.CalledProcessError as e:
    print(e.output.decode('utf-8'))
    raise e
  return output.decode('utf-8') 

def update_file(filename, line_re, edit):
  infile = open(filename, 'r')
  buffer = [] 
  
  changed = False
  for line in infile:
    if not changed:
      match = line_re.search(line)
      if match:
        changed = edit(buffer, match, line)
        if changed is None:
          return False
        continue
    buffer.append(line)
  if not changed:
    raise Exception('Could not find %s in %s' % (line_re, filename))
  with open(filename, 'w') as f:
    f.write(''.join(buffer))
  return True


# branch types are "release", "stable" and "unstable"
class BranchType(Enum):
  unstable = 1
  stable   = 2
  release  = 3

def find_branch_type():
  output = subprocess.check_output('git status', shell=True)
  for line in output.split(b'\n'):
    if line.startswith(b'On branch '):
      branchName = line.split(b' ')[-1]
      break
  else:
    raise Exception('git status missing branch name')

  if branchName == b'main':
    return BranchType.unstable
  if re.match(r'branch_(\d+)x', branchName.decode('UTF-8')):
    return BranchType.stable
  if re.match(r'branch_(\d+)_(\d+)', branchName.decode('UTF-8')):
    return BranchType.release
  raise Exception('Cannot run %s on feature branch' % sys.argv[0].rsplit('/', 1)[-1])


def download(name, urlString, tmpDir, quiet=False, force_clean=True):
  if not quiet:
      print("Downloading %s" % urlString)
  startTime = time.time()
  fileName = '%s/%s' % (tmpDir, name)
  if not force_clean and os.path.exists(fileName):
    if not quiet and fileName.find('.asc') == -1:
      print('    already done: %.1f MB' % (os.path.getsize(fileName)/1024./1024.))
    return
  try:
    attemptDownload(urlString, fileName)
  except Exception as e:
    print('Retrying download of url %s after exception: %s' % (urlString, e))
    try:
      attemptDownload(urlString, fileName)
    except Exception as e:
      raise RuntimeError('failed to download url "%s"' % urlString) from e
  if not quiet and fileName.find('.asc') == -1:
    t = time.time()-startTime
    sizeMB = os.path.getsize(fileName)/1024./1024.
    print('    %.1f MB in %.2f sec (%.1f MB/sec)' % (sizeMB, t, sizeMB/t))


def attemptDownload(urlString, fileName):
  raw_request = urllib.request.Request(urlString)
  raw_request.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:78.0) Gecko/20100101 Firefox/78.0')
  raw_request.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
  fIn = urllib.request.urlopen(raw_request)
  fOut = open(fileName, 'wb')
  success = False
  try:
    while True:
      s = fIn.read(65536)
      if s == b'':
        break
      fOut.write(s)
    fOut.close()
    fIn.close()
    success = True
  finally:
    fIn.close()
    fOut.close()
    if not success:
      os.remove(fileName)

version_prop_re = re.compile(r'baseVersion\s*=\s*([\'"])(.*)\1')

lucene_version_prop_re = re.compile(r'apache-lucene\s*=\s*"([a-zA-Z0-9\.\-]+)"')

def find_current_version():
  script_path = os.path.dirname(os.path.realpath(__file__))
  top_level_dir = os.path.join(os.path.abspath("%s/" % script_path), os.path.pardir, os.path.pardir)
  return version_prop_re.search(open('%s/build.gradle' % top_level_dir).read()).group(2).strip()


def find_current_lucene_version():
  script_path = os.path.dirname(os.path.realpath(__file__))
  top_level_dir = os.path.join(os.path.abspath("%s/" % script_path), os.path.pardir, os.path.pardir)
  versions_file = open('%s/gradle/libs.versions.toml' % top_level_dir).read()
  return lucene_version_prop_re.search(versions_file).group(1).strip()


if __name__ == '__main__':
  print('This is only a support module, it cannot be run')
  sys.exit(1)
