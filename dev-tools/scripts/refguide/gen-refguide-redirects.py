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
Simple script that converts old refguide page names as of 8.11.1 to the new Antora URLs from 9.0
See input files in folder gen-refguide-redirects/

The old-guide.txt is the plain .adoc names from an 'ls | grep adoc' in old ref-guide src folder
The new-guide.txt is the output from this command from the new repo in the 'modules' folder:
    find . | grep adoc | sed 's/\/pages//g' | sed 's/^.\///g'
The mappings.csv comes from the explicit page renamings sourced from spreadsheet
  https://docs.google.com/spreadsheets/d/1mwxSpn5Ky7-P4DLFrJGel2h7Il4muTlHmAA-AuRY1rs/edit#gid=982988701
"""

import os
import sys
from pprint import pprint
sys.path.append(os.path.dirname(__file__))
import argparse


def read_config():
    parser = argparse.ArgumentParser(description='Convert old refguide page names to new')
    parser.add_argument('--old', required=True, help='Old pagenames file, one .adoc filename per line')
    parser.add_argument('--new', required=True, help='New pagenames file, one .adoc filename per line')
    parser.add_argument('--mapping', required=True, help='Semicolon separated from-to file names (adoc)')
    parser.add_argument('--htaccess', action='store_true', default=False, help='Output as htaccess rules')
    newconf = parser.parse_args()
    return newconf


def out(text):
    global conf
    if not conf.htaccess:
        print(text)


def lines_from_file(filename):
    with open(filename, 'r') as fp:
        lines = []
        for line in fp.readlines():
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            lines.append(line.replace(".adoc", ".html").strip())
        return lines


def main():
    global conf
    conf = read_config()

    new = {}
    name_map = {}

    out("Reading config")
    old = lines_from_file(conf.old)
    for line in lines_from_file(conf.new):
        (path, file) = line.split("/")
        new[file] = line
    for line in lines_from_file(conf.mapping):
        (frm, to) = line.split(";")
        name_map[frm] = to

    # Files in src/old-pages as of 2022-02-04
    old_pages = ["configuration-apis.html", "configuration-guide.html", "controlling-results.html", "deployment-guide.html", "enhancing-queries.html", "field-types.html", "fields-and-schema-design.html", "getting-started.html", "indexing-data-operations.html", "installation-deployment.html", "monitoring-solr.html", "query-guide.html", "scaling-solr.html", "schema-indexing-guide.html", "solr-concepts.html", "solr-schema.html", "solrcloud-clusters.html", "user-managed-clusters.html"]

    result = {}
    old_guide = []
    failed = {}
    regex_new = {}
    out("Converting...")
    for frm in old:
        if frm in new:
            (subpath, name) = new[frm].split("/")
            if subpath not in regex_new:
                regex_new[subpath] = []
            regex_new[subpath].append(name.split(".html")[0])
        elif frm in name_map:
            new_name = name_map[frm]
            new_name_without_anchor = new_name
            anchor = ""
            anchor_index = new_name.find("#")
            if anchor_index > 0:
                new_name_without_anchor = new_name[:anchor_index]
                anchor = new_name[anchor_index:]
            if new_name_without_anchor.startswith("https://"):
                result[frm] = new_name
            elif new_name_without_anchor in new:
                result[frm] = new[new_name_without_anchor] + anchor
            elif new_name_without_anchor.startswith("/guide/"):
                result[frm] = new_name[7:]
            elif new_name_without_anchor == "_8_11":
                old_guide.append(frm.split(".html")[0])
            else:
                failed[frm] = "Mapped value %s not in new guide" % new_name_without_anchor
        elif frm in old_pages:
            failed[frm] = "Not yet mapped (in src/old-pages)"
        else:
            failed[frm] = "404"

    if conf.htaccess:
        print("# Existing pages moved to sub path in the 9.0 guide")
        for key in regex_new:
            print("RedirectMatch 301 ^/guide/(%s)\.html /guide/solr/latest/%s/$1.html" % ("|".join(regex_new[key]), key))
        print("# Page renames between 8.x and 9.0")
        print("RewriteRule ^guide/9_0/solr-tutorial.html /guide/solr/latest/getting-started/solr-tutorial.html [R=301,NE,L]")
        for key in result:
            if result[key].startswith("https://"):
                print("RewriteRule ^guide/%s %s [R=301,NE,L]" % (key, result[key]))
            else:
                print("RewriteRule ^guide/%s /guide/solr/latest/%s [R=301,NE,L]" % (key, result[key]))
        print("# Removed pages redirected to latest 8.x guide")
        old_version_pages_regex = "(%s)\.html" % "|".join(old_guide)
        print("RedirectMatch 301 ^/guide/%s /guide/8_11/$1.html" % old_version_pages_regex)
        print("# Paths we could not map")
        for key in failed:
            print("# %s: %s" % (key, failed[key]))

        print("""

# Do not index old reference guide pages on search engines, except for pages that don't exist in 9+
<If "%%{REQUEST_URI} =~ m#/guide/(6|7|8)_.*# && %%{REQUEST_URI} !~ m#/guide/8_11/%s$#">
  Header set X-Robots-Tag "noindex,nofollow,noarchive"
</If>""" % old_version_pages_regex)
    else:
        out("Regex mappings:")
        pprint(regex_new)
        out("Rename mappings:")
        pprint(result)
        out("Old refGuide mappings:")
        pprint(old_guide)
        out("Failed mappings:")
        pprint(failed)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nReceived Ctrl-C, exiting early')
