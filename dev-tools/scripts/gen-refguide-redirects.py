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
Simple script that converts old refguide page names to new
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


def main():
    global conf
    conf = read_config()

    old = []
    new = {}
    name_map = {}

    out("Reading config")
    with open(conf.old, 'r') as fp:
        for line in fp.readlines():
            old.append(line.replace(".adoc", ".html").strip())
    with open(conf.new, 'r') as fp:
        for line in fp.readlines():
            l = line.replace(".adoc", ".html").strip()
            (path, file) = l.split("/")
            new[file] = l
    with open(conf.mapping, 'r') as fp:
        for line in fp.readlines():
            l = line.replace(".adoc", ".html").strip()
            (frm, to) = l.split(";")
            name_map[frm] = to

    # Files in src/old-pages as of 2022-02-04
    old_pages = ["configuration-apis.html", "configuration-guide.html", "controlling-results.html", "deployment-guide.html", "enhancing-queries.html", "field-types.html", "fields-and-schema-design.html", "getting-started.html", "indexing-data-operations.html", "installation-deployment.html", "monitoring-solr.html", "query-guide.html", "scaling-solr.html", "schema-indexing-guide.html", "solr-concepts.html", "solr-schema.html", "solrcloud-clusters.html", "user-managed-clusters.html"]

    result = {}
    failed = {}
    out("Converting...")
    for frm in old:
        if frm in new:
            result[frm] = new[frm]
        elif frm in name_map:
            new_name = name_map[frm]
            if new_name in new:
                result[frm] = new[new_name]
            else:
                failed[frm] = "There was mapping to %s, but it does not exist in new guide" % new_name
        elif frm in old_pages:
            failed[frm] = "Not yet mapped, is in src/old-pages"
        else:
            failed[frm] = "Not found in new guide, mappings or old-pages"

    if conf.htaccess:
        for key in result:
            print("RewriteRule /guide/%s /guide/%s [R=301,NE,L]" % (key, result[key]))
        for key in failed:
            print("# %s: %s" % (key, failed[key]))
    else:
        out("Successful mappings:")
        pprint(result)
        out("Failed mappings:")
        pprint(failed)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nReceived Ctrl-C, exiting early')
