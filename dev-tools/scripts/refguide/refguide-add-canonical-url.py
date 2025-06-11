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
This script processes all static html files for Solr's reference guide
and adds canonical URLs for old pages (Solr 6 - Solr 8, Solr 9+ should not
be affected). Since Google doesn't always respect the canonical URL
directive, the meta tag for robots "noindex" is also added to ensure these
outdated pages do not show up on Google search results.
This script uses the same logic as the htaccess generation script to
determine which pages are the "last" versions of that page, so that it can
be indexed by google as the most recent information.
"""

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import argparse

robots_no_index_html = "<meta name=\"robots\" content=\"noindex\">"

def lines_from_file(filename):
    with open(filename, 'r') as fp:
        lines = []
        for line in fp.readlines():
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            lines.append(line.replace(".adoc", ".html").strip())
        return lines

def generate_canonical_mapping(conf):
    new = {}
    name_map = {}

    print("Reading config")
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
    print("Converting...")
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

    mappings = {
        "index.html": "https://solr.apache.org/guide/solr/latest/index.html",

    }
    # Add direct mappings from old to new files
    for key in regex_new:
        for file in regex_new[key]:
            mappings[file + ".html"] = f"https://solr.apache.org/guide/solr/latest/{key}/{file}.html"

    # Add mappings for renamed files
    for key in result:
        if result[key].startswith("https://"):
            mappings[key] = result[key]
        else:
            mappings[key] = f"https://solr.apache.org/guide/solr/latest/{result[key]}"

    # Add mappings for files removed in 9.0, they will be canonical to 8.11
    for file in old_guide:
        mappings[file + ".html"] = f"https://solr.apache.org/guide/8_11/{file}.html"

    for (key, value) in mappings.items():
        print(key, value)
    return mappings

def extract_filename_from_path(html_file_path):
    """Extract filename from path."""
    match = re.search(r'/([^\/]+)$', html_file_path)
    return match.group(1) if match else None

def process_html_file(html_file_path, url, mappings):
    """Process an HTML file to localize external JS and CSS references."""
    with open(html_file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    file_name = extract_filename_from_path(html_file_path)
    if file_name and file_name in mappings:
        canonical_url = mappings[file_name]
    else:
        canonical_url = url
    if canonical_url == url:
        print(f"Skipped {html_file_path}, filename {file_name}, it is the canonical url: {url}")
        return
    canonical_link_html = f"<link rel=\"canonical\" href=\"{canonical_url}\">\n"

    new_lines = []
    found_title = False
    for line in lines:
        soup = BeautifulSoup(line, "html.parser")
        title = soup.find("title")
        canon_link = soup.find("link", attrs={'rel': 'canonical'})

        if title and not found_title:
            new_lines.append(line)
            new_lines.append(canonical_link_html)
            new_lines.append(robots_no_index_html)
            found_title = True
        elif not (found_title and canon_link):
            # Skip any other canonical url we find
            new_lines.append(line)

    if found_title:
        with open(html_file_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print(f"Updated {html_file_path} to canonical url: {canonical_url}")

def main():
    parser = argparse.ArgumentParser(description='Process HTML files to add Canonical URLs to old ref guide pages')
    parser.add_argument('--old', required=True, help='Old pagenames file, one .adoc filename per line')
    parser.add_argument('--new', required=True, help='New pagenames file, one .adoc filename per line')
    parser.add_argument('--mapping', required=True, help='Semicolon separated from-to file names (adoc)')
    parser.add_argument('--folder', help='Folder of svn checkout (https://svn.apache.org/repos/infra/sites/solr/guide/)')
    args = parser.parse_args()
    mappings = generate_canonical_mapping(args)

    base_dir = args.folder

    # Iterate over the folder structure
    folders = [name for name in os.listdir(base_dir) if re.match(r'\d+_\d+', name)]
    if not folders:
        print(f"No versioned directories 'N_M' found in {base_dir}, exiting.")
        return
    for root_dir in folders:
        print(f"\nProcessing directory {root_dir}")
        print(f"=================================")
        full_path = os.path.join(base_dir, root_dir)
        if not os.path.exists(full_path):
            print(f"Directory {full_path} not found, skipping.")
            continue

        # Process each HTML file in the directory
        for filename in os.listdir(full_path):
            if filename.endswith(".html"):
                html_file_path = os.path.join(full_path, filename)
                url = f"https://solr.apache.org/guide/{root_dir}/{filename}"
                process_html_file(html_file_path, url, mappings)

if __name__ == "__main__":
    main()
