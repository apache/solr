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
This script processes all static html files for Solr's refernce guide
and downloads external JS and CSS files to local folders js/ and css/ for
each version. It also updates the HTML files to reference the local files.
Context is that ASF policy for web sites changed to not allow external
references to JS and CSS files, and these sites were generated long ago.
"""

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import argparse

def extract_version_from_url(url):
    """Extract version number from URL if present."""
    match = re.search(r'/(\d+\.\d+(\.\d+)?)/', url)
    return match.group(1) if match else None

def is_external_url(url):
    if "apache.org" in url:
        return False
    """Check if a URL is external (starts with http/https or //)."""
    return url.startswith("http://") or url.startswith("https://") or url.startswith("//")

def download_file(url, dest_path):
    """Download a file from a URL to a local path."""
    if os.path.exists(dest_path):
        #print(f"Skipping {url} (already downloaded to {dest_path})")
        return
    try:
        if url.startswith("//"):
            url = "https:" + url  # Default to HTTPS for protocol-relative URLs
        if url.startswith("https://oss.maxcdn.com/"):
            url = url.replace("https://oss.maxcdn.com/", "https://cdnjs.cloudflare.com/ajax/")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {url} to {dest_path}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def add_version_to_filename(filename, version):
    """Add version number to filename if not already present.
       Example: jquery.js -> jquery-3.6.0.js
                jquery.min.js -> jquery-3.6.0.min.js
    """
    if filename.endswith(".min.js"):
        filename_parts = filename.rsplit(".min.js", 1)
        filename = f"{filename_parts[0]}-{version}.min.js"
    elif filename.endswith(".min.css"):
        filename_parts = filename.rsplit(".min.css", 1)
        filename = f"{filename_parts[0]}-{version}.min.css"
    else:
        filename_parts = filename.rsplit('.', 1)
        filename = f"{filename_parts[0]}-{version}.{filename_parts[1]}"
    return filename

def process_html_file(html_file_path, js_dir, css_dir, skip_files=None):
    """Process an HTML file to localize external JS and CSS references."""
    with open(html_file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    modified = False
    new_lines = []

    for line in lines:
        soup = BeautifulSoup(line, "html.parser")
        script = soup.find("script", src=True)
        link = soup.find("link", rel="stylesheet", href=True)

        if script and is_external_url(script["src"]):
            src = script["src"]
            filename = os.path.basename(urlparse(src).path)
            version = extract_version_from_url(src)
            if version and not re.search(r'\d+\.\d+(\.\d+)?', filename):
                filename = add_version_to_filename(filename, version)
            local_path = os.path.join(js_dir, filename)
            download_file(src, local_path)
            script["src"] = f"js/{filename}"  # Relative path to js/ folder
            new_lines.append(str(script) + "\n")
            modified = True
        elif link and is_external_url(link["href"]):
            href = link["href"]
            filename = os.path.basename(urlparse(href).path)
            if filename not in skip_files:
                version = extract_version_from_url(href)
                if version and not re.search(r'\d+\.\d+(\.\d+)?', filename):
                    filename = add_version_to_filename(filename, version)
                local_path = os.path.join(css_dir, filename)
                download_file(href, local_path)
            link["href"] = f"css/{filename}"  # Relative path to css/ folder
            new_lines.append(str(link) + "\n")
            modified = True
        else:
            new_lines.append(line)

    if modified:
        with open(html_file_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print(f"Updated {html_file_path}")

def main():
    parser = argparse.ArgumentParser(description='Process HTML files to localize external JS and CSS references.')
    parser.add_argument('folder', help='Folder of svn checkout (https://svn.apache.org/repos/infra/sites/solr/guide/)')
    args = parser.parse_args()

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

        js_dir = os.path.join(full_path, "js")
        css_dir = os.path.join(full_path, "css")
        os.makedirs(js_dir, exist_ok=True)
        os.makedirs(css_dir, exist_ok=True)

        skip_files = ["font-awesome.min.css"]

        # Process each HTML file in the directory
        for filename in os.listdir(full_path):
            if filename.endswith(".html"):
                html_file_path = os.path.join(full_path, filename)
                process_html_file(html_file_path, js_dir, css_dir, skip_files)

if __name__ == "__main__":
    main()