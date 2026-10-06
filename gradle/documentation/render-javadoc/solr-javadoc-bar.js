/*
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

/*
 * Appended to javadoc's own script.js, so every generated API page picks it up
 * without javadoc needing a page template. Puts a Solr bar above javadoc's
 * navigation linking back to the website and to this release's documentation
 * index, which javadoc otherwise gives no way to reach.
 *
 * Paths are worked out at runtime: javadoc writes `var pathtoroot` into every
 * page, pointing at the root of this one javadoc tree, and the build prepends
 * `solrDocsRootUp` here, which climbs from that root to the documentation root.
 */
(function() {
  if (typeof solrDocsRootUp === 'undefined') {
    return;
  }

  function buildBar(docsRoot) {
    var bar = document.createElement('header');
    bar.className = 'solr-docs-bar';

    var brand = document.createElement('a');
    brand.className = 'solr-docs-brand';
    brand.href = 'https://solr.apache.org/';
    var logo = document.createElement('img');
    logo.src = docsRoot + 'solr.svg';
    logo.alt = 'Apache Solr';
    brand.appendChild(logo);
    bar.appendChild(brand);

    var title = document.createElement('span');
    title.className = 'solr-docs-title';
    // The window title is "<page> (Solr <version> <artifact> API)"; the part in
    // parentheses is the only place the artifact name appears on every page.
    var match = /\(([^()]*)\)\s*$/.exec(document.title || '');
    title.textContent = match ? match[1] : 'API';
    bar.appendChild(title);

    var nav = document.createElement('nav');
    [['Home', 'https://solr.apache.org/'],
     ['Docs', docsRoot + 'index.html']].forEach(function(entry) {
      var link = document.createElement('a');
      link.textContent = entry[0];
      link.href = entry[1];
      nav.appendChild(link);
    });
    bar.appendChild(nav);

    return bar;
  }

  function insertBar() {
    if (document.querySelector('.solr-docs-bar')) {
      return;
    }
    var docsRoot = (typeof pathtoroot === 'string' ? pathtoroot : './') + solrDocsRootUp;
    var bar = buildBar(docsRoot);
    // javadoc lays the page out as a fixed-height flex column; joining it as the
    // first item keeps the scrolling content pane sized correctly. Older page
    // shapes without that wrapper just get the bar at the top of the body.
    var flexBox = document.querySelector('div.flex-box');
    var parent = flexBox || document.body;
    parent.insertBefore(bar, parent.firstChild);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', insertBar);
  } else {
    insertBar();
  }
})();
