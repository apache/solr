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

/** Documentation root, relative to the current page. Set by loadScripts below. */
var solrDocsRoot = null;

/*
 * Replaces the loadScripts() declared earlier in javadoc's script.js -- a later
 * declaration of the same name wins -- and is the first thing every page runs
 * after setting `pathtoroot`, which is the one hook javadoc leaves us.
 *
 * When the build has published a merged, site-wide search index, repointing
 * `pathtoroot` at the documentation root is all it takes to make each page's
 * own search box search every module: javadoc uses that one variable both to
 * load the index files below and to resolve the result links it navigates to
 * (search.js), so index and links stay consistent with each other.
 */
function loadScripts(doc, tag) {
  solrDocsRoot = (typeof pathtoroot === 'string' ? pathtoroot : './') + solrDocsRootUp;
  if (typeof solrDocsGlobalSearch !== 'undefined' && solrDocsGlobalSearch) {
    pathtoroot = solrDocsRoot;
  }
  // Same set javadoc's own loadScripts() loads.
  createElem(doc, tag, 'search.js');
  createElem(doc, tag, 'module-search-index.js');
  createElem(doc, tag, 'package-search-index.js');
  createElem(doc, tag, 'type-search-index.js');
  createElem(doc, tag, 'member-search-index.js');
  createElem(doc, tag, 'tag-search-index.js');
}

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
    title.textContent = match ? match[1] : (document.title || '').trim();
    bar.appendChild(title);

    bar.appendChild(buildModulePicker(docsRoot));

    var nav = document.createElement('nav');
    [['Home', 'https://solr.apache.org/'],
     ['Docs', docsRoot + 'index.html'],
     ['Javadocs index', docsRoot + 'javadocs.html']].forEach(function(entry) {
      var link = document.createElement('a');
      link.textContent = entry[0];
      link.href = entry[1];
      nav.appendChild(link);
    });
    bar.appendChild(nav);

    return bar;
  }

  /*
   * Javadoc renders each module as its own self-contained tree with no way to
   * reach the others, so offer them as a jump list.
   */
  function buildModulePicker(docsRoot) {
    var picker = document.createElement('select');
    picker.className = 'solr-docs-modules';
    picker.setAttribute('aria-label', 'Jump to a module');
    // Pages outside any one module -- the Javadocs index -- start on a
    // placeholder rather than preselecting an arbitrary module. Choosing it
    // from inside a module goes back up to the index.
    var placeholder = document.createElement('option');
    placeholder.value = docsRoot + 'javadocs.html';
    placeholder.textContent = '\u2014';
    placeholder.selected = !solrDocsModule;
    picker.appendChild(placeholder);
    (typeof solrDocsModules === 'undefined' ? [] : solrDocsModules).forEach(function(path) {
      var option = document.createElement('option');
      option.value = docsRoot + path + '/index.html';
      option.textContent = path.replace(/\//g, '-');
      option.selected = (path === solrDocsModule);
      picker.appendChild(option);
    });
    picker.addEventListener('change', function() {
      if (picker.value) {
        window.location.href = picker.value;
      }
    });
    return picker;
  }

  /*
   * The SEARCH link beside javadoc's own search box points at this one module's
   * search page. The box now searches every module, so the link belongs on the
   * site-wide Javadocs page.
   */
  function retargetSearchLink(docsRoot) {
    var link = document.querySelector('.nav-list-search a[href$="search.html"]');
    if (link) {
      link.href = docsRoot + 'javadocs.html';
    }
  }

  function insertBar() {
    if (document.querySelector('.solr-docs-bar')) {
      return;
    }
    // loadScripts() may have repointed pathtoroot, so use what it recorded.
    var docsRoot = solrDocsRoot
        || ((typeof pathtoroot === 'string' ? pathtoroot : './') + solrDocsRootUp);

    retargetSearchLink(docsRoot);

    // A documentation site page renders the real masthead server-side, so it
    // needs only the module picker, not a second header.
    var siteNav = document.querySelector('.masthead .masthead-nav');
    if (siteNav) {
      siteNav.insertBefore(buildModulePicker(docsRoot), siteNav.firstChild);
      return;
    }

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
