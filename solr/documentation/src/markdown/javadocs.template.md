<div class="javadoc-search">
  <input type="text" id="page-search-input" disabled aria-label="Search the Solr API"
         placeholder="Search the Solr API" autocomplete="off">
  <input type="reset" id="page-search-reset" disabled value="Reset">
  <p id="page-search-notify">Loading search index...</p>
  <div id="result-section" style="display: none;">
    <div id="result-container"></div>
  </div>
</div>

<script>var pathtoroot = "javadocs/api/";</script>
<script src="javadocs/api/script.js"></script>
<script src="javadocs/api/script-dir/jquery-3.7.1.min.js"></script>
<script src="javadocs/api/script-dir/jquery-ui.min.js"></script>
<script src="javadocs/api/search.js"></script>
<script>
  // javadoc builds result links relative to its own tree root, and this page
  // sits one level above it. getURL caches the unprefixed path on the item and
  // returns it on later calls, so prefixing the return value stays correct.
  var solrTreeGetURL = getURL;
  getURL = function(item, category) {
    return pathtoroot + solrTreeGetURL(item, category);
  };
</script>
<script src="javadocs/api/module-search-index.js"></script>
<script src="javadocs/api/package-search-index.js"></script>
<script src="javadocs/api/type-search-index.js"></script>
<script src="javadocs/api/member-search-index.js"></script>
<script src="javadocs/api/tag-search-index.js"></script>
<script src="javadocs/api/search-page.js"></script>

Searches every Solr module at once.

## Javadoc sets

* [Solr Javadocs](javadocs/api/index.html): every Solr module in one place
* [Solr Test Framework Javadocs](javadocs/test-framework/index.html): the helpers for writing tests against Solr
* [Lucene ${project.luceneDocVersion} Javadocs](${project.luceneDocUrl}/index.html): the Lucene libraries that Solr is built on
