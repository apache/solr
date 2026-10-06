<div class="javadoc-search">
  <input type="text" id="page-search-input" disabled aria-label="Search the Solr API"
         placeholder="Search the Solr API" autocomplete="off">
  <input type="reset" id="page-search-reset" disabled value="Reset">
  <p>
    <input type="checkbox" id="search-redirect" disabled>
    <label for="search-redirect">Redirect to first result</label>
  </p>
  <p id="page-search-notify">Loading search index...</p>
  <div id="result-section" style="display: none;">
    <div id="result-container"></div>
  </div>
</div>

<script>var pathtoroot = "./";</script>
<script src="script.js"></script>
<script src="script-dir/jquery-3.7.1.min.js"></script>
<script src="script-dir/jquery-ui.min.js"></script>
<script>loadScripts(document, 'script');</script>
<script src="search-page.js"></script>

Searches every module below at once. Each module also publishes its own
Javadocs, where the same search box covers the whole API.

## Modules

${projectList}
