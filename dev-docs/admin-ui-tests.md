# Admin UI (AngularJS) Browser Tests

Browser-based tests of the old AngularJS Admin UI (`solr/webapp/web/`), driven by
Selenium WebDriver with headless Chrome. What each screen covers is documented in
the test classes themselves; this page covers how the suite works and what it
deliberately does not do.

## How the tests work

- Tests live in `solr/webapp/src/test/org/apache/solr/webapp/` and extend
  `AdminUiTestBase`, which starts a 2-node `MiniSolrCloudCluster` whose Jetty
  nodes also serve the Admin UI (opt-in `JettyConfig.Builder#enableAdminUi`),
  then starts a headless Chrome via Selenium WebDriver.
- A locally installed Chrome/Chromium is required; without one, tests skip via
  `Assume`. Override discovery with `-Dtests.ui.chrome.binary=/path/to/chrome`.
  The matching chromedriver is provisioned (and cached) by Selenium Manager.
- Display assertions compare UI text against live JSON from the same node's
  admin APIs — never hardcoded values.
- Tests are grouped per screen/feature, so each screen's display and write
  tests live in the same class.
- Most tests run against a 2-node cloud cluster; `AdminUiStandaloneTestBase`
  additionally supports standalone (user-managed, no ZooKeeper) nodes, whose
  UI differs (no Cloud/Collections/Schema Designer menus; the per-core menu
  offers query/replication etc. directly).
- On failure, a screenshot, the page source and the browser console log are
  saved into the test temp dir.
- Run with: `./gradlew :solr:webapp:test`

## Deliberately skipped (effort vs value)

- **JWT/OAuth login flows**: require an external identity provider or heavy
  mocking; BasicAuth covers the UI's login/session mechanics.
- **Keystroke-level entry in the security dialogs**: native clicks/keystrokes
  into the absolutely-positioned dialogs proved unreliable in headless Chrome;
  the dialogs are driven via the Angular controller scope instead. Keyboard
  entry is covered by the login form and the other screens' forms.

## Known limitations

- The generated js-client bundle (`libs/solr/index.js`) only exists inside the
  built WAR, not in the source tree tests serve from. `AdminUiTestBase` serves
  a minimal stub defining the `solrApi` global (only `reloadCollection` is used
  by the AngularJS UI); a future improvement could serve the real bundle when
  it has been built.
- Every test cluster in the JVM registers a log-watcher appender under the same
  name in the shared log4j config, so a later cluster's watcher can be blind;
  the events-viewer test detects this via the API and skips itself.
- The shared menu code logs a benign
  `TypeError: Cannot read properties of null (reading 'name')` from
  `$scope.showCore` while the per-collection menu resolves (filtered in the
  console-error assertion; candidate for a JIRA).
- The core overview ping widget answers 503 when the configset has no
  healthcheck file (allowed in the affected tests).
- The Schema Designer's backend transiently fails its own prep/analyze calls
  with "version mismatch, retry" and recovers via its retry dialog; its API
  errors are excluded from the console-error assertion.
- ASF Jenkins has no Chrome, so these tests skip there; they run on developer
  machines and could run in a GitHub Actions workflow (Chrome preinstalled on
  `ubuntu-latest`) as a follow-up.
