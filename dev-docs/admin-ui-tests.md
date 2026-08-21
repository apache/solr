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
- The tests are opt-in: the suites carry the `@SeleniumTest` test group
  annotation (disabled by default), enabled with `-Ptests.selenium=true`.
- A locally installed Chrome/Chromium is required; since the tests only run
  when explicitly enabled, a missing browser fails the tests rather than
  skipping them. Override discovery with
  `-Dtests.selenium.chrome.binary=/path/to/chrome`. The matching chromedriver is
  provisioned (and cached) by Selenium Manager.
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
- Run with: `./gradlew :solr:webapp:test -Ptests.selenium=true`
- From an IDE, set `-Dtests.selenium=true` on the run configuration; the
  js-client bundle is picked up from the js-client build output, so run a
  Gradle build once first.

## Deliberately skipped (effort vs value)

- **JWT/OAuth login flows**: require an external identity provider or heavy
  mocking; BasicAuth covers the UI's login/session mechanics.
- **Keystroke-level entry in the security dialogs**: native clicks/keystrokes
  into the absolutely-positioned dialogs proved unreliable in headless Chrome;
  the dialogs are driven via the Angular controller scope instead. Keyboard
  entry is covered by the login form and the other screens' forms.

## Known limitations

- The generated js-client bundle (`libs/solr/index.js`) only exists inside the
  built WAR, not in the source tree tests serve from, so the build hands its
  location to the test JVM in `tests.ui.jsclient.bundle`. The bundle (and its
  node/npm toolchain) is only built when `-Ptests.selenium=true` enables the
  tests, keeping node off the default test build chain. With the js-client
  build turned off (`-PdisableJsClient=true`) the bundle cannot be built, so
  the build disables these tests with a warning — and fails with an error if
  `-Ptests.selenium=true` was passed as well.
- Every test cluster in the JVM registers a log-watcher appender under the same
  name in the shared log4j config, so a later cluster's watcher can be blind;
  the events-viewer test detects this via the API and skips itself.
- The shared menu code logs a benign
  `TypeError: Cannot read properties of null (reading 'name')` from
  `$scope.showCore` while the per-collection menu resolves (filtered in the
  console-error assertion; tracked in
  [SOLR-18347](https://issues.apache.org/jira/browse/SOLR-18347)).
- The core overview ping widget answers 503 when the configset has no
  healthcheck file (allowed in the affected tests; tracked in
  [SOLR-18347](https://issues.apache.org/jira/browse/SOLR-18347)).
- The Schema Designer's backend transiently fails its own prep/analyze calls
  with "version mismatch, retry" and recovers via its retry dialog; its API
  errors are excluded from the console-error assertion.
- ASF Jenkins jobs do not pass `-Ptests.selenium=true`, so these tests do
  not run there (a nightly job could opt in if its build nodes have a
  browser). In CI they run via the GitHub Actions workflow
  `.github/workflows/admin-ui-test.yml`, on pull requests that touch the
  webapp, the v2 API contract (`solr/api`) or the v2 API implementations
  (`solr/core/.../handler/admin/api`).
