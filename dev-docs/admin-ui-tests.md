<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Admin UI (AngularJS) Browser Test Plan

This document tracks browser-based test coverage of the old AngularJS Admin UI
(`solr/webapp/web/`), driven by Selenium WebDriver with headless Chrome.

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

## Coverage by screen

### Smoke navigation — `AdminUiSmokeTest`
- [x] Every node-level route (`/`, `~logging`, `~logging/level`,
      `~cloud?view=nodes|tree|zkstatus|graph`, `~cores`, `~collections`,
      `~schema-designer`, `~security`, `~java-properties`, `~threads`, `login`),
      every per-collection route and the per-core routes render their main
      content element without severe browser console errors.

### Dashboard — `AdminUiDashboardTest`
- [x] Versions, JVM info and memory bars vs `/admin/info/system`; security
      warning when security is disabled.

### Node-level screens — `AdminUiNodeScreensTest`
- [x] Java Properties: `java.version` value matches `/admin/info/properties`
- [x] Thread Dump: thread list non-empty, Jetty worker thread shown
- [x] Cloud > Nodes: one row per live node, ports match the cluster
- [x] Cloud > Tree: `live_nodes` and `collections` znodes shown
- [x] Cloud > Graph: d3 SVG renders circles for collection/shard/replicas
- [x] Cloud > ZK Status: status green, ensemble size shown
- [x] Security screen: "not enabled" warning without auth
- [x] Login screen: authentication info page without auth

### Collections screen — `AdminUiCollectionsScreenTest`
- [x] Collection detail display (shards)
- [x] Create + delete collection via the dialogs (verified via API)
- [x] Create + delete alias via the dialogs (verified via LISTALIASES)
- [x] Add + delete replica via the shard detail (verified via cluster state)
- [x] Reload collection (verified via core start time reset)

### Query screen — `AdminUiQueryScreenTest`
- [x] `*:*` and `id:` queries via the form, `numFound` in the response
- [x] `rows` and `fl` parameters affect the returned documents
- [x] Paramsets dropdown applies a paramset created via the API
- [x] defType dismax/edismax toggles reveal their parameter fields; edismax
      query with `qf` returns the expected results
- [x] Raw query parameters (e.g. an extra `fq`) are applied

### Documents screen — `AdminUiDocumentsScreenTest`
- [x] Form renders with JSON/XML/CSV document types
- [x] Index a JSON document via the form; becomes searchable

### Schema screen — `AdminUiSchemaScreenTest`
- [x] Field list browsable, editable-schema action buttons shown
- [x] Field detail shows flags matching `/schema/fields`; term info loads
- [x] Add + delete a field via the dialogs (verified via `/schema/fields`)

### Paramsets screen — `AdminUiParamsetsScreenTest`
- [x] Form renders
- [x] Create + delete a paramset via the form (verified via `/config/params`)

### Logging screens — `AdminUiLoggingScreenTest`
- [x] Logger level tree renders
- [x] Set + unset a logger level via the tree (verified via API)
- [x] Events viewer shows a WARN event logged in the server JVM (skips itself
      when the node's log watcher is blind due to shared-JVM log4j state, see
      Known limitations)

### Core Admin screen — `AdminUiCoreAdminScreenTest` (cloud) and
### `AdminUiCoreAdminStandaloneTest` (standalone)
- [x] Hosted core listed, matching `/admin/cores`
- [x] Reload core via the button (success indicator)
- [x] Standalone-mode menu differences (no cloud menus; core menu offers
      query/replication)
- [x] Add core via the dialog (pre-created instance dir)
- [x] Rename core via the dialog
- [x] Swap cores via the dialog (verified by the indexes exchanging)
- [x] Unload core, accepting the native confirm dialog

### Per-collection display screens — `AdminUiCollectionScreensTest`
- [x] Analysis: `text_general` tokenizes and lowercases entered text
- [x] Files: tree lists `solrconfig.xml`, file content renders
- [x] Segments: at least one segment rendered after commit
- [x] Plugins/Stats: searcher stats present (needs `metricsEnabled=true`)
- [x] Collection overview: shard info; Core overview: numDocs

### Stream screen — `AdminUiStreamScreenTest`
- [x] A `search(...)` streaming expression executes and renders all docs

### SQL screen — `AdminUiSqlScreenTest`
- [x] A SQL query executes through the form and the result grid lists the
      documents (the sql module is a test-only dependency of `solr:webapp`)

### Replication screen — `AdminUiReplicationScreenTest` (cloud) and
### `AdminUiReplicationStandaloneTest` (standalone leader/follower)
- [x] Renders index version info in cloud mode
- [x] Follower screen shows the leader's info
- [x] Disable polling, index on the leader, replicate-now transfers the
      index, re-enable polling — verified via the replication API

### Security with BasicAuth — `AdminUiSecurityAuthTest`
- [x] Unauthenticated visit redirects to login; login form authenticates
- [x] Security screen shows authn/authz plugins, users, roles, permissions
- [x] Add a user through the dialog (verified via `/admin/authentication`)
- [x] Add a role for the user through the dialog (verified via API)
- [x] Grant a predefined permission to the role (verified via API)

### Schema Designer — `AdminUiSchemaDesignerTest` (`@AwaitsFix`)
- [x] Create a new schema, paste a sample doc, analyze; derived field shown —
      but the designer backend is too flaky under automation (see Possible UI
      bugs), so the test awaits a fix before running by default

## Deliberately skipped (effort vs value)

- **JWT/OAuth login flows**: require an external identity provider or heavy
  mocking; BasicAuth covers the UI's login/session mechanics.
- **Keystroke-level entry in the security dialogs**: native clicks/keystrokes
  into the absolutely-positioned dialogs proved unreliable in headless Chrome;
  the dialogs are driven via the Angular controller scope instead. Keyboard
  entry is covered by the login form and the other screens' forms.

## Possible UI bugs to investigate

Issues surfaced by these tests that look like real bugs, weaknesses or
flakiness in the Admin UI (or its backing APIs) rather than bad test code.
Tests work around them as noted; each deserves investigation and possibly a
JIRA:

1. **Menu TypeError on per-collection pages**: navigating to any
   per-collection screen intermittently logs
   `TypeError: Cannot read properties of null (reading 'name')` from
   `$scope.showCore` in `js/angular/app.js` — the core selector fires its
   change handler with a null core while the menu resolves. Workaround: the
   console-error assertion filters this signature.
2. **Collections screen dies without the js-client bundle**: the
   `CollectionsV2` service factory (`services.js`) references the `solrApi`
   global at injection time; if `libs/solr/index.js` fails to load, the whole
   `CollectionsController` fails and the screen is blank. Only
   `reloadCollection` is used from that bundle — a lazy/optional lookup would
   degrade gracefully. Workaround: tests serve a stub bundle.
3. **Security screen dialogs unreliable under automation**: native clicks on
   the Add User toggle and keystrokes into the absolutely-positioned dialog
   (jQuery-positioned, `escape-pressed` directive) are dropped in headless
   Chrome even though the same interactions work on other screens. May
   indicate a focus/z-index issue. Workaround: the test drives the dialog via
   the Angular controller scope.
4. **Schema Designer races itself**: creating a schema and analyzing sample
   docs transiently fails with `Failed to persist managed schema ... version
   mismatch, retry` from its own `prep`/`analyze` calls, surfacing an error
   dialog the user has to dismiss. Workaround: the test retries via the
   offered Reload Schema button and ignores the designer's own 5xx console
   errors.
5. **Plugins screen 500s when metrics are disabled**: `/admin/metrics` with
   `wt=prometheus` returns HTTP 500 ("No metrics found in response") when
   metrics collection is disabled, instead of a clean error; the Plugins
   screen just shows nothing while the console logs the 500. Workaround:
   tests enable `metricsEnabled`.
6. **Core overview ping widget logs a 503**: with a configset that has no
   healthcheck file, the ping status call answers 503 and the console shows a
   resource-load error on every visit; the widget could handle "healthcheck
   not configured" gracefully. Workaround: allowed in the affected tests.
7. **Reload success indicator is a 1-second flash**: the Collections screen's
   reload button only flags success via a CSS class for one second, which is
   easy to miss (and impossible to assert on reliably). Workaround: the test
   verifies the reload via the core start time instead.
8. **ui-grid icon font is missing from the webapp**: `css/angular/ui-grid.min.css`
   references `fonts/ui-grid.woff` (and .ttf/.eot), but no such font files are
   shipped anywhere under `solr/webapp/web` — the SQL screen's result grid
   logs a 404 for it in production too, and grid icons render as boxes.
   Workaround: the console-error assertion filters this 404.

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
