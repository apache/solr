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
- On failure, a screenshot, the page source and the browser console log are
  saved into the test temp dir.
- Run with: `./gradlew :solr:webapp:test` (add `-Ptests.nightly=true` for the
  security and schema-designer classes)

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
- [ ] Paramsets dropdown, dismax/edismax toggles, raw query parameters

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

### Core Admin screen — `AdminUiCoreAdminScreenTest`
- [x] Hosted core listed, matching `/admin/cores`
- [x] Reload core via the button (success indicator)
- [ ] Add/rename/swap/unload core — cloud-mode core admin operations conflict
      with the Overseer; needs a standalone-mode harness

### Per-collection display screens — `AdminUiCollectionScreensTest`
- [x] Analysis: `text_general` tokenizes and lowercases entered text
- [x] Files: tree lists `solrconfig.xml`, file content renders
- [x] Segments: at least one segment rendered after commit
- [x] Plugins/Stats: searcher stats present (needs `metricsEnabled=true`)
- [x] Collection overview: shard info; Core overview: numDocs

### Stream screen — `AdminUiStreamScreenTest`
- [x] A `search(...)` streaming expression executes and renders all docs

### Replication screen — `AdminUiReplicationScreenTest`
- [x] Renders index version info in cloud mode
- [ ] Standalone leader/follower actions (replicate now, disable polling) —
      needs a standalone-mode harness

### Security with BasicAuth — `AdminUiSecurityAuthTest` (`@Nightly`)
- [x] Unauthenticated visit redirects to login; login form authenticates
- [x] Security screen shows authn/authz plugins, users, roles, permissions
- [x] Add a user through the dialog (verified via `/admin/authentication`)
- [ ] Add role / add permission dialogs

### Schema Designer — `AdminUiSchemaDesignerTest` (`@Nightly`)
- [x] Create a new schema, paste a sample doc, analyze; derived field shown

## Deliberately skipped (effort vs value)

- **SQL screen**: needs the `sql` module (Calcite and friends) on the webapp
  test classpath, dragging in many jars and license files for one screen.
- **JWT/OAuth login flows**: require an external identity provider or heavy
  mocking; BasicAuth covers the UI's login/session mechanics.
- **Keystroke-level entry in the security dialogs**: native clicks/keystrokes
  into the absolutely-positioned dialogs proved unreliable in headless Chrome;
  the dialogs are driven via the Angular controller scope instead. Keyboard
  entry is covered by the login form and the other screens' forms.
- **Standalone (non-cloud) mode screens**: replication actions and core admin
  rename/swap/unload need a standalone harness (`JettySolrRunner` without ZK);
  the cloud harness covers everything else.

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
