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
- Run with: `./gradlew :solr:webapp:test`

## Phase 1 — Smoke/navigation (`AdminUiSmokeTest`)

Navigate every route, wait for a screen-specific anchor element, assert no
severe browser console errors.

- [x] Node-level routes: `/`, `~logging`, `~logging/level`, `~cloud?view=nodes`,
      `~cloud?view=tree`, `~cloud?view=zkstatus`, `~cloud?view=graph`, `~cores`,
      `~collections`, `~schema-designer`, `~security`, `~java-properties`,
      `~threads`, `login`
- [x] Per-collection routes (fixture collection): `collection-overview`,
      `analysis`, `documents`, `files`, `query`, `stream`, `paramsets`,
      `schema`; per-core routes: `core-overview`, `plugins`, `segments`

Flaky-risk flags: `~cloud?view=graph` (d3 svg async), `~cloud?view=zkstatus`
(ZK admin-command availability in the embedded ensemble), `~schema-designer`
(many chained requests), `sqlquery` (needs sql module — excluded).

## Phase 2 — Node-level screens, display depth

- [x] Dashboard (`AdminUiDashboardTest`): versions, JVM info, memory bars,
      security warning vs `/admin/info/system`
- [x] Java Properties: `java.version` value matches `/admin/info/properties`
      (`AdminUiNodeScreensTest`)
- [x] Thread Dump: thread list non-empty, Jetty worker thread shown
- [x] Logging: logger tree renders with `org.apache.solr` row
- [x] Cloud > Nodes: one row per live node, ports match the cluster
- [x] Cloud > Tree: `live_nodes` and `collections` znodes shown
- [x] Cloud > ZK Status / Graph: render without console errors (smoke only)
- [x] Collections: collection listed; detail shows shard info
- [x] Core Admin: hosted core name shown, matching `/admin/cores`
- [x] Security: "security is not enabled" warning panel (no auth configured)
- [x] Login: authentication info page shown when no authenticationPlugin
- [ ] Deeper assertions: cloud graph replica leaves, ZK status ensemble
      details, logging events viewer content

## Phase 3 — Per-collection screens (fixture: collection with pre-indexed docs)

Covered by `AdminUiCollectionScreensTest`:

- [x] Collection Overview: shard info shown
- [x] Query: `*:*` finds all fixture docs, `id:` query finds exactly one
- [x] Analysis: `text_general` tokenizes and lowercases entered text
- [x] Documents (display): doc-type dropdown offers JSON/XML/CSV, submit present
- [x] Schema Browser: editable-schema action buttons, `_version_` field listed
- [x] Files: tree lists `solrconfig.xml`, file content renders
- [x] Plugins/Stats: searcher stats present (needs `metricsEnabled=true`)
- [x] Segments: at least one segment rendered after commit
- [x] Paramsets (display): form renders
- [ ] Query: paramsets dropdown, dismax/edismax toggles, raw query params
- [ ] Schema Browser: per-field flags vs `/schema` API, term info loading
- [ ] Stream: simple streaming expression executes and renders result
- [ ] Replication in cloud mode; standalone-mode coverage deferred

## Phase 4 — Write actions through the UI

Covered by `AdminUiWriteActionsTest`:

- [x] Collections: create collection via dialog → verify via API → delete via
      UI with typed confirmation → gone
- [x] Documents: submit JSON doc via form → success response → searchable
- [x] Logging: set logger to WARN via level editor → verify via API → revert
      to unset
- [ ] Collections: create/delete alias, add/delete replica, reload
- [ ] Schema Browser: add field → verify via `/schema/fields` → delete field
- [ ] Core Admin: RELOAD core via UI (rename/swap/unload deferred to a
      standalone-mode class)
- [ ] Paramsets: create paramset via UI → verify via `/config/params`
- [ ] Security with BasicAuth (`@Nightly`): bootstrap `security.json`, login via
      form, add user/role/permission, verify via security APIs (high flake risk)
- [ ] Schema Designer happy path (`@Nightly`, high flake risk)

Policy: phases 1–3 run in the default test run; heavyweight phase-4 classes
(security, schema designer) are `@Nightly`.

## Known limitations

- The generated js-client bundle (`libs/solr/index.js`) only exists inside the
  built WAR, not in the source tree tests serve from. `AdminUiTestBase` serves
  a minimal stub defining the `solrApi` global (only `reloadCollection` is used
  by the AngularJS UI) so the Collections screen works; a future improvement
  could serve the real bundle when it has been built.
- The shared menu code intermittently logs a benign
  `TypeError: Cannot read properties of null (reading 'name')` while the
  per-collection menu resolves; allowed in the paramsets test.
- The core overview ping widget answers 503 when the configset has no
  healthcheck file; allowed in the smoke test.
- ASF Jenkins has no Chrome, so these tests skip there; they run on developer
  machines and could run in a GitHub Actions workflow (Chrome preinstalled on
  `ubuntu-latest`) as a follow-up.
