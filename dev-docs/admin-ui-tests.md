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

- [ ] Node-level routes: `/`, `~logging`, `~logging/level`, `~cloud?view=nodes`,
      `~cloud?view=tree`, `~cloud?view=zkstatus`, `~cloud?view=graph`, `~cores`,
      `~collections`, `~schema-designer`, `~security`, `~java-properties`,
      `~threads`, `login`
- [ ] Per-collection routes (fixture collection): `collection-overview`,
      `analysis`, `documents`, `files`, `query`, `stream`, `paramsets`,
      `plugins`, `schema`, `segments`

Flaky-risk flags: `~cloud?view=graph` (d3 svg async), `~cloud?view=zkstatus`
(ZK admin-command availability in the embedded ensemble), `~schema-designer`
(many chained requests), `sqlquery` (needs sql module — excluded).

## Phase 2 — Node-level screens, display depth

- [x] Dashboard (`AdminUiDashboardTest`): versions, JVM info, memory bars,
      security warning vs `/admin/info/system`
- [ ] Java Properties: a few props from `/admin/info/properties` rendered
- [ ] Thread Dump: thread list non-empty, known thread name, expand stacktrace
- [ ] Logging: logger tree renders, `org.apache.solr` row with level
- [ ] Cloud > Nodes: both nodes listed, host:port match cluster
- [ ] Cloud > Tree: `/live_nodes` count matches, expand collection `state.json`
- [ ] Cloud > ZK Status: ensemble status shown (lenient assertions)
- [ ] Cloud > Graph: collection node and replica leaves in SVG (lenient)
- [ ] Collections: created collection listed; detail shows shards/replicas Active
- [ ] Core Admin: core selector lists the core, overview matches `/admin/cores`
- [ ] Security: "security is not enabled" warning panel (no auth configured)
- [ ] Login: not-authenticated info page when no authenticationPlugin

## Phase 3 — Per-collection screens (fixture: collection with pre-indexed docs)

- [ ] Collection Overview: numDocs/maxDoc match API, healthy replica badge
- [ ] Query: run `*:*` via form, response block shows expected `numFound`;
      change `rows` and re-run
- [ ] Analysis: analyze a value for `text_general`, token table lowercases
- [ ] Documents (display): form renders, doc-type dropdown options present
- [ ] Schema Browser: field list contains `id`, flags match `/schema` API,
      term info loads for a populated field
- [ ] Files: tree lists `solrconfig.xml`, content loads
- [ ] Plugins/Stats: categories listed, searcher stats show numDocs
- [ ] Segments: segment bars present after commit
- [ ] Paramsets (display): empty-state or created paramset shown
- [ ] Stream: simple streaming expression executes and renders result (medium risk)
- [ ] Replication in cloud mode: verify what the screen shows; standalone-mode
      coverage deferred

## Phase 4 — Write actions through the UI

- [ ] Collections: create collection via dialog → verify via CLUSTERSTATUS →
      delete via UI → gone. Create/delete alias. Add replica.
- [ ] Documents: submit JSON doc → success response → found via UI Query and SolrJ
- [ ] Schema Browser: add field → verify in UI and `/schema/fields` → delete field
- [ ] Logging: set a logger to WARN via level editor → verify via API → revert
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
  built WAR, not in the source tree tests serve from; its 404 is whitelisted in
  the console-error assertion and v2-API-backed UI features relying on it are
  not exercised.
- ASF Jenkins has no Chrome, so these tests skip there; they run on developer
  machines and could run in a GitHub Actions workflow (Chrome preinstalled on
  `ubuntu-latest`) as a follow-up.
