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

# Solr Test Framework

Base classes and utilities for JUnit tests of Solr, published as `solr-test-framework` for plugin and client authors.

Built on `lucene-test-framework`: `SolrTestCase` extends `LuceneTestCase`, so tests inherit randomizedtesting's `RandomizedRunner` — seeded randomness, thread-leak detection, test groups, the `-Ptests.*` options. Solr is on **JUnit 4** (the build calls `useJUnit()`, not `useJUnitPlatform()`); there is no Jupiter support.

## Which base class

| Class | Use it when |
| --- | --- |
| `SolrTestCase` | Default. Anything needing no Solr core. |
| `cloud.SolrCloudTestCase` | You need a real cluster: in-JVM `MiniSolrCloudCluster` with embedded ZooKeeper. |
| `shell.SolrCatsTestCase` | You are testing `bin/solr` / `bin\solr.cmd` itself, as a real subprocess. |
| `BaseDistributedSearchTestCase` | Distributed-search comparisons against a control core. |
| `SolrTestCaseJ4` | Legacy. Don't extend it in new tests. |

`util.SolrClientTestRule` (with `EmbeddedSolrServerTestRule` / `SolrJettyTestRule`) hands a test a running Solr plus client as a rule instead of a superclass. It composes better than inheritance and is the recommended route for external projects.

## SolrTestCase

Deliberately thin — its javadoc asks you to keep it that way. It contributes the rules every Solr test needs: thread-leak filters (`SolrIgnoredThreadsFilter`), `SystemPropertiesRestoreRule`, `ObjectReleaseTracker` verification after a passing class, `@LogLevel` support, and the test-class naming check. `@ThreadLeakFilters` is not additive: a subclass adding a filter must restate the inherited ones. [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) has the full list and the lifecycle conventions.

## SolrCloudTestCase

`configureCluster(n).addConfig("conf", ExternalPaths.DEFAULT_CONFIGSET).configure()` in `@BeforeClass`; the cluster is the static `cluster` field and is shut down for you. Neighbours: `MultiSolrCloudTestCase`, `SolrCloudAuthTestCase`, `ChaosMonkey` and `SocketProxy` for fault injection.

## SolrCatsTestCase

CATS — Cross-platform Automated Testing System — is the harness for the start scripts: one Java test runs `bin/solr` under bash and `bin\solr.cmd` under cmd.exe, which the BATS suites cannot do. `SolrCatsTestCase` in `org.apache.solr.shell` is the base class a suite extends, the generic harness under it is `org.apache.solr.cats` in the same module, and the suites live in `solr/packaging/src/test/org/apache/solr/shell/`. [`dev-docs/cats.adoc`](../../dev-docs/cats.adoc) covers both layers — the DSL, the portable replacements for Unix tools, transcripts and how a failure reads, `@EnabledOnShell`, port blocks, the `cats.*` settings, and what the Solr layer adds.

## SolrTestCaseJ4

The old harness: `initCore`, `assertQ`, `assertU`, `req(..)` and a large pile of statics. Much of the existing suite still extends it, `SolrCloudTestCase` included. Don't extend it in new tests — use `SolrTestCase` and plain SolrJ types. Members are being removed as replacements land (`ignoreException` → `ErrorLogMuter`).

## Utilities

The ones you actually reach for:

- **Running Solr** — `embedded.JettySolrRunner` (+ `JettyConfig`), `cloud.MiniSolrCloudCluster`, `cloud.ZkTestServer`.
- **Randomization** — `util.RandomizeSSL` / `@SuppressSSL` and `util.SSLTestConfig`; `util.ExternalPaths` for paths outside the classpath.
- **Logging** — `@LogLevel`, `util.LogListener` to assert a message was or wasn't logged, `util.ErrorLogMuter` to silence expected ERRORs.
- **Assertions** — `JSONTestUtil`, `util.QueryMatchers` (Hamcrest matchers for parsed `Query` trees), `util.SolrMatchers`.
- **Inspection** — `TrackingShardHandlerFactory`, `TrackingUpdateProcessorFactory`, `RecordingUpdateProcessorFactory`, `TrackingBackupRepository`.
- **Narrower bases** — `AbstractBadConfigTestBase`, `util.RestTestBase`, `UpdateProcessorTestBase`; `util.SecurityJson` for `security.json` constants.

## Running tests

`./gradlew helpTests` covers seeds, filtering and the rest. Two things specific to CATS: the harness' own unit tests are part of `check` and run with `./gradlew -p solr/test-framework test --tests "org.apache.solr.cats.*"`, while `solr/packaging`'s CLI suites are not, because they need an unpacked distribution — `./gradlew -p solr/packaging catsTests`. To run a suite from an IDE, build a distribution (`./gradlew -p solr/packaging dev`) and point the harness at it with `SOLR_TIP` or `-Dsolr.cli.solr.tip=`.

## See also

- [`dev-docs/cats.adoc`](../../dev-docs/cats.adoc) — the CATS harness: the generic core and the Solr layer on top of it.
- [`solr/packaging/README.md`](../packaging/README.md) — the CLI suites and the distribution they run against.
- [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) — which kind of test to write.
- [`dev-docs/admin-ui-tests.md`](../../dev-docs/admin-ui-tests.md) — the Selenium browser tests.
