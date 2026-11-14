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

CATS — Cross-platform Automated Testing System — is the harness for the start scripts: one Java test runs `bin/solr` under bash and `bin\solr.cmd` under cmd.exe, which the BATS suites cannot do. The harness itself is generic and documented in [`solr/test-framework-cats/README.md`](../test-framework-cats/README.md) — sessions, `run`/`runFail` and the rest of the DSL, the portable file and HTTP replacements for Unix tools, transcripts and how a failure reads, `@EnabledOnShell`, port blocks, the `cats.*` settings. What follows is only the Solr layer. Suites live in `solr/packaging/src/test/org/apache/solr/shell/`.

`SolrCatsTestCase extends SolrTestCase implements CatsDsl`, so a suite gets Solr's test conventions and the generic vocabulary in one base class — including the `classCli` and `cli` sessions and the shorthands that delegate to them:

```java
@BeforeClass
public static void startSolr() {
  classCli.run("solr start");
}

@Test
public void createsCollection() {
  run("solr create -c coll").assertContains("Created collection 'coll'");
}
```

What it adds beyond the DSL:

- `ports()`, narrowed to `SolrPorts`: `solr()`, `solr2()`, `solr3()`, `zk()` over the class's port block.
- `collectionExists`, `configExists`, `deleteAllCollections`, `shutdownAll`, `dumpSolrLog` — delegating to `SolrCliHelpers`, which is `CatsCliHelpers` plus those five.
- `forCli(commonsCliText, picocliText)`, the wording for the command-line implementation this run targets.

### The `cli` trait

`SolrProgram.traits()` reports one trait, `cli`: `picocli` or `commons-cli`, as `SOLR_PICOCLI` says, defaulting to picocli in step with `SolrCLI.main`. `@EnabledOnCli("picocli")` on a method or a class skips a test that holds for only one implementation, before any node starts; where the two differ in wording alone, prefer `forCli`. `CliGate` is the rule behind the annotation, and the counterpart of the generic `ShellGate`.

### SolrProgram

The `ProgramUnderTest` implementation — how the generic harness learns the Solr facts it cannot guess: the port layout (the node port `P`, two more for the multi-node suites, embedded ZooKeeper on `P+1000`, stride 4, bases 20 000–30 000), node discovery by command-line marker through `SolrProcessManager`, `solr stop -p <port>` for a graceful stop, and `solr-<port>.pid` / `.port` as the stale files to delete after a force-kill.

### SolrCatsHarnessEnv and SolrPorts

`SolrCatsHarnessEnv` assembles the environment every spawned shell is given: `SOLR_TIP` (the unpacked distribution — required, with no fallback), a per-class `SOLR_HOME` wiped at setup, `SOLR_PID_DIR` inside it (which is what scopes `solr stop --all` to this class and away from your own Solr), `SOLR_LOGS_DIR`, `SOLR_PORT`/`SOLR_PORT_LISTEN`/`SOLR2_PORT`/`SOLR3_PORT`/`ZK_PORT` from the claimed block, `SOLR_ULIMIT_CHECKS=false`, the two-channel process-scan marker (`SOLR_PROCESS_SCAN_MARKER` for the CLI's JVM, `SOLR_OPTS_INTERNAL` for the node's command line), and `TEST_OUTPUT_DIR`/`TEST_FAILURE_DIR`. Each value is read from the environment, then from the system property `solr.cli.<lowercase.dotted.name>` (for IDE runs), then a fallback.

`SOLR_HOME` is also what gets snapshotted into `$TEST_FAILURE_DIR/<Class>-<method>/` when a test fails, alongside the transcript.

`SolrPorts` is a `PortBlock` view that gives the block's four ports the names the BATS suites used.

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

`./gradlew helpTests` covers seeds, filtering and the rest. Two things specific to CATS: `./gradlew catsTests` from the root runs both tasks of that name — `solr/test-framework-cats`'s harness unit tests, which are part of `check`, and `solr/packaging`'s CLI suites, which are not because they need an unpacked distribution. To run a suite from an IDE, build one (`./gradlew -p solr/packaging dev`) and point the harness at it with `SOLR_TIP` or `-Dsolr.cli.solr.tip=`.

## See also

- [`solr/test-framework-cats/README.md`](../test-framework-cats/README.md) — the generic CATS harness the Solr layer sits on.
- [`solr/packaging/README.md`](../packaging/README.md) — the CLI suites and the distribution they run against.
- [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) — which kind of test to write.
- [`dev-docs/admin-ui-tests.md`](../../dev-docs/admin-ui-tests.md) — the Selenium browser tests.
