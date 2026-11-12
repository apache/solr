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
| `shell.SolrShellTestCase` | You are testing `bin/solr` / `bin\solr.cmd` itself, as a real subprocess. |
| `BaseDistributedSearchTestCase` | Distributed-search comparisons against a control core. |
| `SolrTestCaseJ4` | Legacy. Don't extend it in new tests. |

`util.SolrClientTestRule` (with `EmbeddedSolrServerTestRule` / `SolrJettyTestRule`) hands a test a running Solr plus client as a rule instead of a superclass. It composes better than inheritance and is the recommended route for external projects.

## SolrTestCase

Deliberately thin — its javadoc asks you to keep it that way. It contributes the rules every Solr test needs: thread-leak filters (`SolrIgnoredThreadsFilter`), `SystemPropertiesRestoreRule`, `ObjectReleaseTracker` verification after a passing class, `@LogLevel` support, and the test-class naming check. `@ThreadLeakFilters` is not additive: a subclass adding a filter must restate the inherited ones. [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) has the full list and the lifecycle conventions.

## SolrCloudTestCase

`configureCluster(n).addConfig("conf", ExternalPaths.DEFAULT_CONFIGSET).configure()` in `@BeforeClass`; the cluster is the static `cluster` field and is shut down for you. Neighbours: `MultiSolrCloudTestCase`, `SolrCloudAuthTestCase`, `ChaosMonkey` and `SocketProxy` for fault injection.

## SolrShellTestCase

CATS — Cross-platform Automated Testing System — the harness for the start scripts: one Java test runs `bin/solr` under bash and `bin\solr.cmd` under cmd.exe, which the BATS suites cannot do. Suites live in `solr/packaging/src/test/org/apache/solr/shell/`; the harness' own unit tests are in `src/test/org/apache/solr/shell/`.

No long-lived shell process — each command starts a fresh one with the session's environment and cwd, so quoting and expansion are the real shell's while control flow stays in Java.

### Sessions

| Field | Scope | BATS equivalent |
| --- | --- | --- |
| `classCli` (static) | the class; nodes it starts are stopped after it | `setup_file` |
| `cli` | one test; env, cwd and nodes rolled back after it | `setup` |

Test methods use the inherited shorthands (`run`, `http`, `env`, `read`, …), which delegate to `cli`. Only `@BeforeClass`/`@AfterClass` name `classCli`.

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

### Commands and assertions

`run(String)` is one command with merged streams, as BATS' `run`, and **asserts that it succeeded** — so the happy path carries no status boilerplate. A command expected to fail uses `runFail(String)`, which asserts a non-zero status and hands back the result to assert on what it printed. Both fail at the call site, naming the command and its output, and both fail if the status is the opposite of the one implied. For an exact code there is `assertExitCode(int)`. A predicate that inspects the status itself - a poll that expects failure until some state changes - uses `tryRun(String)`, which asserts nothing.

Also `run(Duration, String)`, `run(List<String>)` (harness quotes each argument for the shell in use), `runFail(List<String>)`, `tryRun(String)`, `runSeparated(String)`, `script(bash, cmd)` for shell-specific logic, and `lastResult()`. Timeouts default to 120 s, 300 s for `solr start`/`restart`.

`CommandResult`'s assertions are fluent and chainable, named after their `bats-assert` originals: `assertExitCode`, `assertContains`, `assertNotContains`, `assertMatches`, `assertNotMatches`, `assertExact`, `assertLine`, `assertLineCount`, `assertContainsTimes`, and `assertContainsForCli(commonsCliText, picocliText)` for wording that differs between the two CLI implementations. Accessors replace the pipelines a BATS line would use: `lines`, `linesMatching`, `firstMatch`, `firstLines`, `lastLines`, `fields`, `countOccurrences`.

### Variables

Three mechanisms, and no others:

1. `${NAME}` (also `$NAME`, `{{NAME}}`) — resolved by the *harness*, in command lines and in expected text alike, so a BATS line ports unchanged. An unknown name fails the test rather than becoming empty. `%NAME%` is rejected. Supplied: `SOLR_TIP`, `SOLR_HOME`, `SOLR_PID_DIR`, `SOLR_LOGS_DIR`, `TEST_OUTPUT_DIR`, `TEST_FAILURE_DIR`, `WORK_DIR`, `SOLR_PORT`, `SOLR2_PORT`, `SOLR3_PORT`, `ZK_PORT`, plus `env(name, value)` / `unset(name)`.
2. Java concatenation for Java values: `run("solr start -p " + myPort)`.
3. `run(List.of(..))` when an argument needs per-shell quoting.

Inside `script(bash, cmd)` only `{{NAME}}` is harness-resolved; `$NAME` belongs to the shell, and a `$NAME` in the *cmd* body is rejected because cmd.exe would pass it through as text.

### Portability rails

A `run()` line that could only pass on bash fails immediately, with the replacement named: Unix-only tools (`grep`, `sed`, `awk`, `cat`, `ls`, `curl`, `sleep` and ~40 more), bash-only syntax outside quotes (`|`, `&&`, `;`, `$(..)`, backticks, single-quoted arguments, leading `NAME=value`, `/dev/null`), more than one command, and `%NAME%`.

The replacements are built in. File work, resolved against the session's cwd: `path`, `mkdirs`, `write`, `append`, `read`, `readLinesMatching`, `fileContains`, `exists`, `rm`, `copy`, `touch`, `list`. HTTP: `http(path)` and the `https(path)` builder, instead of `curl`. Waiting: `awaitOrFail(what, timeout, interval, condition)`, instead of `sleep`.

`@EnabledOnShell` switches the rails off for that test — it has declared one shell, so there is nothing left to break.

### Skipping

`@EnabledOnShell(Shell.BASH|CMD)` and `@EnabledOnCli(Cli.PICOCLI|COMMONS_CLI)` work on a method or a class, and skip as assumption failures before any node starts. No annotation means the test must pass everywhere, which is what a ported BATS test normally should be. Where only wording differs, prefer `assertContainsForCli`. A test that drives the harness without extending `SolrShellTestCase` honours `@EnabledOnShell` via `@Rule public final ShellGate shellGate = new ShellGate();`.

### Isolation

Every test class gets its own `SOLR_HOME` (wiped at setup), `SOLR_PID_DIR` (inside it, which is what scopes `solr stop --all` to this class and away from your own Solr), logs directory, and port block claimed under a file lock. The block layout is dictated by Solr: a node on `P` also listens on `P-1000` for `solr stop` and runs ZooKeeper on `P+1000`. A pre-flight check fails fast if the block is busy, reaping nodes a killed earlier run left behind; a post-flight check fails the class if a node outlived it. That is what makes parallel forks safe.

Each value is read from the environment, then the system property `solr.cli.<lowercase.dotted.name>` (for IDE runs), then a fallback. `SOLR_TIP` has none.

### How a failure reads

`ShellAssertionError` carries no stack trace — the frames would all be harness internals. You get the transcript and one `FAILED:` line:

```
$ solr zk ls / -z localhost:47894
exit 0 in 1.2s
| aliases.json

FAILED: output did not contain: collections
```

A malformed *test* — unknown `${NAME}`, a non-portable line — still throws a normal `AssertionError` with frames, because there the test line is what matters. On failure `SOLR_HOME` and the transcript are copied to `$TEST_FAILURE_DIR/<Class>-<method>/` and the path printed; that is BATS' `save_home_on_failure`.

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

`./gradlew helpTests` covers seeds, filtering and the rest. Two things specific to CATS: `./gradlew catsTests` from the root runs both tasks of that name — `solr/test-framework`'s harness unit tests, which are part of `check`, and `solr/packaging`'s CLI suites, which are not because they need an unpacked distribution. To run a suite from an IDE, build one (`./gradlew -p solr/packaging dev`) and point the harness at it with `SOLR_TIP` or `-Dsolr.cli.solr.tip=`.

## See also

- [`solr/packaging/README.md`](../packaging/README.md) — the CLI suites and the distribution they run against.
- [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) — which kind of test to write.
- [`dev-docs/admin-ui-tests.md`](../../dev-docs/admin-ui-tests.md) — the Selenium browser tests.
