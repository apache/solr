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

Base classes and utilities for JUnit tests of Solr. Solr's own tests use it, and it is published as the `solr-test-framework` artifact for plugin and client authors who want the same harness.

It builds on Lucene's `lucene-test-framework`: `SolrTestCase` extends `LuceneTestCase`, so every Solr test inherits randomizedtesting's `RandomizedRunner` — seeded randomness, `random()`, thread-leak detection, `MockDirectoryFactory`, test groups such as `@Nightly`, and the `-Ptests.*` options.

Solr is on **JUnit 4**. The build calls `useJUnit()` (`gradle/testing/defaults-tests.gradle`), not `useJUnitPlatform()`, so there is no JUnit 5 / Jupiter support: use `org.junit.Test`, `@BeforeClass`/`@AfterClass`, `@Rule`, and `Assume` rather than their Jupiter equivalents. (`junit-jupiter-api` appears in the lockfiles only as a transitive dependency.)

## Which base class

| Class | Use it when |
| --- | --- |
| `org.apache.solr.SolrTestCase` | The root. Anything that needs no Solr core: unit tests of a single class, parsers, utilities. Preferred for new tests. |
| `org.apache.solr.cloud.SolrCloudTestCase` | You need a real SolrCloud: starts an in-JVM `MiniSolrCloudCluster` with embedded ZooKeeper. |
| `org.apache.solr.shell.SolrShellTestCase` | You are testing `bin/solr` / `bin\solr.cmd` itself, by running it as a real subprocess against an unpacked distribution. |
| `org.apache.solr.SolrTestCaseJ4` | Legacy. Do not extend it in new tests (see below). |
| `org.apache.solr.BaseDistributedSearchTestCase` | Distributed-search comparisons: runs a query against N shards and against a control core and diffs the responses. |

Alternatives to extending a base class at all: the `SolrClientTestRule` family (see *Utilities* below) gives a test a Solr instance plus client as a JUnit rule, which composes better than inheritance and is the recommended route for external projects.

## SolrTestCase

The root of every Solr test. It is deliberately thin — its javadoc says *"be conservative in what's here; avoid arbitrary/misc utilities"* — and provides the cross-cutting rules every Solr test needs:

- Thread-leak filters (`SolrIgnoredThreadsFilter` plus Lucene's `QuickPatchThreadsFilter`) and a 1 s thread-leak lingering window.
- `SystemPropertiesRestoreRule`, so a test that sets a system property cannot leak it into the next class.
- `ObjectReleaseTracker` verification after a successful class: anything Solr tracks must have been closed.
- `@LogLevel` support via `LogLevelTestRule`, and `RevertDefaultThreadHandlerRule`.
- The test-class naming convention check, and defaulting `solr.configset.default.confdir` to `ExternalPaths.DEFAULT_CONFIGSET`.

Note that `@ThreadLeakFilters` is not additive: a subclass that needs its own filter must restate the inherited ones.

## SolrCloudTestCase

Starts a `MiniSolrCloudCluster` — N `JettySolrRunner` nodes plus a `ZkTestServer`, all in the test JVM — and exposes it as the static `cluster` field. Call `configureCluster(int)` from `@BeforeClass` and shutdown is automatic:

```java
public class MyCloudTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", ExternalPaths.DEFAULT_CONFIGSET).configure();
  }

  @Test
  public void createsCollection() throws Exception {
    CollectionAdminRequest.createCollection("test", "conf", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("test", 2, 2);
  }
}
```

Useful neighbours: `MultiSolrCloudTestCase` (several clusters at once), `SolrCloudAuthTestCase` (authentication/authorization metrics assertions), `AbstractFullDistribZkTestBase` (the older, heavier cloud base), `ChaosMonkey` and `SocketProxy` (fault injection), and the `AbstractCloudBackupRestoreTestCase` / `AbstractIncrementalBackupTest` bases that backup repository implementations reuse.

## SolrShellTestCase

`org.apache.solr.shell` is a cross-platform harness for testing the start scripts. It runs `bin/solr` under `bash` and `bin\solr.cmd` under `cmd.exe` from **one** Java test, which is what the BATS suites in `solr/packaging/test` cannot do. Ported BATS suites live in `solr/packaging/src/test/org/apache/solr/shell/`; the harness' own unit tests live in `solr/test-framework/src/test/org/apache/solr/shell/`.

There is no long-lived shell process. Every command starts a fresh one with the session's environment and working directory, so quoting, globbing and expansion are done by the real shell while control flow stays in Java.

### Sessions: classCli and cli

Two scopes, mirroring BATS' `setup_file` and `setup`:

| Field | Scope | Mirrors | Cleanup |
| --- | --- | --- | --- |
| `classCli` (static) | The whole class | `setup_file` | Nodes it started are stopped after the class |
| `cli` | One test method | `setup` | Environment and cwd changes discarded; nodes it started stopped after the test |

The base class' `@BeforeClass` has already run by the time a subclass' own `@BeforeClass` does, so starting a long-lived node there is safe. Test methods normally use the inherited shorthands (`run`, `http`, `env`, `read`, …), which all delegate to `cli`; only `@BeforeClass`/`@AfterClass` have to name `classCli` explicitly.

```java
public class CreateCollectionTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    classCli.run("solr start").assertSuccess();
  }

  @Test
  public void createCollection() {
    run("solr create -c coll_default")
        .assertContains("Created collection 'coll_default'")
        .assertContains("assuming solr url is http://localhost:${SOLR_PORT}");
  }
}
```

### Running commands

| Call | What it does |
| --- | --- |
| `run(String line)` | One command, variables resolved by the harness, stdout and stderr merged as BATS' `run` does |
| `run(Duration, String)` | Same, with an explicit timeout |
| `run(List<String> argv)` | Same, but the harness quotes each argument for the shell in use — the way to pass an argument needing per-shell quoting |
| `runSeparated(String)` | Keeps `stdout()` and `stderr()` apart |
| `script(String bash, String cmd)` | Escape hatch for shell-specific, possibly multi-line logic; `null` for a shell means the test is skipped there |
| `lastResult()` | The previous command's result |

Defaults: 120 s per command, 300 s for a line starting `solr start` or `solr restart`; override per session with `timeout(..)` / `startTimeout(..)`. A command that exceeds its timeout is killed and fails the test.

### Assertions

`CommandResult` is a record with fluent, chainable assertions ported from `bats-assert`:

| Assertion | BATS equivalent |
| --- | --- |
| `assertSuccess()`, `assertFailure()`, `assertStatus(int)` | `[ "$status" -eq .. ]` |
| `assertContains`, `assertNotContains` | `assert_output --partial`, `refute_output --partial` |
| `assertMatches`, `assertNotMatches` | `assert_output --regexp`, `refute_output --regexp` |
| `assertExact` | `assert_output` with no flag |
| `assertLine(int, String)`, `assertLineCount(int)`, `assertContainsTimes(String, int)` | `assert_line --index`, `wc -l`, `grep -c` |
| `assertContainsForCli(commonsCliText, picocliText)` | `assert_output_for_cli` |

And accessors that replace the pipelines a BATS line would use: `lines()`, `lineCount()`, `linesMatching(regex)`, `firstMatch(regex)`, `firstLines(n)`, `lastLines(n)`, `fields(lineIndex)`, `countOccurrences(substring)`.

HTTP goes through `http(pathAndQuery)` / `http(port, pathAndQuery)` (an `HttpResult` with `status()`, `body()` and its own assertions) and `https(..)`, so no test needs `curl`. `collectionExists`, `configExists`, `deleteAllCollections`, `shutdownAll` and `dumpSolrLog` are the `bats_helper.bash` conveniences.

### Variables: three mechanisms

1. **`${NAME}`, `$NAME` and `{{NAME}}` resolved by the harness.** All three name an environment variable and all work identically in a command line *and* in an assertion's expected text, so a BATS line ports over unchanged. The harness substitutes them, not the shell — that is what makes one line work on both, since bash expands `$NAME` and `cmd.exe` does not, while `cmd.exe` wants `%NAME%`, which bash leaves alone. An unknown name fails the test instead of silently becoming the empty string, and the transcript records the real command. `%NAME%` is rejected in a cross-shell line rather than substituted. `$'..'`, `$1` and a bare `$` are left for the shell.
2. **Java concatenation** for a value that comes from Java rather than from the environment: `run("solr start -p " + myPort)`. One sharp edge — concatenation happens before the harness resolves the line, so a concatenated value containing `${...}` is read as a variable reference.
3. **`run(List.of(..))`** when an argument needs per-shell quoting. The harness resolves variables in each element and then quotes it correctly for bash or for `cmd.exe`.

Harness-supplied variables include `SOLR_TIP`, `SOLR_HOME`, `SOLR_PID_DIR`, `SOLR_LOGS_DIR`, `TEST_OUTPUT_DIR`, `TEST_FAILURE_DIR`, `WORK_DIR` (the scope's scratch directory) and the port block `SOLR_PORT`, `SOLR2_PORT`, `SOLR3_PORT`, `ZK_PORT`. Add your own with `env(name, value)`, and remove one with `unset(name)` — needed because some `bin/solr` behaviour is only reachable with a variable absent rather than empty.

Inside `script(bash, cmd)` the rules change: `$NAME` and `%NAME%` are left entirely to the shell, because a script has its own locals and positional parameters, and `{{NAME}}` is the one form the harness still resolves. A `$NAME` in the *cmd* body is rejected, since `cmd.exe` would pass it through as literal text while the bash body of the same call appears to work.

### Cross-shell portability rails

A `run()` line is checked before it is executed, and a line that could only ever pass on bash fails immediately with the replacement named. The checks reject:

- Unix-only tools — `grep`, `sed`, `awk`, `cat`, `ls`, `find`, `curl`, `jq`, `sleep`, `ps`, `kill` and about 40 more — each mapped to the harness feature that replaces it.
- bash-only syntax outside quotes: `|`, `&&`, `||`, `;`, `$(..)`, backticks, single-quoted arguments, a leading `NAME=value`, `/dev/null`, `/tmp/`.
- More than one command in a `run()` line (a newline), and the `%NAME%` variable form.

A test that declares itself one shell's with `@EnabledOnShell` has the rails switched off automatically: there is no other shell left for it to break on. Otherwise, genuinely shell-specific logic goes in `script(bash, cmd)`.

### File and output helpers

Because `mkdir`, `rm`, `cp`, `cat`, `ls` and `find` do not exist on `cmd.exe`, the harness does that work in Java. Every path resolves against the session's working directory, so a relative path reads exactly as it did in the shell: `path`, `mkdirs`, `write`, `append`, `read`, `readLinesMatching`, `fileContains`, `exists`, `rm`, `copy`, `touch`, `list(dir)`, `list(dir, glob)`, `cwd`, `workDir`, `cd`. Waiting is `waitFor(..)` / `awaitOrFail(what, timeout, interval, condition)` rather than `sleep`.

### Skipping: @EnabledOnShell and @EnabledOnCli

Both work on a method or on a whole class, and skip as an assumption failure. On a class they skip before any Solr is started, so a `@BeforeClass` that starts a node costs nothing on the wrong shell or CLI.

- `@EnabledOnShell(Shell.BASH)` / `(Shell.CMD)` — for places where `bin/solr` and `bin/solr.cmd` genuinely differ. No annotation is the default and means the test is expected to pass on every shell, which is what a ported BATS test normally should be.
- `@EnabledOnCli(Cli.PICOCLI)` / `(Cli.COMMONS_CLI)` — for the few tests that cannot hold for both CLI implementations, usually because they assert on usage or error wording the two phrase differently. Where only a word or two differs, prefer `assertContainsForCli(commonsCliText, picocliText)`, which keeps one test covering both.

A test that drives the harness without needing a distribution, and so does not extend `SolrShellTestCase`, can honour `@EnabledOnShell` by adding `@Rule public final ShellGate shellGate = new ShellGate();`.

### Isolation and cleanup

`HarnessEnv.forClass(className)` resolves everything a class can write to or listen on per test class, so classes can run concurrently in separate forked JVMs:

- Its own `SOLR_HOME` (wiped at class setup), `SOLR_PID_DIR` (inside `SOLR_HOME`, which is what scopes `solr stop --all` to this class and away from a developer's own Solr), and logs directory.
- Its own port block from `PortBlocks`, claimed under an exclusive file lock on a counter file in the shared `TEST_OUTPUT_DIR` — safe across forks, unlike asking the OS for a free port. The block layout is dictated by Solr: a node on port `P` also listens on `P-1000` for `solr stop` and runs embedded ZooKeeper on `P+1000`, so a class occupies three such triples.
- A pre-flight check that fails fast if the block is not free (reaping any node of ours left behind by a killed earlier run), and a post-flight check that fails the class if a node outlived it.

Each value is looked up as a process environment variable first (how the Gradle task supplies it), then as the system property `solr.cli.<lowercase.dotted.name>` (so an IDE run can override), then a fallback. `SOLR_TIP` has no fallback. The shell defaults to `CMD` on Windows and `BASH` elsewhere, overridable with `-Dsolr.cli.shell=` or `SOLR_CLI_SHELL`.

### How a failure reads

A failing command throws `ShellAssertionError`, which carries **no** stack trace — the Java frames would all be harness internals. What you get instead is the scope's transcript, formatted like a shell session, followed by one line saying how the command differed from what the test expected:

```
$ solr start
exit 0 in 3.1s
| Started Solr server on port 46894 (pid=1234). Happy searching!

$ solr zk ls / -z localhost:47894
exit 0 in 1.2s
| aliases.json
```

A malformed *test* — an unknown `${NAME}`, a `cmd.exe`-only spelling in a cross-shell line, a non-portable tool — still throws an ordinary `AssertionError` with its frames intact, because there the useful information is which line of the test is wrong.

On failure, the class' `SOLR_HOME` and the transcript are also copied to `$TEST_FAILURE_DIR/<Class>-<method>/` and the path is printed. That is BATS' `save_home_on_failure`.

## SolrTestCaseJ4

The old "junit4 Solr test harness": `initCore(..)`, `assertQ`, `assertU`, `req(..)`, the `TestHarness`, SSL randomization, numeric-type randomization and a large pile of static helpers. It is still what much of the existing test suite extends, including `SolrCloudTestCase`.

**Do not extend it in new tests** — extend `SolrTestCase` and build documents and params with plain SolrJ types instead (`AGENTS.md`: "Use SolrTestCase instead of SolrTestCaseJ4 for new tests"). Individual members of it are being deprecated and removed as replacements land; `ignoreException`/`unIgnoreException`/`resetExceptionIgnores`, for example, are gone in favour of `ErrorLogMuter`.

## Utilities

Not an exhaustive list — these are the ones a newcomer actually reaches for.

**Running Solr**

- `embedded/JettySolrRunner` — one Solr node in Jetty, in this JVM. The building block under everything else.
- `embedded/JettyConfig` — its configuration (SSL, context, request filters, opt-in Admin UI).
- `cloud/MiniSolrCloudCluster` — N `JettySolrRunner`s plus a `ZkTestServer`, with collection and config-set helpers.
- `cloud/ZkTestServer` — a standalone embedded ZooKeeper for tests that only need ZK.
- `util/SolrClientTestRule` — abstract JUnit `TestRule` that hands a test a `SolrClient` and shuts Solr down afterwards; implemented by `util/EmbeddedSolrServerTestRule` (strictly in-process) and `util/SolrJettyTestRule` (over HTTP). Preferred over inheritance, and the recommended entry point for projects testing their own Solr plugins.

**Randomization and environment**

- `util/RandomizeSSL` — class annotation controlling the odds that a test runs over SSL (`@SuppressSSL` opts out). `util/SSLTestConfig` is the machinery; `util/NotSecurePseudoRandomSpi` and `util/SolrTestNonSecureRandomProvider` keep SSL tests off entropy-starved blocking.
- `SolrIgnoredThreadsFilter` — the thread-leak filter for threads Solr legitimately leaves behind. Applied by `SolrTestCase`; restate it if your subclass adds filters of its own.
- `util/ExternalPaths` — paths outside the classpath: `SOURCE_HOME`, `DEFAULT_CONFIGSET`, `TECHPRODUCTS_CONFIGSET`, `SERVER_HOME`.
- `util/RandomMergePolicy*` / `util/RandomForceMergePolicy*`, `core/MockDirectoryFactory`, `core/MockFSDirectoryFactory` — the randomized Lucene-level plumbing referenced from test configs.

**Logging**

- `util/LogLevel` — `@LogLevel("org.apache.solr.foo=DEBUG")` on a class or method; applied by `LogLevelTestRule` from `SolrTestCase`.
- `util/LogListener` — assert that a particular log message was (or was not) produced: `try (LogListener errLog = LogListener.error(SolrCore.class)) { ...; assertEquals(0, errLog.getCount()); }`.
- `util/ErrorLogMuter` — silence ERROR messages a test *expects*, so they are not a distraction in the logs, while still counting them.

**Assertions and inspection**

- `JSONTestUtil` — compare a JSON string (or an already-parsed object) against an expected value at a given JSON path; `match(..)` returns null on a match and a description of the mismatch otherwise.
- `util/QueryMatchers` — Hamcrest matchers for parsed Lucene `Query` trees (term, phrase, boolean, disjunction-max). `util/SolrMatchers` — extra Hamcrest matchers, Solr-specific or just filling gaps Hamcrest leaves.
- `handler/component/TrackingShardHandlerFactory`, `update/processor/TrackingUpdateProcessorFactory`, `update/processor/RecordingUpdateProcessorFactory`, `core/TrackingBackupRepository` — record what Solr actually did, for tests that need to assert on internals.
- `util/SecurityJson` — ready-made `security.json` constants (`SIMPLE`, plus `USER`/`PASS`) for tests that enable authentication.

**Narrower test bases**

- `core/AbstractBadConfigTestBase` — assert that a bad `solrconfig.xml`/`schema.xml` fails to load with the expected message.
- `util/RestTestBase` and `util/RestTestHarness` — a `SolrJettyTestRule` plus a harness for driving Solr's REST-ish APIs over HTTP.
- `update/processor/UpdateProcessorTestBase` — exercise an `UpdateRequestProcessor` chain directly.
- `util/DOMUtilTestBase`, `analysis/*` mocks, `schema/*` mocks, `search/*` plugins — small fixtures referenced by name from test configs.
- `util/SeleniumTest` — the `@SeleniumTest` test group annotation (disabled by default, enabled with `-Ptests.selenium=true`) used by the browser-based Admin UI tests.

## Running tests

```bash
./gradlew test                                  # everything
./gradlew -p solr/core test                     # one module
./gradlew -p solr/core test --tests TestDemo    # one class (gradle filtering)
./gradlew -p solr/core test --tests "org.apache.solr.index.*"
./gradlew -p solr/core test -Ptests.seed=deadbeef   # reproduce a failure's seed
./gradlew -p solr/core test -Ptests.iters=5 --tests TestDemo
./gradlew -p solr/core test -Ptests.nightly=true    # enable the @Nightly group
./gradlew -p solr/core testOpts                 # list every tests.* property and its value
```

`./gradlew helpTests` prints the full guide (the source is `dev-docs/gradle-help/tests.txt`): filtering, test groups and `-Ptests.filter` boolean expressions, beasting, verbose mode, profiling.

### The shell harness has its own tasks

`SolrShellTestCase` spawns real shell processes, which the test security policy forbids (it grants `execute` for `java` alone), so the shell suites are excluded from `test` and run from dedicated tasks that drop `java.security.manager`:

| Task | What it runs |
| --- | --- |
| `./gradlew -p solr/test-framework shellTests` | The harness' own unit tests. No Solr distribution needed, runs in seconds, and **wired into `check`** so a harness regression is caught immediately. |
| `./gradlew -p solr/packaging shellTests` | The CLI suites against the unpacked distribution. Not part of `check` (like the BATS suites, it needs `installFullDist`). Classes run in parallel; cap the forks with `-Pshell.tests.jvms=N`. |
| `./gradlew -p solr/packaging shellTestsPicocli` | The same suites against the picocli CLI. |
| `./gradlew -p solr/packaging shellTestsCommonsCli` | The same suites against commons-cli. Deprecated; goes away with commons-cli itself. |

Both tasks are called `shellTests`, so `./gradlew shellTests` from the root runs both.

To run a shell suite from an IDE, build a distribution first and point the harness at it:

```bash
./gradlew -p solr/packaging dev
# then run the test class with -Dsolr.cli.solr.tip=/path/to/solr/packaging/build/dev
```

## See also

- `dev-docs/writing-tests.adoc` — how to write a Solr test: conventions, what to assert, what to avoid.
- `solr/packaging/test/README.md` — the BATS suites for `bin/solr`, run with `./gradlew integrationTests`. The `org.apache.solr.shell` harness is their cross-platform successor; the BATS suites still exist and still run.
- `dev-docs/admin-ui-tests.md` — the Selenium/headless-Chrome tests of the AngularJS Admin UI.
- `dev-docs/gradle-help/tests.txt` — the text behind `./gradlew helpTests`.
